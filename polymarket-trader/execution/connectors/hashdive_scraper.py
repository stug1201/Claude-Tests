#!/usr/bin/env python3
"""
hashdive_scraper.py -- Playwright-based Hashdive trader profile scraper.

Scrapes trader profile pages on Hashdive to extract Smart Scores, insider
classification flags, and wallet performance metrics for IPS bootstrap
labeling.  Intercepts XHR responses during page load to capture structured
JSON data before falling back to DOM parsing.

Method: Playwright headless browser with stealth settings
Auth: HASHDIVE_SESSION_COOKIE (manually extracted after browser login)

HIGH RISK: Scraping is fragile by nature.  If the page structure changes,
anti-bot detection is encountered, or the session cookie expires, this
connector degrades gracefully (logs the issue, saves debug HTML, and
returns None for affected wallets).  Update directives/01_data_infrastructure.md
if any new scraping issues are discovered.

Rate limit assumption: 2 second delay between profile page loads to avoid
triggering anti-bot mechanisms.  Adjust SCRAPE_DELAY_SECONDS if Hashdive
begins rate-limiting at a different threshold.

Usage:
    python execution/connectors/hashdive_scraper.py                         # Live mode (single wallet)
    python execution/connectors/hashdive_scraper.py --wallet 0xABC123       # Specific wallet
    python execution/connectors/hashdive_scraper.py --batch wallets.txt     # Batch from file
    python execution/connectors/hashdive_scraper.py --test                  # Fixture mode (no browser)

Environment variables (loaded via execution.utils.config):
    HASHDIVE_SESSION_COOKIE -- Session cookie from manual browser login

See: directives/01_data_infrastructure.md
     crypto-digest/LESSONS_LEARNED.md  (logging best practice #7)
"""

import argparse
import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent            # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                       # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                     # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
HASHDIVE_TMP_DIR = TMP_DIR / "hashdive"
FIXTURE_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURE_DIR / "hashdive_fixture.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HASHDIVE_BASE_URL = "https://hashdive.com"
HASHDIVE_PROFILE_URL = HASHDIVE_BASE_URL + "/trader/{wallet_address}"

# Rate limit: 2 second delay between profile page loads (see directive)
SCRAPE_DELAY_SECONDS = 2.0

# Smart Score threshold for insider flag derivation.
# Wallets with a Smart Score >= 80 are flagged as potential insiders
# for IPS bootstrap labeling.  Wallets >= 90 are high-confidence insiders.
INSIDER_SCORE_THRESHOLD = 80

# Navigation timeout (ms) for Playwright page.goto
PAGE_TIMEOUT_MS = 30_000

# User agents for rotation (stealth mode)
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]


def _ensure_directories() -> None:
    """Create .tmp/hashdive/ and .tmp/fixtures/ directories if missing."""
    HASHDIVE_TMP_DIR.mkdir(parents=True, exist_ok=True)
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Fixture / test data
# ---------------------------------------------------------------------------

_FIXTURE_SCORES: list[dict] = [
    {
        "wallet_address": "0xDEADBEEF00000000000000000000000000000001",
        "smart_score": 92,
        "trader_rank": "Elite",
        "win_rate": 0.847,
        "total_trades": 1423,
        "profit_loss": 58320.50,
        "insider_flag": True,
    },
    {
        "wallet_address": "0xDEADBEEF00000000000000000000000000000002",
        "smart_score": 67,
        "trader_rank": "Advanced",
        "win_rate": 0.612,
        "total_trades": 342,
        "profit_loss": 4210.75,
        "insider_flag": False,
    },
    {
        "wallet_address": "0xDEADBEEF00000000000000000000000000000003",
        "smart_score": 85,
        "trader_rank": "Expert",
        "win_rate": 0.789,
        "total_trades": 891,
        "profit_loss": 31450.00,
        "insider_flag": True,
    },
    {
        "wallet_address": "0xDEADBEEF00000000000000000000000000000004",
        "smart_score": 34,
        "trader_rank": "Beginner",
        "win_rate": 0.381,
        "total_trades": 57,
        "profit_loss": -1280.30,
        "insider_flag": False,
    },
    {
        "wallet_address": "0xDEADBEEF00000000000000000000000000000005",
        "smart_score": 45,
        "trader_rank": "Intermediate",
        "win_rate": 0.502,
        "total_trades": 189,
        "profit_loss": 620.10,
        "insider_flag": False,
    },
]


def _generate_fixture_data() -> list[dict]:
    """Write fixture data to disk and return it.

    Fixture mode allows CI runs and development without credentials
    or a running Playwright browser.
    """
    _ensure_directories()

    FIXTURE_FILE.write_text(
        json.dumps(_FIXTURE_SCORES, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        "Fixture data written: %d wallets -> %s",
        len(_FIXTURE_SCORES),
        FIXTURE_FILE,
    )
    return _FIXTURE_SCORES


# ---------------------------------------------------------------------------
# HashdiveScraper class
# ---------------------------------------------------------------------------

class HashdiveScraper:
    """Playwright-based scraper for Hashdive trader profile pages.

    Launches a headless Chromium browser with stealth settings, navigates
    to individual trader profile pages, intercepts XHR responses for
    structured data, and falls back to DOM parsing when XHR capture
    fails.

    Args:
        session_cookie: HASHDIVE_SESSION_COOKIE value for authenticated
            access.  If empty or None, the scraper will attempt
            unauthenticated access (may have limited data).

    Usage::

        scraper = HashdiveScraper(session_cookie="abc123...")
        await scraper.setup_browser()
        result = await scraper.scrape_trader_profile("0xWALLET...")
        await scraper.close_browser()
    """

    def __init__(self, session_cookie: Optional[str] = None) -> None:
        self._session_cookie = session_cookie
        self._browser = None
        self._context = None
        self._page = None
        self._ua_index = 0
        # Buffer for intercepted XHR responses (reset per page load)
        self._xhr_data: list[dict] = []

        if not self._session_cookie:
            logger.warning(
                "HASHDIVE_SESSION_COOKIE is not set or empty.  "
                "Scraper will attempt unauthenticated access, which may "
                "return limited data."
            )

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    async def setup_browser(self) -> None:
        """Launch Playwright Chromium with stealth settings.

        Configures:
        - Headless mode
        - Rotated User-Agent string
        - Disabled WebDriver flag (anti-bot evasion)
        - Session cookie injection (if provided)
        - XHR response interception
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error(
                "Playwright is not installed.  "
                "Install with: pip install playwright && python -m playwright install chromium"
            )
            raise

        self._playwright = await async_playwright().start()

        # Select user agent (rotate through list)
        user_agent = _USER_AGENTS[self._ua_index % len(_USER_AGENTS)]
        self._ua_index += 1

        # Launch with stealth arguments
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )

        # Create browser context with stealth settings
        self._context = await self._browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
            java_script_enabled=True,
        )

        # Inject session cookie if available
        if self._session_cookie:
            await self._context.add_cookies([
                {
                    "name": "session",
                    "value": self._session_cookie,
                    "domain": ".hashdive.com",
                    "path": "/",
                    "httpOnly": True,
                    "secure": True,
                    "sameSite": "Lax",
                },
            ])
            logger.info("Session cookie injected into browser context.")

        # Create page and configure XHR interception
        self._page = await self._context.new_page()

        # Remove the webdriver property to evade detection
        await self._page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            // Override permissions query
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) =>
                parameters.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : originalQuery(parameters);
            // Override plugins length
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
        """)

        # Attach XHR response listener
        self._page.on("response", self._intercept_xhr)

        logger.info(
            "Browser launched (headless, stealth, UA: %s...)",
            user_agent[:50],
        )

    async def close_browser(self) -> None:
        """Gracefully close browser, context, and Playwright instance."""
        if self._page:
            await self._page.close()
            self._page = None
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if hasattr(self, "_playwright") and self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Browser closed.")

    # ------------------------------------------------------------------
    # XHR interception
    # ------------------------------------------------------------------

    async def _intercept_xhr(self, response) -> None:
        """Capture XHR/Fetch responses that contain trader profile data.

        Hashdive may load profile data via AJAX calls to internal API
        endpoints.  Intercepting these provides clean JSON instead of
        requiring fragile DOM parsing.

        Captured data is stored in self._xhr_data for the current page
        load.  It is cleared at the start of each scrape_trader_profile()
        call.
        """
        try:
            url = response.url
            content_type = response.headers.get("content-type", "")

            # Only capture JSON responses from Hashdive API endpoints
            if (
                "hashdive.com" in url
                and "application/json" in content_type
                and response.status == 200
            ):
                body = await response.text()
                try:
                    data = json.loads(body)
                    self._xhr_data.append({
                        "url": url,
                        "data": data,
                    })
                    logger.debug(
                        "Captured XHR response: %s (%d bytes)",
                        url, len(body),
                    )
                except json.JSONDecodeError:
                    logger.debug(
                        "XHR response not valid JSON: %s (first 300 chars: %.300s)",
                        url, body,
                    )
        except Exception as exc:
            # Response interception should never crash the scraper
            logger.debug("XHR interception error (non-fatal): %s", exc)

    # ------------------------------------------------------------------
    # Profile scraping
    # ------------------------------------------------------------------

    async def scrape_trader_profile(self, wallet_address: str) -> Optional[dict]:
        """Navigate to a Hashdive trader profile and extract scoring data.

        Attempts data extraction in two phases:
        1. XHR interception -- check captured JSON responses for structured
           profile data (preferred, more reliable).
        2. DOM parsing -- fall back to extracting visible page elements
           using CSS selectors.

        On failure, saves raw HTML to .tmp/hashdive/ for debugging and
        logs the first 300 chars of the page body (crypto-digest lesson #7).

        Args:
            wallet_address: Ethereum wallet address (0x...).

        Returns:
            Dict with keys: wallet_address, smart_score, trader_rank,
            win_rate, total_trades, profit_loss, insider_flag.
            Returns None if scraping fails entirely.
        """
        if not self._page:
            logger.error("Browser not initialised.  Call setup_browser() first.")
            return None

        # Reset XHR buffer for this page load
        self._xhr_data = []

        profile_url = HASHDIVE_PROFILE_URL.format(wallet_address=wallet_address)
        logger.info("Scraping profile: %s", profile_url)

        try:
            response = await self._page.goto(
                profile_url,
                wait_until="networkidle",
                timeout=PAGE_TIMEOUT_MS,
            )
        except Exception as exc:
            logger.error(
                "Navigation failed for %s: %s",
                wallet_address, exc,
            )
            return None

        # -- Check for anti-bot detection -------------------------------------
        if response and response.status in (403, 429, 503):
            page_text = await self._page.text_content("body") or ""
            logger.error(
                "Anti-bot detection encountered for %s (HTTP %d).  "
                "Mechanism: %s.  Gracefully degrading.",
                wallet_address,
                response.status,
                _detect_anti_bot_mechanism(page_text),
            )
            await self._save_debug_html(wallet_address, "antibot")
            return None

        # -- Check for session cookie expiry ----------------------------------
        page_content = await self._page.content()
        if _is_login_page(page_content):
            logger.error(
                "Session cookie appears expired -- redirected to login page "
                "for wallet %s.  Continuing without Hashdive data.  "
                "Obtain a fresh HASHDIVE_SESSION_COOKIE.",
                wallet_address,
            )
            await self._save_debug_html(wallet_address, "session_expired")
            return None

        # -- Phase 1: Try XHR captured data -----------------------------------
        result = self._parse_xhr_data(wallet_address)
        if result:
            logger.info(
                "Extracted profile via XHR for %s: smart_score=%d insider=%s",
                wallet_address, result["smart_score"], result["insider_flag"],
            )
            return result

        # -- Phase 2: Fall back to DOM parsing --------------------------------
        logger.info(
            "No usable XHR data for %s.  Falling back to DOM parsing.",
            wallet_address,
        )
        result = await self._parse_dom(wallet_address)
        if result:
            logger.info(
                "Extracted profile via DOM for %s: smart_score=%d insider=%s",
                wallet_address, result["smart_score"], result["insider_flag"],
            )
            return result

        # -- Both methods failed ----------------------------------------------
        logger.warning(
            "Failed to extract profile data for %s.  "
            "Page structure may have changed.  Saving HTML for debugging.",
            wallet_address,
        )
        # Log first 300 chars of response body (crypto-digest lesson #7)
        body_text = await self._page.text_content("body") or ""
        logger.warning(
            "Page body preview (first 300 chars): %.300s",
            body_text,
        )
        await self._save_debug_html(wallet_address, "parse_failure")
        return None

    # ------------------------------------------------------------------
    # XHR data parsing
    # ------------------------------------------------------------------

    def _parse_xhr_data(self, wallet_address: str) -> Optional[dict]:
        """Attempt to extract profile data from captured XHR responses.

        Looks for JSON payloads that contain smart_score or similar
        keys in the intercepted XHR responses.

        Returns:
            Parsed profile dict, or None if no usable XHR data found.
        """
        for xhr in self._xhr_data:
            data = xhr["data"]

            # The XHR payload may be a dict or nested structure.
            # Try common response envelope patterns.
            profile = None
            if isinstance(data, dict):
                # Direct profile object
                if "smart_score" in data or "smartScore" in data:
                    profile = data
                # Wrapped in a data/result envelope
                elif "data" in data and isinstance(data["data"], dict):
                    profile = data["data"]
                elif "result" in data and isinstance(data["result"], dict):
                    profile = data["result"]

            if profile and ("smart_score" in profile or "smartScore" in profile):
                try:
                    return _normalize_profile(wallet_address, profile)
                except (KeyError, ValueError, TypeError) as exc:
                    logger.warning(
                        "XHR data found but parsing failed for %s: %s  "
                        "XHR URL: %s  Data preview: %.300s",
                        wallet_address, exc, xhr["url"], json.dumps(profile),
                    )
                    continue

        return None

    # ------------------------------------------------------------------
    # DOM parsing
    # ------------------------------------------------------------------

    async def _parse_dom(self, wallet_address: str) -> Optional[dict]:
        """Extract profile data from page DOM using CSS selectors.

        This is the fallback path when XHR interception yields no data.
        CSS selectors target common UI patterns for score displays.

        Returns:
            Parsed profile dict, or None if DOM parsing fails.
        """
        page = self._page

        try:
            # Smart Score -- commonly displayed as a large number in a
            # score card or badge.  Try multiple selector strategies.
            smart_score = await self._extract_number(
                selectors=[
                    "[data-testid='smart-score']",
                    ".smart-score",
                    ".score-value",
                    ".trader-score .value",
                    "div.score >> text=/\\d+/",
                ],
                name="smart_score",
            )

            # Trader Rank
            trader_rank = await self._extract_text(
                selectors=[
                    "[data-testid='trader-rank']",
                    ".trader-rank",
                    ".rank-label",
                    ".rank-badge",
                ],
                name="trader_rank",
                default="Unknown",
            )

            # Win Rate
            win_rate = await self._extract_percentage(
                selectors=[
                    "[data-testid='win-rate']",
                    ".win-rate",
                    ".win-rate-value",
                    "div:has-text('Win Rate') >> .value",
                ],
                name="win_rate",
            )

            # Total Trades
            total_trades = await self._extract_number(
                selectors=[
                    "[data-testid='total-trades']",
                    ".total-trades",
                    ".trade-count",
                    "div:has-text('Total Trades') >> .value",
                ],
                name="total_trades",
            )

            # Profit/Loss
            profit_loss = await self._extract_currency(
                selectors=[
                    "[data-testid='profit-loss']",
                    ".profit-loss",
                    ".pnl-value",
                    "div:has-text('P&L') >> .value",
                    "div:has-text('Profit') >> .value",
                ],
                name="profit_loss",
            )

            if smart_score is None:
                logger.warning(
                    "DOM parsing: could not find smart_score for %s.",
                    wallet_address,
                )
                return None

            insider_flag = smart_score >= INSIDER_SCORE_THRESHOLD

            return {
                "wallet_address": wallet_address,
                "smart_score": smart_score,
                "trader_rank": trader_rank,
                "win_rate": win_rate if win_rate is not None else 0.0,
                "total_trades": total_trades if total_trades is not None else 0,
                "profit_loss": profit_loss if profit_loss is not None else 0.0,
                "insider_flag": insider_flag,
            }

        except Exception as exc:
            logger.warning(
                "DOM parsing exception for %s: %s",
                wallet_address, exc,
            )
            return None

    # ------------------------------------------------------------------
    # DOM extraction helpers
    # ------------------------------------------------------------------

    async def _extract_number(
        self,
        selectors: list[str],
        name: str,
    ) -> Optional[int]:
        """Try each selector in order and extract an integer value."""
        for selector in selectors:
            try:
                element = await self._page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text:
                        # Strip non-numeric characters (commas, spaces, etc.)
                        cleaned = re.sub(r"[^\d]", "", text.strip())
                        if cleaned:
                            return int(cleaned)
            except Exception:
                continue
        logger.debug("Could not extract %s from DOM.", name)
        return None

    async def _extract_text(
        self,
        selectors: list[str],
        name: str,
        default: str = "",
    ) -> str:
        """Try each selector in order and extract text content."""
        for selector in selectors:
            try:
                element = await self._page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text and text.strip():
                        return text.strip()
            except Exception:
                continue
        logger.debug("Could not extract %s from DOM, using default.", name)
        return default

    async def _extract_percentage(
        self,
        selectors: list[str],
        name: str,
    ) -> Optional[float]:
        """Try each selector and extract a percentage as a float (0..1)."""
        for selector in selectors:
            try:
                element = await self._page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text:
                        cleaned = text.strip().rstrip("%")
                        cleaned = re.sub(r"[^\d.]", "", cleaned)
                        if cleaned:
                            value = float(cleaned)
                            # If value > 1, assume it is a percentage (e.g., 84.7%)
                            if value > 1.0:
                                value = value / 100.0
                            return round(value, 4)
            except Exception:
                continue
        logger.debug("Could not extract %s from DOM.", name)
        return None

    async def _extract_currency(
        self,
        selectors: list[str],
        name: str,
    ) -> Optional[float]:
        """Try each selector and extract a currency value as float."""
        for selector in selectors:
            try:
                element = await self._page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text:
                        # Remove $, commas, whitespace; preserve minus sign
                        cleaned = re.sub(r"[^\d.\-]", "", text.strip())
                        if cleaned:
                            return round(float(cleaned), 2)
            except Exception:
                continue
        logger.debug("Could not extract %s from DOM.", name)
        return None

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------

    async def _save_debug_html(self, wallet_address: str, reason: str) -> None:
        """Save raw page HTML to .tmp/hashdive/ for post-mortem debugging.

        File naming: {wallet_address}_{reason}_{timestamp}.html
        """
        _ensure_directories()
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{wallet_address}_{reason}_{timestamp}.html"
        filepath = HASHDIVE_TMP_DIR / filename

        try:
            html = await self._page.content()
            filepath.write_text(html, encoding="utf-8")
            logger.info("Debug HTML saved: %s", filepath)
        except Exception as exc:
            logger.warning("Failed to save debug HTML: %s", exc)

    # ------------------------------------------------------------------
    # Batch scraping
    # ------------------------------------------------------------------

    async def scrape_batch(
        self,
        wallet_addresses: list[str],
        delay: float = SCRAPE_DELAY_SECONDS,
    ) -> list[dict]:
        """Scrape multiple wallet profiles with rate-limiting delays.

        Processes wallets sequentially with a configurable delay between
        requests to avoid triggering anti-bot detection.

        Args:
            wallet_addresses: List of Ethereum wallet addresses.
            delay: Seconds to wait between profile scrapes (default 2.0).

        Returns:
            List of successfully scraped profile dicts.  Failed scrapes
            are logged and omitted from the result.
        """
        results: list[dict] = []
        total = len(wallet_addresses)

        logger.info(
            "Starting batch scrape: %d wallets, %.1fs delay",
            total, delay,
        )

        for idx, wallet in enumerate(wallet_addresses):
            logger.info(
                "Scraping wallet %d/%d: %s",
                idx + 1, total, wallet,
            )

            result = await self.scrape_trader_profile(wallet)
            if result:
                results.append(result)
            else:
                logger.warning(
                    "Wallet %d/%d failed: %s (skipping)",
                    idx + 1, total, wallet,
                )

            # Rate-limiting delay (skip after last wallet)
            if idx < total - 1:
                logger.debug("Rate limit pause: %.1fs", delay)
                await asyncio.sleep(delay)

        logger.info(
            "Batch scrape complete: %d/%d wallets succeeded.",
            len(results), total,
        )
        return results


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _normalize_profile(wallet_address: str, raw: dict) -> dict:
    """Convert a raw JSON profile (from XHR) into the standard output format.

    Handles both camelCase and snake_case field names since the API
    format is not guaranteed.

    Args:
        wallet_address: The wallet address being scraped.
        raw: Raw profile dict from XHR capture.

    Returns:
        Normalized dict with standard keys.

    Raises:
        KeyError: If smart_score cannot be found in the data.
    """
    # Smart score (required)
    smart_score = raw.get("smart_score") or raw.get("smartScore")
    if smart_score is None:
        raise KeyError("smart_score not found in profile data")
    smart_score = int(smart_score)

    # Trader rank
    trader_rank = (
        raw.get("trader_rank")
        or raw.get("traderRank")
        or raw.get("rank")
        or "Unknown"
    )

    # Win rate (expect 0..1 or 0..100)
    win_rate = raw.get("win_rate") or raw.get("winRate") or 0.0
    win_rate = float(win_rate)
    if win_rate > 1.0:
        win_rate = win_rate / 100.0

    # Total trades
    total_trades = raw.get("total_trades") or raw.get("totalTrades") or 0
    total_trades = int(total_trades)

    # Profit/loss
    profit_loss = raw.get("profit_loss") or raw.get("profitLoss") or raw.get("pnl") or 0.0
    profit_loss = round(float(profit_loss), 2)

    # Insider flag derived from Smart Score threshold
    insider_flag = smart_score >= INSIDER_SCORE_THRESHOLD

    return {
        "wallet_address": wallet_address,
        "smart_score": smart_score,
        "trader_rank": str(trader_rank),
        "win_rate": round(win_rate, 4),
        "total_trades": total_trades,
        "profit_loss": profit_loss,
        "insider_flag": insider_flag,
    }


def _detect_anti_bot_mechanism(page_text: str) -> str:
    """Identify the type of anti-bot protection from page content.

    Returns a short description of the detected mechanism for logging.
    """
    text_lower = page_text.lower() if page_text else ""

    if "cloudflare" in text_lower:
        return "Cloudflare challenge page"
    if "captcha" in text_lower or "recaptcha" in text_lower:
        return "CAPTCHA challenge"
    if "rate limit" in text_lower or "too many requests" in text_lower:
        return "Rate limiting"
    if "access denied" in text_lower or "forbidden" in text_lower:
        return "Access denied / IP block"
    if "please verify" in text_lower or "human verification" in text_lower:
        return "Human verification challenge"
    if "bot" in text_lower and "detect" in text_lower:
        return "Bot detection system"

    return "Unknown mechanism"


def _is_login_page(html: str) -> bool:
    """Heuristically detect if the page is a login/auth redirect.

    Returns True if the HTML appears to be a login page rather than
    a trader profile.
    """
    if not html:
        return False
    html_lower = html.lower()

    login_indicators = [
        '<form.*action.*login',
        'sign.?in',
        'log.?in.*form',
        'authenticate',
        'session.*expired',
    ]
    for pattern in login_indicators:
        if re.search(pattern, html_lower):
            return True

    return False


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

async def _store_results(results: list[dict], test_mode: bool = False) -> None:
    """Persist scraped results to PostgreSQL via db.upsert_wallet_score().

    In test mode, uses the mock pool from db.py.

    Args:
        results: List of profile dicts from scraping.
        test_mode: If True, uses in-memory mock DB.
    """
    if not results:
        logger.info("No results to store.")
        return

    # Late import to avoid circular imports and allow --test without DB
    try:
        from execution.utils.db import upsert_wallet_score
    except ImportError:
        # When running standalone, try relative import path
        try:
            sys.path.insert(0, str(PROJECT_ROOT))
            from execution.utils.db import upsert_wallet_score
        except ImportError:
            logger.error(
                "Could not import db.upsert_wallet_score.  "
                "Results will not be persisted to database."
            )
            return

    for profile in results:
        try:
            await upsert_wallet_score(
                wallet_address=profile["wallet_address"],
                ips_score=0.0,  # IPS score is computed separately; bootstrap with 0
                win_rate_24h=profile["win_rate"],
                hashdive_score=profile["smart_score"],
            )
            logger.info(
                "Stored wallet score: %s (hashdive=%d, win_rate=%.3f)",
                profile["wallet_address"],
                profile["smart_score"],
                profile["win_rate"],
            )
        except Exception as exc:
            logger.error(
                "Failed to store wallet score for %s: %s",
                profile["wallet_address"], exc,
            )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

async def _run_live(
    wallet: Optional[str] = None,
    batch_file: Optional[str] = None,
) -> None:
    """Run the scraper in live mode against real Hashdive pages.

    Args:
        wallet: Single wallet address to scrape.
        batch_file: Path to a text file with one wallet address per line.
    """
    # Load config
    try:
        from execution.utils.config import config
        session_cookie = config.HASHDIVE_SESSION_COOKIE
    except ImportError:
        import os
        session_cookie = os.getenv("HASHDIVE_SESSION_COOKIE")
        logger.warning("Could not import config; using env var directly.")

    if not session_cookie:
        logger.error(
            "HASHDIVE_SESSION_COOKIE is not set.  Cannot scrape in live mode.  "
            "Set it in .env or pass --test for fixture data."
        )
        return

    # Determine wallet list
    wallets: list[str] = []
    if wallet:
        wallets = [wallet]
    elif batch_file:
        batch_path = Path(batch_file)
        if not batch_path.exists():
            logger.error("Batch file not found: %s", batch_path)
            return
        wallets = [
            line.strip()
            for line in batch_path.read_text().splitlines()
            if line.strip() and line.strip().startswith("0x")
        ]
        logger.info("Loaded %d wallets from %s", len(wallets), batch_path)
    else:
        logger.error(
            "No wallet specified.  Use --wallet 0x... or --batch wallets.txt"
        )
        return

    if not wallets:
        logger.error("No valid wallet addresses to scrape.")
        return

    # Scrape
    scraper = HashdiveScraper(session_cookie=session_cookie)
    try:
        await scraper.setup_browser()

        if len(wallets) == 1:
            result = await scraper.scrape_trader_profile(wallets[0])
            results = [result] if result else []
        else:
            results = await scraper.scrape_batch(wallets)

        # Store results
        await _store_results(results)

        # Print summary
        logger.info("=" * 72)
        logger.info("  Scrape Summary")
        logger.info("=" * 72)
        for r in results:
            logger.info(
                "  %s  score=%d  rank=%s  insider=%s",
                r["wallet_address"],
                r["smart_score"],
                r["trader_rank"],
                r["insider_flag"],
            )
        logger.info("=" * 72)
        logger.info(
            "  Total: %d/%d wallets scraped successfully",
            len(results), len(wallets),
        )
        logger.info("=" * 72)

    finally:
        await scraper.close_browser()


async def _run_test() -> None:
    """Run in --test mode: return fixture scores without launching browser."""
    logger.info("Running in --test mode: returning fixture data.")

    results = _generate_fixture_data()

    # Store fixture results in mock DB
    # Import db and set test mode
    try:
        from execution.utils import db as db_module
        db_module._test_mode = True
        await _store_results(results, test_mode=True)
    except ImportError:
        try:
            sys.path.insert(0, str(PROJECT_ROOT))
            from execution.utils import db as db_module
            db_module._test_mode = True
            await _store_results(results, test_mode=True)
        except ImportError:
            logger.warning(
                "Could not import db module for test storage.  "
                "Fixture data generated but not stored."
            )

    # Print summary
    logger.info("=" * 72)
    logger.info("  Fixture Data Summary (--test mode)")
    logger.info("=" * 72)
    for r in results:
        logger.info(
            "  %s  score=%d  rank=%-12s  wr=%.1f%%  trades=%d  pnl=$%.2f  insider=%s",
            r["wallet_address"][:10] + "...",
            r["smart_score"],
            r["trader_rank"],
            r["win_rate"] * 100,
            r["total_trades"],
            r["profit_loss"],
            r["insider_flag"],
        )
    logger.info("=" * 72)
    logger.info(
        "  Total: %d fixture wallets.  Insider threshold: score >= %d",
        len(results), INSIDER_SCORE_THRESHOLD,
    )
    logger.info("=" * 72)


def main() -> None:
    """CLI entry point: parse arguments and dispatch to live or test mode.

    Usage:
        python execution/connectors/hashdive_scraper.py --test
        python execution/connectors/hashdive_scraper.py --wallet 0x...
        python execution/connectors/hashdive_scraper.py --batch wallets.txt
    """
    parser = argparse.ArgumentParser(
        description=(
            "Hashdive trader profile scraper.  Extracts Smart Scores and "
            "insider classification flags for IPS bootstrap labeling."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Return fixture data without launching a browser.",
    )
    parser.add_argument(
        "--wallet",
        type=str,
        default=None,
        help="Single wallet address to scrape (0x...).",
    )
    parser.add_argument(
        "--batch",
        type=str,
        default=None,
        help="Path to text file with wallet addresses (one per line).",
    )
    args = parser.parse_args()

    if args.test:
        asyncio.run(_run_test())
    else:
        asyncio.run(_run_live(wallet=args.wallet, batch_file=args.batch))


if __name__ == "__main__":
    main()
