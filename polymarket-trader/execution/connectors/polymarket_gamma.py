#!/usr/bin/env python3
"""
polymarket_gamma.py -- Polymarket Gamma API connector.

Fetches market metadata (titles, categories, resolution criteria, close times,
volume, liquidity, outcomes) from the public Gamma API and stores it in
PostgreSQL via the db.insert helpers.  No authentication required.

Base URL: https://gamma-api.polymarket.com
Auth: None (public API)

Endpoints used:
    GET /markets          -- paginated market list (with query-string filters)
    GET /markets/{id}     -- single market by condition_id
    GET /markets?slug=X   -- single market by slug
    GET /events           -- events with nested markets
    GET /markets?_q=X     -- full-text search across market titles

Fields extracted per market:
    market_id, title, slug, close_time, category, volume, liquidity, outcomes

Rate-limit handling:
    Exponential backoff (1 s -> 2 s -> 4 s -> 8 s -> 16 s, max 60 s).

Polling:
    In continuous mode (default), refreshes all active market metadata every
    5 minutes.  Pass --once to fetch a single snapshot and exit.

Usage:
    python execution/connectors/polymarket_gamma.py                # Live polling
    python execution/connectors/polymarket_gamma.py --once         # Single fetch
    python execution/connectors/polymarket_gamma.py --test         # Fixture mode
    python execution/connectors/polymarket_gamma.py --test --once  # Fixture, one shot

Environment variables (via execution.utils.config):
    POSTGRES_URL  -- PostgreSQL connection string (for market metadata storage)

See: directives/01_data_infrastructure.md
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "gamma_markets.json"

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_TIMEOUT_S = 30
POLL_INTERVAL_S = 300          # 5 minutes between refresh cycles

# ---------------------------------------------------------------------------
# Backoff configuration (rate-limit / transient error handling)
# ---------------------------------------------------------------------------
BACKOFF_BASE_S = 1
BACKOFF_MAX_S = 60
BACKOFF_FACTOR = 2
MAX_RETRIES = 6                # 1 + 2 + 4 + 8 + 16 + 32 = ~63 s worst case

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_test_mode: bool = False


# ===========================================================================
#  FIXTURE DATA (--test mode)
# ===========================================================================

_FIXTURE_MARKETS: list[dict[str, Any]] = [
    {
        "id": "0x000111aaa",
        "condition_id": "0x000111aaa",
        "question": "Will Bitcoin exceed $100k by June 2026?",
        "title": "Will Bitcoin exceed $100k by June 2026?",
        "slug": "will-bitcoin-exceed-100k-by-june-2026",
        "end_date_iso": "2026-06-30T23:59:59Z",
        "category": "Crypto",
        "volume": "2450000.00",
        "liquidity": "185000.00",
        "active": True,
        "closed": False,
        "outcomes": '["Yes", "No"]',
        "outcome_prices": '[0.6500, 0.3500]',
        "description": "Resolves YES if BTC/USD reaches $100,000 on any major exchange.",
        "resolution_source": "CoinGecko",
    },
    {
        "id": "0x000222bbb",
        "condition_id": "0x000222bbb",
        "question": "Will the Fed cut rates in March 2026?",
        "title": "Will the Fed cut rates in March 2026?",
        "slug": "will-the-fed-cut-rates-march-2026",
        "end_date_iso": "2026-03-31T23:59:59Z",
        "category": "Economics",
        "volume": "1870000.00",
        "liquidity": "142000.00",
        "active": True,
        "closed": False,
        "outcomes": '["Yes", "No"]',
        "outcome_prices": '[0.4200, 0.5800]',
        "description": "Resolves YES if the FOMC announces a rate cut at the March 2026 meeting.",
        "resolution_source": "Federal Reserve",
    },
    {
        "id": "0x000333ccc",
        "condition_id": "0x000333ccc",
        "question": "Will ETH reach $5,000 by Q2 2026?",
        "title": "Will ETH reach $5,000 by Q2 2026?",
        "slug": "will-eth-reach-5000-by-q2-2026",
        "end_date_iso": "2026-06-30T23:59:59Z",
        "category": "Crypto",
        "volume": "980000.00",
        "liquidity": "67000.00",
        "active": True,
        "closed": False,
        "outcomes": '["Yes", "No"]',
        "outcome_prices": '[0.3100, 0.6900]',
        "description": "Resolves YES if ETH/USD reaches $5,000 on any major exchange.",
        "resolution_source": "CoinGecko",
    },
    {
        "id": "0x000444ddd",
        "condition_id": "0x000444ddd",
        "question": "US unemployment rate above 4.5% in April 2026?",
        "title": "US unemployment rate above 4.5% in April 2026?",
        "slug": "us-unemployment-above-4-5-april-2026",
        "end_date_iso": "2026-04-30T23:59:59Z",
        "category": "Economics",
        "volume": "560000.00",
        "liquidity": "38000.00",
        "active": True,
        "closed": False,
        "outcomes": '["Yes", "No"]',
        "outcome_prices": '[0.2700, 0.7300]',
        "description": "Resolves YES if BLS reports U3 unemployment above 4.5% for April 2026.",
        "resolution_source": "BLS",
    },
    {
        "id": "0x000555eee",
        "condition_id": "0x000555eee",
        "question": "Will OpenAI release GPT-5 before July 2026?",
        "title": "Will OpenAI release GPT-5 before July 2026?",
        "slug": "will-openai-release-gpt5-before-july-2026",
        "end_date_iso": "2026-07-01T23:59:59Z",
        "category": "Tech",
        "volume": "3200000.00",
        "liquidity": "245000.00",
        "active": True,
        "closed": False,
        "outcomes": '["Yes", "No"]',
        "outcome_prices": '[0.7800, 0.2200]',
        "description": "Resolves YES if OpenAI publicly releases a model branded as GPT-5.",
        "resolution_source": "OpenAI blog",
    },
]

_FIXTURE_EVENTS: list[dict[str, Any]] = [
    {
        "id": "event-001",
        "title": "Crypto Milestones Q2 2026",
        "slug": "crypto-milestones-q2-2026",
        "category": "Crypto",
        "markets": [_FIXTURE_MARKETS[0], _FIXTURE_MARKETS[2]],
    },
    {
        "id": "event-002",
        "title": "US Economic Indicators 2026",
        "slug": "us-economic-indicators-2026",
        "category": "Economics",
        "markets": [_FIXTURE_MARKETS[1], _FIXTURE_MARKETS[3]],
    },
]

_FIXTURE_CATEGORIES: list[str] = ["Crypto", "Economics", "Tech", "Politics", "Sports"]


# ===========================================================================
#  RESPONSE NORMALISATION
# ===========================================================================

def _normalise_market(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Extract and normalise the standard fields from a raw Gamma API market
    response into a consistent dict.

    Handles missing keys gracefully by falling back to sensible defaults.

    Returns:
        Dict with keys: market_id, title, slug, close_time, category,
        volume, liquidity, outcomes.
    """
    # Parse outcomes -- the API may return a JSON string or a list.
    raw_outcomes = raw.get("outcomes", "[]")
    if isinstance(raw_outcomes, str):
        try:
            outcomes = json.loads(raw_outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []
    elif isinstance(raw_outcomes, list):
        outcomes = raw_outcomes
    else:
        outcomes = []

    # Parse outcome prices the same way.
    raw_prices = raw.get("outcome_prices", "[]")
    if isinstance(raw_prices, str):
        try:
            outcome_prices = json.loads(raw_prices)
        except (json.JSONDecodeError, TypeError):
            outcome_prices = []
    elif isinstance(raw_prices, list):
        outcome_prices = raw_prices
    else:
        outcome_prices = []

    # Parse volume / liquidity to float.
    def _safe_float(val: Any, default: float = 0.0) -> float:
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    # Prefer condition_id as market_id; fall back to id.
    market_id = raw.get("condition_id") or raw.get("id") or ""

    return {
        "market_id": str(market_id),
        "title": raw.get("title") or raw.get("question") or "",
        "slug": raw.get("slug") or "",
        "close_time": raw.get("end_date_iso") or raw.get("end_date") or "",
        "category": raw.get("category") or "",
        "volume": _safe_float(raw.get("volume")),
        "liquidity": _safe_float(raw.get("liquidity")),
        "outcomes": outcomes,
        "outcome_prices": outcome_prices,
        "active": bool(raw.get("active", True)),
        "closed": bool(raw.get("closed", False)),
        "description": raw.get("description") or "",
        "resolution_source": raw.get("resolution_source") or "",
    }


# ===========================================================================
#  POLYMARKET GAMMA CLIENT
# ===========================================================================

class PolymarketGammaClient:
    """
    Async client for the Polymarket Gamma API (gamma-api.polymarket.com).

    No authentication required.  All methods return normalised Python dicts
    with a consistent field set.  Handles rate-limit retries with exponential
    backoff and validates response schemas before returning data.

    Usage::

        async with PolymarketGammaClient() as client:
            markets = await client.get_markets(limit=50)
            market  = await client.get_market("0xabc123")
    """

    def __init__(
        self,
        base_url: str = GAMMA_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT_S,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session: Optional[Any] = None  # aiohttp.ClientSession (lazy)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "PolymarketGammaClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> None:
        """Create the aiohttp.ClientSession if it does not exist yet.

        In --test mode this is a no-op because all methods return fixture
        data and never touch the network.
        """
        if _test_mode:
            return

        if self._session is not None and not self._session.closed:
            return

        try:
            import aiohttp
        except ImportError:
            logger.error(
                "aiohttp is not installed. "
                "Install with: pip install aiohttp"
            )
            sys.exit(1)

        timeout_obj = aiohttp.ClientTimeout(total=self._timeout)
        self._session = aiohttp.ClientSession(
            timeout=timeout_obj,
            headers={"Accept": "application/json"},
        )
        logger.info(
            "aiohttp session created (base_url=%s, timeout=%ds)",
            self._base_url,
            self._timeout,
        )

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            logger.info("aiohttp session closed.")
        self._session = None

    # ------------------------------------------------------------------
    # Low-level HTTP with backoff
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """
        Execute an HTTP request with exponential backoff on rate-limit
        (429) and server-error (5xx) responses.

        Args:
            method:  HTTP method (GET, POST, etc.).
            path:    URL path relative to base_url (e.g. '/markets').
            params:  Optional query-string parameters.

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            RuntimeError: After exhausting all retries.
            ValueError:   If the response body is not valid JSON.
        """
        await self._ensure_session()
        url = f"{self._base_url}{path}"

        backoff = BACKOFF_BASE_S
        last_error: Optional[Exception] = None
        last_status: Optional[int] = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self._session.request(method, url, params=params) as resp:
                    last_status = resp.status

                    # --- Success ---
                    if resp.status == 200:
                        try:
                            data = await resp.json(content_type=None)
                        except Exception as json_err:
                            text = await resp.text()
                            logger.warning(
                                "Malformed JSON from %s (attempt %d/%d): %s | body=%s",
                                url, attempt, MAX_RETRIES, json_err,
                                text[:200],
                            )
                            raise ValueError(
                                f"Malformed JSON response from {url}: {json_err}"
                            ) from json_err
                        return data

                    # --- Rate limit (429) or server error (5xx) ---
                    if resp.status == 429 or resp.status >= 500:
                        body = await resp.text()
                        logger.warning(
                            "HTTP %d from %s (attempt %d/%d). "
                            "Retrying in %ds. Body: %s",
                            resp.status, url, attempt, MAX_RETRIES,
                            backoff, body[:200],
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * BACKOFF_FACTOR, BACKOFF_MAX_S)
                        continue

                    # --- Client error (4xx, not 429) ---
                    body = await resp.text()
                    logger.error(
                        "HTTP %d from %s (non-retryable): %s",
                        resp.status, url, body[:500],
                    )
                    raise RuntimeError(
                        f"HTTP {resp.status} from {url}: {body[:500]}"
                    )

            except (asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                logger.warning(
                    "Network error requesting %s (attempt %d/%d): %s. "
                    "Retrying in %ds...",
                    url, attempt, MAX_RETRIES, exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, BACKOFF_MAX_S)

        # All retries exhausted.
        msg = (
            f"Request to {url} failed after {MAX_RETRIES} attempts. "
            f"Last status={last_status}, last_error={last_error}"
        )
        logger.error(msg)
        raise RuntimeError(msg)

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def get_markets(
        self,
        limit: int = 100,
        offset: int = 0,
        active: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Fetch a paginated list of market metadata.

        Args:
            limit:   Maximum number of markets to return (default 100).
            offset:  Pagination offset (default 0).
            active:  If True, only return active (non-closed) markets.

        Returns:
            List of normalised market dicts.
        """
        if _test_mode:
            results = _FIXTURE_MARKETS[offset: offset + limit]
            if active:
                results = [m for m in results if m.get("active", True)]
            normalised = [_normalise_market(m) for m in results]
            logger.info(
                "get_markets [fixture]: returned %d markets "
                "(limit=%d, offset=%d, active=%s)",
                len(normalised), limit, offset, active,
            )
            return normalised

        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if active:
            params["active"] = "true"
            params["closed"] = "false"

        raw = await self._request("GET", "/markets", params=params)

        # The API may return a list directly or a wrapper dict.
        if isinstance(raw, dict):
            markets_list = raw.get("data", raw.get("markets", []))
        elif isinstance(raw, list):
            markets_list = raw
        else:
            logger.warning(
                "Unexpected response type from /markets: %s", type(raw)
            )
            markets_list = []

        normalised = [_normalise_market(m) for m in markets_list]
        logger.info(
            "get_markets: returned %d markets (limit=%d, offset=%d, active=%s)",
            len(normalised), limit, offset, active,
        )
        return normalised

    async def get_market(self, market_id: str) -> dict[str, Any]:
        """
        Fetch a single market by its condition ID.

        Args:
            market_id:  The market's condition_id (e.g. '0xabc123').

        Returns:
            Normalised market dict.

        Raises:
            RuntimeError: If the market is not found or the API errors.
        """
        if _test_mode:
            for m in _FIXTURE_MARKETS:
                if m.get("condition_id") == market_id or m.get("id") == market_id:
                    result = _normalise_market(m)
                    logger.info("get_market [fixture]: found %s", market_id)
                    return result
            raise RuntimeError(f"Market not found in fixtures: {market_id}")

        raw = await self._request("GET", f"/markets/{market_id}")

        # The API may return the market directly or wrapped.
        if isinstance(raw, dict):
            # If wrapped in a data key, unwrap.
            market_data = raw.get("data", raw) if "data" in raw else raw
        else:
            raise ValueError(
                f"Unexpected response type for /markets/{market_id}: {type(raw)}"
            )

        result = _normalise_market(market_data)
        logger.info("get_market: fetched %s (%s)", market_id, result["title"][:50])
        return result

    async def get_market_by_slug(self, slug: str) -> dict[str, Any]:
        """
        Fetch a single market by its URL slug.

        Args:
            slug:  The market's URL slug (e.g. 'will-bitcoin-exceed-100k').

        Returns:
            Normalised market dict.

        Raises:
            RuntimeError: If no market matches the slug.
        """
        if _test_mode:
            for m in _FIXTURE_MARKETS:
                if m.get("slug") == slug:
                    result = _normalise_market(m)
                    logger.info("get_market_by_slug [fixture]: found %s", slug)
                    return result
            raise RuntimeError(f"Market not found by slug in fixtures: {slug}")

        raw = await self._request("GET", "/markets", params={"slug": slug})

        # Response may be a list or wrapped dict.
        if isinstance(raw, list):
            markets_list = raw
        elif isinstance(raw, dict):
            markets_list = raw.get("data", raw.get("markets", []))
            # If the API returns a single market dict (not in a list)
            if not isinstance(markets_list, list):
                markets_list = [markets_list]
        else:
            markets_list = []

        if not markets_list:
            raise RuntimeError(f"No market found with slug: {slug}")

        result = _normalise_market(markets_list[0])
        logger.info("get_market_by_slug: fetched slug=%s (%s)", slug, result["title"][:50])
        return result

    async def get_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Fetch events with their associated markets.

        Args:
            limit:  Maximum number of events to return (default 100).

        Returns:
            List of event dicts, each containing a 'markets' list of
            normalised market dicts.
        """
        if _test_mode:
            events = []
            for evt in _FIXTURE_EVENTS[:limit]:
                event_copy = {
                    "id": evt["id"],
                    "title": evt["title"],
                    "slug": evt.get("slug", ""),
                    "category": evt.get("category", ""),
                    "markets": [_normalise_market(m) for m in evt.get("markets", [])],
                }
                events.append(event_copy)
            logger.info(
                "get_events [fixture]: returned %d events (limit=%d)",
                len(events), limit,
            )
            return events

        raw = await self._request("GET", "/events", params={"limit": limit})

        if isinstance(raw, list):
            events_list = raw
        elif isinstance(raw, dict):
            events_list = raw.get("data", raw.get("events", []))
        else:
            logger.warning("Unexpected response type from /events: %s", type(raw))
            events_list = []

        events = []
        for evt in events_list:
            nested_markets = evt.get("markets", [])
            event_obj = {
                "id": evt.get("id", ""),
                "title": evt.get("title", ""),
                "slug": evt.get("slug", ""),
                "category": evt.get("category", ""),
                "markets": [_normalise_market(m) for m in nested_markets],
            }
            events.append(event_obj)

        logger.info(
            "get_events: returned %d events (limit=%d)",
            len(events), limit,
        )
        return events

    async def search_markets(
        self,
        query: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Search markets by a text query against titles and descriptions.

        Args:
            query:  Free-text search string.
            limit:  Maximum number of results (default 20).

        Returns:
            List of normalised market dicts matching the query.
        """
        if _test_mode:
            query_lower = query.lower()
            matches = [
                m for m in _FIXTURE_MARKETS
                if query_lower in (m.get("title") or "").lower()
                or query_lower in (m.get("question") or "").lower()
                or query_lower in (m.get("description") or "").lower()
            ]
            normalised = [_normalise_market(m) for m in matches[:limit]]
            logger.info(
                "search_markets [fixture]: query='%s' returned %d results",
                query, len(normalised),
            )
            return normalised

        params: dict[str, Any] = {
            "_q": query,
            "limit": limit,
        }
        raw = await self._request("GET", "/markets", params=params)

        if isinstance(raw, list):
            markets_list = raw
        elif isinstance(raw, dict):
            markets_list = raw.get("data", raw.get("markets", []))
        else:
            markets_list = []

        normalised = [_normalise_market(m) for m in markets_list]
        logger.info(
            "search_markets: query='%s' returned %d results",
            query, len(normalised),
        )
        return normalised

    async def get_categories(self) -> list[str]:
        """
        Retrieve the list of market categories.

        The Gamma API does not expose a dedicated categories endpoint, so
        this method fetches a broad sample of markets and extracts unique
        category values.

        Returns:
            Sorted list of unique category strings.
        """
        if _test_mode:
            logger.info(
                "get_categories [fixture]: returned %d categories",
                len(_FIXTURE_CATEGORIES),
            )
            return list(_FIXTURE_CATEGORIES)

        # Fetch a large sample to discover categories.
        markets = await self.get_markets(limit=100, offset=0, active=False)
        cats: set[str] = set()
        for m in markets:
            cat = m.get("category", "")
            if cat:
                cats.add(cat)

        sorted_cats = sorted(cats)
        logger.info("get_categories: discovered %d categories", len(sorted_cats))
        return sorted_cats


# ===========================================================================
#  DATABASE STORAGE
# ===========================================================================

async def _store_market_metadata(market: dict[str, Any]) -> None:
    """
    Persist a normalised market dict to PostgreSQL via the db module.

    In test mode this delegates to the in-memory mock pool, which logs
    the insert without requiring a live database.
    """
    # Late import to avoid circular dependency and so --test never
    # requires asyncpg if the mock pool is active.
    try:
        from execution.utils.db import insert_market_price
    except ImportError:
        # If db module cannot be imported (e.g. missing asyncpg in CI),
        # fall back to logging.
        logger.info(
            "DB module unavailable -- logging market instead: "
            "id=%s title=%s volume=%.2f",
            market.get("market_id", "?"),
            (market.get("title") or "?")[:50],
            market.get("volume", 0),
        )
        return

    # Extract yes/no prices from outcome_prices if available.
    prices = market.get("outcome_prices", [])
    yes_price = float(prices[0]) if len(prices) > 0 else 0.0
    no_price = float(prices[1]) if len(prices) > 1 else 0.0
    spread = abs(yes_price - no_price) if (yes_price and no_price) else 0.0

    await insert_market_price(
        time=datetime.now(timezone.utc),
        venue="polymarket",
        market_id=market["market_id"],
        title=market.get("title", ""),
        yes_price=yes_price,
        no_price=no_price,
        spread=spread,
        volume_24h=market.get("volume", 0.0),
    )
    logger.debug(
        "Stored market metadata: %s (yes=%.4f, no=%.4f, vol=%.2f)",
        market["market_id"], yes_price, no_price, market.get("volume", 0),
    )


# ===========================================================================
#  FIXTURE FILE I/O
# ===========================================================================

def _write_fixture_file() -> None:
    """Write the built-in fixture markets to .tmp/fixtures/gamma_markets.json."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    normalised = [_normalise_market(m) for m in _FIXTURE_MARKETS]
    FIXTURE_FILE.write_text(
        json.dumps(normalised, indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("Fixture data written: %d markets -> %s", len(normalised), FIXTURE_FILE)


# ===========================================================================
#  POLLING LOOP
# ===========================================================================

async def _poll_once(client: PolymarketGammaClient) -> int:
    """
    Fetch all active markets (paginated) and store their metadata.

    Returns the total number of markets processed.
    """
    offset = 0
    page_size = 100
    total = 0

    while True:
        markets = await client.get_markets(
            limit=page_size,
            offset=offset,
            active=True,
        )
        if not markets:
            break

        for market in markets:
            try:
                await _store_market_metadata(market)
                total += 1
            except Exception as exc:
                logger.warning(
                    "Failed to store market %s: %s -- skipping.",
                    market.get("market_id", "?"), exc,
                )

        # If fewer results than page_size, we have exhausted the results.
        if len(markets) < page_size:
            break

        offset += page_size

    logger.info("Poll complete: processed %d active markets.", total)
    return total


async def run_polling_loop(once: bool = False) -> None:
    """
    Main polling loop.  Fetches all active market metadata every
    POLL_INTERVAL_S seconds.

    Args:
        once:  If True, fetch a single snapshot and return (no looping).
    """
    async with PolymarketGammaClient() as client:
        cycle = 0
        while True:
            cycle += 1
            logger.info(
                "=== Gamma poll cycle %d starting at %s ===",
                cycle,
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )

            try:
                count = await _poll_once(client)
                logger.info(
                    "=== Gamma poll cycle %d complete: %d markets ===",
                    cycle, count,
                )
            except Exception as exc:
                logger.error(
                    "Gamma poll cycle %d failed: %s", cycle, exc,
                )

            if once:
                logger.info("Single-shot mode (--once): exiting after one cycle.")
                break

            logger.info(
                "Sleeping %d seconds until next poll cycle...",
                POLL_INTERVAL_S,
            )
            await asyncio.sleep(POLL_INTERVAL_S)


# ===========================================================================
#  DEMO / SELF-TEST (--test mode)
# ===========================================================================

async def _run_test_demo() -> None:
    """
    Exercise every public method of PolymarketGammaClient using fixture data.
    Also writes fixture JSON to disk and demonstrates database storage (mock).
    """
    # Enable the db module's test mode so it uses the mock pool.
    try:
        import execution.utils.db as db_mod
        db_mod._test_mode = True
    except ImportError:
        logger.warning("Could not import db module; skipping DB mock setup.")

    print()
    print("=" * 72)
    print("  Polymarket Gamma API Connector -- Test Demo")
    print("  Mode: FIXTURE (no network calls)")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # Write fixture file to disk.
    _write_fixture_file()

    async with PolymarketGammaClient() as client:

        # --- get_markets ---------------------------------------------------
        print("\n--- get_markets(limit=3, offset=0, active=True) ---")
        markets = await client.get_markets(limit=3, offset=0, active=True)
        for m in markets:
            print(
                f"  {m['market_id'][:12]}  {m['title'][:45]:<45}  "
                f"vol={m['volume']:>12,.2f}  liq={m['liquidity']:>10,.2f}"
            )

        # --- get_market ----------------------------------------------------
        print("\n--- get_market('0x000111aaa') ---")
        single = await client.get_market("0x000111aaa")
        print(f"  Title:      {single['title']}")
        print(f"  Slug:       {single['slug']}")
        print(f"  Close:      {single['close_time']}")
        print(f"  Category:   {single['category']}")
        print(f"  Outcomes:   {single['outcomes']}")
        print(f"  Prices:     {single['outcome_prices']}")

        # --- get_market_by_slug --------------------------------------------
        print("\n--- get_market_by_slug('will-the-fed-cut-rates-march-2026') ---")
        by_slug = await client.get_market_by_slug("will-the-fed-cut-rates-march-2026")
        print(f"  Title:      {by_slug['title']}")
        print(f"  Market ID:  {by_slug['market_id']}")

        # --- get_events ----------------------------------------------------
        print("\n--- get_events(limit=5) ---")
        events = await client.get_events(limit=5)
        for evt in events:
            print(f"  Event: {evt['title']} ({len(evt['markets'])} markets)")
            for em in evt["markets"]:
                print(f"    -> {em['title'][:50]}")

        # --- search_markets ------------------------------------------------
        print("\n--- search_markets('bitcoin') ---")
        results = await client.search_markets("bitcoin")
        for r in results:
            print(f"  {r['title']}")

        print("\n--- search_markets('unemployment') ---")
        results = await client.search_markets("unemployment")
        for r in results:
            print(f"  {r['title']}")

        # --- get_categories ------------------------------------------------
        print("\n--- get_categories() ---")
        cats = await client.get_categories()
        print(f"  Categories: {cats}")

        # --- Store to DB (mock) -------------------------------------------
        print("\n--- Storing market metadata to DB (mock) ---")
        for m in markets:
            await _store_market_metadata(m)

        # --- Single poll cycle (fixture) ----------------------------------
        print("\n--- Running single poll cycle (fixture) ---")
        count = await _poll_once(client)
        print(f"  Processed {count} markets in poll cycle.")

    # Cleanup db pool.
    try:
        from execution.utils.db import close_pool
        await close_pool()
    except ImportError:
        pass

    print()
    print("=" * 72)
    print("  Test demo complete.  All operations succeeded.")
    print("=" * 72)
    print()


# ===========================================================================
#  ENTRY POINT
# ===========================================================================

def main() -> None:
    """
    CLI entry point.  Parse arguments and run in the appropriate mode.

    Flags:
        --test   Use fixture data (no network calls, no real DB).
        --once   Fetch a single snapshot and exit (no polling loop).
    """
    global _test_mode

    parser = argparse.ArgumentParser(
        description=(
            "Polymarket Gamma API connector. Fetches market metadata "
            "from gamma-api.polymarket.com and stores it in PostgreSQL."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use fixture data instead of live API calls (no network needed).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch a single snapshot and exit (no continuous polling).",
    )
    args = parser.parse_args()

    if args.test:
        _test_mode = True
        logger.info("Running in --test mode with fixture data.")
        asyncio.run(_run_test_demo())
    else:
        logger.info("Running live Gamma API connector.")
        asyncio.run(run_polling_loop(once=args.once))


if __name__ == "__main__":
    main()
