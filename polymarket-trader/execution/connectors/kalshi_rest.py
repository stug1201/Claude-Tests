#!/usr/bin/env python3
"""
kalshi_rest.py -- Kalshi REST API connector for polymarket-trader.

Handles RSA-PSS token authentication with 30-minute TTL, proactive token
refresh at the 25-minute mark, market queries, order placement, position
tracking, and publishes raw price ticks to stream:prices:kalshi via Redis
Streams.

Base URL: https://api.elections.kalshi.com/trade-api/v2
Auth: RSA-PSS token (POST /login with email + password)

CRITICAL: Token expires every 30 minutes.  Must refresh at the 25-minute
mark (5 minutes BEFORE expiry).  A background asyncio.Task handles this
automatically once login() is called.

Endpoints implemented:
    POST   /trade-api/v2/login                 -- authenticate
    GET    /trade-api/v2/markets               -- list markets
    GET    /trade-api/v2/markets/{ticker}      -- single market detail
    GET    /trade-api/v2/markets/{ticker}/orderbook -- orderbook snapshot
    POST   /trade-api/v2/portfolio/orders      -- place order
    DELETE /trade-api/v2/portfolio/orders/{id}  -- cancel order
    GET    /trade-api/v2/portfolio/positions    -- current positions
    GET    /trade-api/v2/portfolio/balance      -- portfolio balance

Error handling:
    - Auth failures: log full response body, raise
    - Rate limits: honour Retry-After header, exponential backoff
    - 401 Unauthorized: trigger immediate token refresh and retry once
    - Network timeouts: 30s default, log and raise

Usage:
    python execution/connectors/kalshi_rest.py          # Live mode
    python execution/connectors/kalshi_rest.py --test   # Fixture mode

Environment variables (via execution.utils.config):
    KALSHI_EMAIL     -- Kalshi account email
    KALSHI_PASSWORD  -- Kalshi account password
    KALSHI_API_KEY   -- Kalshi API key (optional, for enhanced access)

See: directives/01_data_infrastructure.md
"""

import argparse
import asyncio
import json
import logging
import sys
import time
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
SCRIPT_DIR = Path(__file__).resolve().parent            # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                       # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                     # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "kalshi_rest_fixtures.json"

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Auth endpoints
PATH_LOGIN = "/trade-api/v2/login"

# Market data endpoints
PATH_MARKETS = "/trade-api/v2/markets"
PATH_MARKET_DETAIL = "/trade-api/v2/markets/{ticker}"
PATH_ORDERBOOK = "/trade-api/v2/markets/{ticker}/orderbook"

# Portfolio / order endpoints
PATH_PLACE_ORDER = "/trade-api/v2/portfolio/orders"
PATH_CANCEL_ORDER = "/trade-api/v2/portfolio/orders/{order_id}"
PATH_POSITIONS = "/trade-api/v2/portfolio/positions"
PATH_BALANCE = "/trade-api/v2/portfolio/balance"

# ---------------------------------------------------------------------------
# Token timing constants
# ---------------------------------------------------------------------------
TOKEN_TTL_SECONDS = 30 * 60          # 30 minutes
TOKEN_REFRESH_SECONDS = 25 * 60      # 25 minutes (refresh 5 min before expiry)
TOKEN_REFRESH_MARGIN_SECONDS = 5 * 60  # 5-minute safety margin

# ---------------------------------------------------------------------------
# HTTP / retry constants
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT_SECONDS = 30
RATE_LIMIT_BACKOFF_BASE = 1          # seconds
RATE_LIMIT_BACKOFF_MAX = 60          # seconds
RATE_LIMIT_MAX_RETRIES = 5

# ---------------------------------------------------------------------------
# Redis stream name
# ---------------------------------------------------------------------------
STREAM_PRICES_KALSHI = "stream:prices:kalshi"

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_test_mode: bool = False


# ===========================================================================
#  FIXTURE DATA
# ===========================================================================

_FIXTURE_MARKETS: list[dict] = [
    {
        "ticker": "KXBTC-26MAR28-100K",
        "title": "Will Bitcoin exceed $100k by March 28, 2026?",
        "status": "open",
        "yes_price": 0.65,
        "no_price": 0.35,
        "volume": 245000,
        "open_interest": 18500,
        "close_time": "2026-03-28T23:59:59Z",
        "category": "Crypto",
    },
    {
        "ticker": "KXETH-26Q2-5K",
        "title": "Will Ethereum reach $5k by Q2 2026?",
        "status": "open",
        "yes_price": 0.42,
        "no_price": 0.58,
        "volume": 132000,
        "open_interest": 9200,
        "close_time": "2026-06-30T23:59:59Z",
        "category": "Crypto",
    },
    {
        "ticker": "KXUNEMP-26MAR-4.5",
        "title": "US unemployment rate above 4.5% in March 2026?",
        "status": "open",
        "yes_price": 0.31,
        "no_price": 0.69,
        "volume": 78000,
        "open_interest": 5400,
        "close_time": "2026-04-15T23:59:59Z",
        "category": "Economics",
    },
]

_FIXTURE_ORDERBOOK: dict = {
    "ticker": "KXBTC-26MAR28-100K",
    "yes_bids": [
        {"price": 0.64, "quantity": 500},
        {"price": 0.63, "quantity": 800},
        {"price": 0.62, "quantity": 1200},
    ],
    "yes_asks": [
        {"price": 0.66, "quantity": 450},
        {"price": 0.67, "quantity": 700},
        {"price": 0.68, "quantity": 1100},
    ],
    "no_bids": [
        {"price": 0.34, "quantity": 400},
        {"price": 0.33, "quantity": 650},
    ],
    "no_asks": [
        {"price": 0.36, "quantity": 500},
        {"price": 0.37, "quantity": 750},
    ],
}

_FIXTURE_POSITIONS: list[dict] = [
    {
        "ticker": "KXBTC-26MAR28-100K",
        "side": "yes",
        "quantity": 100,
        "avg_price": 0.62,
        "market_value": 65.00,
        "unrealised_pnl": 3.00,
    },
]

_FIXTURE_BALANCE: dict = {
    "balance": 5000.00,
    "available_balance": 4350.00,
    "reserved_balance": 650.00,
    "portfolio_value": 5650.00,
}

_FIXTURE_ORDER_COUNTER: int = 0


def _get_all_fixtures() -> dict:
    """Return a dict containing all fixture data for serialisation."""
    return {
        "markets": _FIXTURE_MARKETS,
        "orderbook": _FIXTURE_ORDERBOOK,
        "positions": _FIXTURE_POSITIONS,
        "balance": _FIXTURE_BALANCE,
    }


def _write_fixture_file() -> None:
    """Write fixture data to .tmp/fixtures/kalshi_rest_fixtures.json."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    FIXTURE_FILE.write_text(
        json.dumps(_get_all_fixtures(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Fixture data written to %s", FIXTURE_FILE)


# ===========================================================================
#  KALSHI REST CLIENT
# ===========================================================================

class KalshiRESTClient:
    """
    Async REST client for the Kalshi prediction market API.

    Manages RSA-PSS token authentication with automatic background refresh.
    All HTTP methods go through _request(), which ensures a valid token is
    present before every call.

    Args:
        email:    Kalshi account email (from config KALSHI_EMAIL).
        password: Kalshi account password (from config KALSHI_PASSWORD).
        api_key:  Kalshi API key for enhanced access (optional).

    Example::

        client = KalshiRESTClient(email, password, api_key)
        token = await client.login()
        markets = await client.get_markets(limit=50)
        await client.close()
    """

    def __init__(
        self,
        email: str,
        password: str,
        api_key: Optional[str] = None,
    ) -> None:
        self._email = email
        self._password = password
        self._api_key = api_key
        self._base_url = BASE_URL

        # Token state
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0          # Unix timestamp
        self._token_obtained_at: float = 0.0     # Unix timestamp
        self._refresh_task: Optional[asyncio.Task] = None

        # aiohttp session (created lazily)
        self._session: Optional[Any] = None

        logger.info(
            "KalshiRESTClient initialised for email=%s api_key=%s",
            email[:4] + "****" if email else "<none>",
            (api_key[:4] + "****") if api_key else "<none>",
        )

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _get_session(self):
        """Return the aiohttp.ClientSession, creating it lazily."""
        if self._session is None or self._session.closed:
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            self._session = aiohttp.ClientSession(timeout=timeout)
            logger.debug("aiohttp session created (timeout=%ds)", REQUEST_TIMEOUT_SECONDS)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session and cancel the background refresh task."""
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            logger.info("Background token refresh task cancelled.")
            self._refresh_task = None

        if self._session is not None and not self._session.closed:
            await self._session.close()
            logger.info("aiohttp session closed.")
            self._session = None

        self._token = None
        self._token_expiry = 0.0
        self._token_obtained_at = 0.0

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def login(self) -> str:
        """
        Authenticate with Kalshi via POST /trade-api/v2/login.

        Sends email + password, receives a bearer token and its expiry.
        Stores the token internally and starts a background refresh task
        that will re-authenticate at the 25-minute mark.

        Returns:
            The bearer token string.

        Raises:
            RuntimeError: If authentication fails (logs full response body).
        """
        session = await self._get_session()
        url = f"{self._base_url.rstrip('/trade-api/v2')}{PATH_LOGIN}"
        # Normalise URL: ensure we hit the base host + path
        # BASE_URL = https://api.elections.kalshi.com/trade-api/v2
        # PATH_LOGIN = /trade-api/v2/login
        # We want: https://api.elections.kalshi.com/trade-api/v2/login
        url = f"{BASE_URL}/login"

        payload = {
            "email": self._email,
            "password": self._password,
        }

        logger.info("Authenticating with Kalshi API (email=%s)...", self._email[:4] + "****")

        try:
            async with session.post(url, json=payload) as resp:
                body = await resp.text()

                if resp.status != 200:
                    logger.error(
                        "Kalshi login failed (status=%d). Response body: %s",
                        resp.status,
                        body,
                    )
                    raise RuntimeError(
                        f"Kalshi login failed with status {resp.status}: {body}"
                    )

                data = json.loads(body)

        except RuntimeError:
            raise
        except Exception as exc:
            logger.error("Kalshi login network error: %s", exc)
            raise RuntimeError(f"Kalshi login network error: {exc}") from exc

        # Extract token and expiry from response
        self._token = data.get("token")
        if not self._token:
            logger.error("Kalshi login response missing 'token' field: %s", body)
            raise RuntimeError("Kalshi login response missing 'token' field")

        # The API returns an expiry timestamp (ISO 8601 or unix).
        # Handle both formats defensively.
        now = time.time()
        self._token_obtained_at = now

        expiry_raw = data.get("expiry") or data.get("expires_at")
        if expiry_raw:
            if isinstance(expiry_raw, (int, float)):
                self._token_expiry = float(expiry_raw)
            elif isinstance(expiry_raw, str):
                try:
                    dt = datetime.fromisoformat(expiry_raw.replace("Z", "+00:00"))
                    self._token_expiry = dt.timestamp()
                except ValueError:
                    # Fallback: assume 30-minute TTL from now
                    self._token_expiry = now + TOKEN_TTL_SECONDS
                    logger.warning(
                        "Could not parse expiry '%s', assuming 30min TTL.",
                        expiry_raw,
                    )
            else:
                self._token_expiry = now + TOKEN_TTL_SECONDS
        else:
            # No expiry in response: assume 30-minute TTL
            self._token_expiry = now + TOKEN_TTL_SECONDS

        remaining = self._token_expiry - now
        logger.info(
            "Kalshi login successful. Token expires in %.0f seconds (%.1f min).",
            remaining,
            remaining / 60,
        )

        # Start (or restart) the background refresh task
        self._start_refresh_task()

        return self._token

    def _start_refresh_task(self) -> None:
        """Start the background token refresh asyncio.Task."""
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()

        self._refresh_task = asyncio.create_task(
            self._background_refresh_loop(),
            name="kalshi-token-refresh",
        )
        logger.info("Background token refresh task started.")

    async def _background_refresh_loop(self) -> None:
        """
        Background loop that refreshes the token at the 25-minute mark.

        Runs continuously until cancelled.  After each refresh, sleeps
        until the next 25-minute mark.
        """
        while True:
            try:
                # Calculate how long until we need to refresh
                now = time.time()
                elapsed = now - self._token_obtained_at
                sleep_until_refresh = max(0, TOKEN_REFRESH_SECONDS - elapsed)

                if sleep_until_refresh > 0:
                    logger.info(
                        "Token refresh scheduled in %.0f seconds (%.1f min).",
                        sleep_until_refresh,
                        sleep_until_refresh / 60,
                    )
                    await asyncio.sleep(sleep_until_refresh)

                # Refresh the token
                logger.info("Proactive token refresh triggered (25-minute mark).")
                await self.login()

                # After login() restarts this task, this instance should exit.
                # But login() cancels the old task and creates a new one, so
                # this coroutine will receive CancelledError.  In case it
                # doesn't (race condition), we break to avoid duplication.
                break

            except asyncio.CancelledError:
                logger.info("Background refresh loop cancelled.")
                raise
            except Exception as exc:
                logger.error(
                    "Background token refresh failed: %s. Retrying in 30s.",
                    exc,
                )
                await asyncio.sleep(30)

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    async def _ensure_token(self) -> None:
        """
        Check the current token's validity and refresh if needed.

        Refreshes proactively if less than 5 minutes remain before expiry.
        This is called before every API request by _request().
        """
        if self._token is None:
            logger.warning("No token present; calling login().")
            await self.login()
            return

        now = time.time()
        remaining = self._token_expiry - now

        if remaining <= 0:
            logger.warning(
                "Token has expired (%.0fs ago). Re-authenticating.",
                abs(remaining),
            )
            await self.login()
        elif remaining < TOKEN_REFRESH_MARGIN_SECONDS:
            logger.info(
                "Token expiring soon (%.0fs remaining). Proactive refresh.",
                remaining,
            )
            await self.login()
        else:
            logger.debug(
                "Token valid for %.0fs (%.1f min).",
                remaining,
                remaining / 60,
            )

    # ------------------------------------------------------------------
    # Core HTTP request method
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict:
        """
        Execute an authenticated HTTP request against the Kalshi API.

        Calls _ensure_token() before every request.  Handles:
        - 401 Unauthorized: immediate token refresh + single retry
        - 429 Rate Limited: honour Retry-After header with exp backoff
        - Network timeouts: raise after REQUEST_TIMEOUT_SECONDS

        Args:
            method:  HTTP method (GET, POST, DELETE, etc.).
            path:    API path (e.g. /trade-api/v2/markets).
            **kwargs: Passed through to aiohttp (json, params, etc.).

        Returns:
            Parsed JSON response as a dict.

        Raises:
            RuntimeError: On non-recoverable API errors.
        """
        await self._ensure_token()

        session = await self._get_session()
        url = f"https://api.elections.kalshi.com{path}"

        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._api_key:
            headers["X-Api-Key"] = self._api_key

        # Merge any caller-provided headers
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        backoff = RATE_LIMIT_BACKOFF_BASE

        for attempt in range(1, RATE_LIMIT_MAX_RETRIES + 1):
            try:
                async with session.request(
                    method, url, headers=headers, **kwargs
                ) as resp:
                    body = await resp.text()

                    # -- Success -------------------------------------------
                    if 200 <= resp.status < 300:
                        if not body or body.strip() == "":
                            return {}
                        return json.loads(body)

                    # -- 401 Unauthorized: refresh token and retry once ----
                    if resp.status == 401:
                        if attempt == 1:
                            logger.warning(
                                "401 Unauthorized on %s %s. "
                                "Refreshing token and retrying...",
                                method, path,
                            )
                            await self.login()
                            headers["Authorization"] = f"Bearer {self._token}"
                            continue
                        else:
                            logger.error(
                                "401 Unauthorized on %s %s after token "
                                "refresh. Response: %s",
                                method, path, body,
                            )
                            raise RuntimeError(
                                f"Kalshi 401 after refresh on {method} {path}: {body}"
                            )

                    # -- 429 Rate Limited ----------------------------------
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                wait = float(retry_after)
                            except ValueError:
                                wait = backoff
                        else:
                            wait = backoff

                        wait = min(wait, RATE_LIMIT_BACKOFF_MAX)
                        logger.warning(
                            "429 Rate limited on %s %s (attempt %d/%d). "
                            "Retry-After: %s. Waiting %.1fs...",
                            method, path, attempt, RATE_LIMIT_MAX_RETRIES,
                            retry_after, wait,
                        )
                        await asyncio.sleep(wait)
                        backoff = min(backoff * 2, RATE_LIMIT_BACKOFF_MAX)
                        continue

                    # -- Other errors --------------------------------------
                    logger.error(
                        "Kalshi API error: %s %s status=%d body=%s",
                        method, path, resp.status, body,
                    )
                    raise RuntimeError(
                        f"Kalshi API error {resp.status} on {method} {path}: {body}"
                    )

            except (RuntimeError, asyncio.CancelledError):
                raise
            except Exception as exc:
                if attempt < RATE_LIMIT_MAX_RETRIES:
                    logger.warning(
                        "Network error on %s %s (attempt %d/%d): %s. "
                        "Retrying in %.1fs...",
                        method, path, attempt, RATE_LIMIT_MAX_RETRIES,
                        exc, backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, RATE_LIMIT_BACKOFF_MAX)
                else:
                    logger.error(
                        "Network error on %s %s after %d attempts: %s",
                        method, path, RATE_LIMIT_MAX_RETRIES, exc,
                    )
                    raise RuntimeError(
                        f"Kalshi network error on {method} {path} "
                        f"after {RATE_LIMIT_MAX_RETRIES} attempts: {exc}"
                    ) from exc

        # Should not reach here, but just in case
        raise RuntimeError(f"Kalshi request exhausted retries on {method} {path}")

    # ------------------------------------------------------------------
    # Market data endpoints
    # ------------------------------------------------------------------

    async def get_markets(
        self,
        limit: int = 100,
        cursor: Optional[str] = None,
        status: str = "open",
    ) -> list[dict]:
        """
        Fetch a list of markets from Kalshi.

        GET /trade-api/v2/markets

        Args:
            limit:   Maximum number of markets to return (default 100).
            cursor:  Pagination cursor from a previous response.
            status:  Market status filter (default 'open').

        Returns:
            List of market dicts.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "status": status,
        }
        if cursor:
            params["cursor"] = cursor

        data = await self._request("GET", PATH_MARKETS, params=params)
        markets = data.get("markets", [])
        logger.info(
            "get_markets: returned %d markets (limit=%d, status=%s).",
            len(markets), limit, status,
        )
        return markets

    async def get_market(self, ticker: str) -> dict:
        """
        Fetch detail for a single market.

        GET /trade-api/v2/markets/{ticker}

        Args:
            ticker:  Kalshi market ticker (e.g. 'KXBTC-26MAR28-100K').

        Returns:
            Market detail dict.
        """
        path = PATH_MARKET_DETAIL.format(ticker=ticker)
        data = await self._request("GET", path)
        market = data.get("market", data)
        logger.info("get_market: ticker=%s title=%s", ticker, market.get("title", "?"))
        return market

    async def get_orderbook(self, ticker: str) -> dict:
        """
        Fetch the full orderbook for a market.

        GET /trade-api/v2/markets/{ticker}/orderbook

        Args:
            ticker:  Kalshi market ticker.

        Returns:
            Orderbook dict with bids and asks.
        """
        path = PATH_ORDERBOOK.format(ticker=ticker)
        data = await self._request("GET", path)
        orderbook = data.get("orderbook", data)
        logger.info(
            "get_orderbook: ticker=%s",
            ticker,
        )
        return orderbook

    # ------------------------------------------------------------------
    # Order endpoints
    # ------------------------------------------------------------------

    async def place_order(
        self,
        ticker: str,
        side: str,
        price: float,
        size: int,
        order_type: str = "limit",
    ) -> dict:
        """
        Place an order on a Kalshi market.

        POST /trade-api/v2/portfolio/orders

        Args:
            ticker:     Market ticker.
            side:       'yes' or 'no'.
            price:      Price in cents (integer 1-99) or decimal (0.01-0.99).
            size:       Number of contracts.
            order_type: Order type ('limit' or 'market').

        Returns:
            Order confirmation dict (includes order_id, status).
        """
        # Normalise price: Kalshi API expects cents (integer 1-99)
        if isinstance(price, float) and price < 1.0:
            price_cents = int(round(price * 100))
        else:
            price_cents = int(price)

        payload = {
            "ticker": ticker,
            "action": "buy" if side.lower() in ("yes", "buy") else "sell",
            "side": side.lower(),
            "type": order_type,
            "count": size,
            "yes_price": price_cents if side.lower() == "yes" else None,
            "no_price": price_cents if side.lower() == "no" else None,
        }
        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        data = await self._request("POST", PATH_PLACE_ORDER, json=payload)
        order = data.get("order", data)
        logger.info(
            "place_order: ticker=%s side=%s price=%d size=%d -> order_id=%s",
            ticker, side, price_cents, size, order.get("order_id", "?"),
        )
        return order

    async def cancel_order(self, order_id: str) -> dict:
        """
        Cancel an open order.

        DELETE /trade-api/v2/portfolio/orders/{order_id}

        Args:
            order_id:  The order ID to cancel.

        Returns:
            Cancellation confirmation dict.
        """
        path = PATH_CANCEL_ORDER.format(order_id=order_id)
        data = await self._request("DELETE", path)
        logger.info("cancel_order: order_id=%s cancelled.", order_id)
        return data

    # ------------------------------------------------------------------
    # Portfolio endpoints
    # ------------------------------------------------------------------

    async def get_positions(self) -> list[dict]:
        """
        Fetch current portfolio positions.

        GET /trade-api/v2/portfolio/positions

        Returns:
            List of position dicts.
        """
        data = await self._request("GET", PATH_POSITIONS)
        positions = data.get("market_positions", data.get("positions", []))
        if isinstance(positions, dict):
            positions = [positions]
        logger.info("get_positions: %d positions.", len(positions))
        return positions

    async def get_portfolio_balance(self) -> dict:
        """
        Fetch portfolio balance.

        GET /trade-api/v2/portfolio/balance

        Returns:
            Balance dict with available_balance, portfolio_value, etc.
        """
        data = await self._request("GET", PATH_BALANCE)
        logger.info(
            "get_portfolio_balance: balance=%s",
            data.get("balance", "?"),
        )
        return data


# ===========================================================================
#  TEST-MODE CLIENT (no network calls)
# ===========================================================================

class KalshiRESTClientTest:
    """
    Test-mode stand-in for KalshiRESTClient.

    Returns fixture data for every method.  No network calls are made.
    No aiohttp dependency required.
    """

    def __init__(
        self,
        email: str = "test@example.com",
        password: str = "test_password",
        api_key: Optional[str] = None,
    ) -> None:
        self._email = email
        self._token = "test_token_fixture_abc123"
        self._order_counter = 0
        logger.info("KalshiRESTClientTest initialised (fixture mode).")

    async def login(self) -> str:
        logger.info("KalshiRESTClientTest.login(): returning fixture token.")
        return self._token

    async def close(self) -> None:
        logger.info("KalshiRESTClientTest.close(): no-op.")

    async def get_markets(
        self,
        limit: int = 100,
        cursor: Optional[str] = None,
        status: str = "open",
    ) -> list[dict]:
        markets = [m for m in _FIXTURE_MARKETS if m["status"] == status][:limit]
        logger.info(
            "KalshiRESTClientTest.get_markets(): returning %d fixture markets.",
            len(markets),
        )
        return markets

    async def get_market(self, ticker: str) -> dict:
        for m in _FIXTURE_MARKETS:
            if m["ticker"] == ticker:
                logger.info(
                    "KalshiRESTClientTest.get_market(%s): found.",
                    ticker,
                )
                return m
        logger.warning(
            "KalshiRESTClientTest.get_market(%s): not in fixtures, "
            "returning first market.",
            ticker,
        )
        return _FIXTURE_MARKETS[0]

    async def get_orderbook(self, ticker: str) -> dict:
        ob = dict(_FIXTURE_ORDERBOOK)
        ob["ticker"] = ticker
        logger.info("KalshiRESTClientTest.get_orderbook(%s): returning fixture.", ticker)
        return ob

    async def place_order(
        self,
        ticker: str,
        side: str,
        price: float,
        size: int,
        order_type: str = "limit",
    ) -> dict:
        self._order_counter += 1
        order = {
            "order_id": f"test-order-{self._order_counter:04d}",
            "ticker": ticker,
            "side": side,
            "price": price,
            "size": size,
            "type": order_type,
            "status": "open",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(
            "KalshiRESTClientTest.place_order(): order_id=%s ticker=%s side=%s.",
            order["order_id"], ticker, side,
        )
        return order

    async def cancel_order(self, order_id: str) -> dict:
        result = {
            "order_id": order_id,
            "status": "cancelled",
            "cancelled_at": datetime.now(timezone.utc).isoformat(),
        }
        logger.info("KalshiRESTClientTest.cancel_order(%s): cancelled.", order_id)
        return result

    async def get_positions(self) -> list[dict]:
        logger.info(
            "KalshiRESTClientTest.get_positions(): returning %d fixture positions.",
            len(_FIXTURE_POSITIONS),
        )
        return list(_FIXTURE_POSITIONS)

    async def get_portfolio_balance(self) -> dict:
        logger.info("KalshiRESTClientTest.get_portfolio_balance(): returning fixture.")
        return dict(_FIXTURE_BALANCE)


# ===========================================================================
#  REDIS PUBLISHING HELPER
# ===========================================================================

async def publish_market_prices(
    client: Any,
    redis_publish: Any = None,
) -> int:
    """
    Fetch current markets from Kalshi and publish price data to Redis.

    Publishes each market as a separate message to stream:prices:kalshi.

    Args:
        client:        KalshiRESTClient or KalshiRESTClientTest instance.
        redis_publish: Async publish function (from redis_client.publish).
                       If None, imports it from the redis_client module.

    Returns:
        Number of price messages published.
    """
    if redis_publish is None:
        from execution.utils.redis_client import publish
        redis_publish = publish

    markets = await client.get_markets(limit=100, status="open")
    published = 0

    for market in markets:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        yes_price = market.get("yes_price", market.get("yes_bid", 0))
        no_price = market.get("no_price", market.get("no_bid", 0))
        ticker = market.get("ticker", "")
        title = market.get("title", "")
        volume = market.get("volume", market.get("volume_24h", 0))

        # Compute spread from yes/no prices
        spread = round(abs(1.0 - (yes_price + no_price)), 4) if yes_price and no_price else 0.0

        msg = {
            "timestamp": now_iso,
            "venue": "kalshi",
            "market_id": ticker,
            "title": title,
            "yes_price": yes_price,
            "no_price": no_price,
            "spread": spread,
            "volume_24h": volume,
        }

        await redis_publish(STREAM_PRICES_KALSHI, msg)
        published += 1

    logger.info(
        "Published %d Kalshi price messages to '%s'.",
        published,
        STREAM_PRICES_KALSHI,
    )
    return published


# ===========================================================================
#  DEMO / SELF-TEST
# ===========================================================================

async def _run_demo(test_mode: bool) -> None:
    """
    Run a full demonstration of the Kalshi REST connector.

    In --test mode, uses KalshiRESTClientTest with fixture data and
    InMemoryRedis.  In live mode, connects to the real Kalshi API and Redis.
    """
    print()
    print("=" * 72)
    print("  Kalshi REST API Connector -- Demo")
    print("  Mode: %s" % ("TEST (fixture data)" if test_mode else "LIVE"))
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # -- Initialise client -------------------------------------------------
    if test_mode:
        client = KalshiRESTClientTest()
        # Write fixture file for reference
        _write_fixture_file()
    else:
        from execution.utils.config import config
        email = config.get_required("KALSHI_EMAIL")
        password = config.get_required("KALSHI_PASSWORD")
        api_key = config.get_optional("KALSHI_API_KEY")
        client = KalshiRESTClient(email, password, api_key)

    # -- Step 1: Login -----------------------------------------------------
    print("\n  Step 1: Authenticating...")
    token = await client.login()
    print(f"    Token: {token[:12]}****" if len(token) > 12 else f"    Token: {token}")

    # -- Step 2: Fetch markets ---------------------------------------------
    print("\n  Step 2: Fetching markets...")
    markets = await client.get_markets(limit=10, status="open")
    print(f"    Received {len(markets)} markets:")
    for m in markets[:5]:
        print(
            f"      {m.get('ticker', '?'):30s}  "
            f"yes={m.get('yes_price', '?'):>6}  "
            f"no={m.get('no_price', '?'):>6}  "
            f"vol={m.get('volume', '?')}"
        )
    if len(markets) > 5:
        print(f"      ... and {len(markets) - 5} more")

    # -- Step 3: Fetch single market ---------------------------------------
    if markets:
        ticker = markets[0].get("ticker", "KXBTC-26MAR28-100K")
        print(f"\n  Step 3: Fetching market detail for '{ticker}'...")
        detail = await client.get_market(ticker)
        print(f"    Title: {detail.get('title', '?')}")
        print(f"    Status: {detail.get('status', '?')}")
        print(f"    Close: {detail.get('close_time', '?')}")

    # -- Step 4: Fetch orderbook -------------------------------------------
    if markets:
        ticker = markets[0].get("ticker", "KXBTC-26MAR28-100K")
        print(f"\n  Step 4: Fetching orderbook for '{ticker}'...")
        ob = await client.get_orderbook(ticker)
        yes_bids = ob.get("yes_bids", ob.get("yes", {}).get("bids", []))
        yes_asks = ob.get("yes_asks", ob.get("yes", {}).get("asks", []))
        print(f"    Yes bids: {len(yes_bids)} levels")
        print(f"    Yes asks: {len(yes_asks)} levels")

    # -- Step 5: Place and cancel an order (test mode only) ----------------
    if test_mode and markets:
        ticker = markets[0].get("ticker", "KXBTC-26MAR28-100K")
        print(f"\n  Step 5: Placing test order on '{ticker}'...")
        order = await client.place_order(
            ticker=ticker, side="yes", price=0.63, size=10, order_type="limit"
        )
        order_id = order.get("order_id", "?")
        print(f"    Order placed: id={order_id} status={order.get('status', '?')}")

        print(f"\n  Step 6: Cancelling order '{order_id}'...")
        cancel_result = await client.cancel_order(order_id)
        print(f"    Cancel result: status={cancel_result.get('status', '?')}")

    # -- Step 7: Fetch positions -------------------------------------------
    print("\n  Step 7: Fetching positions...")
    positions = await client.get_positions()
    print(f"    {len(positions)} position(s):")
    for p in positions:
        print(
            f"      {p.get('ticker', '?'):30s}  "
            f"side={p.get('side', '?'):4s}  "
            f"qty={p.get('quantity', '?')}"
        )

    # -- Step 8: Fetch balance ---------------------------------------------
    print("\n  Step 8: Fetching portfolio balance...")
    balance = await client.get_portfolio_balance()
    print(f"    Balance: ${balance.get('balance', '?')}")
    print(f"    Available: ${balance.get('available_balance', '?')}")
    print(f"    Portfolio value: ${balance.get('portfolio_value', '?')}")

    # -- Step 9: Publish prices to Redis -----------------------------------
    print("\n  Step 9: Publishing prices to Redis Stream...")
    if test_mode:
        # Use InMemoryRedis for test mode
        import execution.utils.redis_client as rc
        rc._use_mock = True
        # Reset singleton so mock gets picked up
        rc._redis_instance = None
        from execution.utils.redis_client import publish, get_redis, close_redis

        # Ensure consumer groups exist for the demo
        redis = await get_redis()

        count = await publish_market_prices(client, redis_publish=publish)
        print(f"    Published {count} price messages to '{STREAM_PRICES_KALSHI}'.")

        # Verify stream length
        stream_len = await redis.xlen(STREAM_PRICES_KALSHI)
        print(f"    Stream length: {stream_len}")

        await close_redis()
    else:
        from execution.utils.redis_client import publish, close_redis
        count = await publish_market_prices(client, redis_publish=publish)
        print(f"    Published {count} price messages to '{STREAM_PRICES_KALSHI}'.")
        await close_redis()

    # -- Cleanup -----------------------------------------------------------
    await client.close()

    print()
    print("=" * 72)
    print("  Demo complete.  All operations succeeded.")
    print("=" * 72)
    print()


# ===========================================================================
#  ENTRY POINT
# ===========================================================================

def main() -> None:
    """
    CLI entry point: parse --test flag and run the Kalshi REST demo.

    In --test mode, uses fixture data and InMemoryRedis (no API or Redis
    connection needed).  In live mode, authenticates with Kalshi and
    connects to Redis.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Kalshi REST API connector for polymarket-trader. "
            "Runs a full demonstration of authentication, market queries, "
            "order management, and Redis publishing."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use fixture data (no Kalshi API or Redis connection needed).",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode with fixture data.")
    else:
        logger.info("Running in live mode against Kalshi API.")

    asyncio.run(_run_demo(test_mode=args.test))


if __name__ == "__main__":
    main()
