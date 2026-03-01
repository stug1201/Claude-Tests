#!/usr/bin/env python3
"""
polymarket_data_api.py -- Polymarket Data API connector.

Fetches wallet positions, trade activity, leaderboard rankings, wallet
profit summaries, and market-level trade feeds from the public Polymarket
Data API.  No authentication required.

Base URL: https://data-api.polymarket.com
Auth: None (public API)

Endpoints used:
    GET /positions          -- Current positions for a wallet
    GET /activity           -- On-chain activity for a wallet
    GET /trades             -- Trades for a wallet and/or market
    GET /v1/leaderboard     -- Top traders by PnL or volume
    GET /value              -- Current portfolio value for a wallet
    GET /closed-positions   -- Resolved/closed positions with realized PnL

PolymarketDataClient methods:
    get_wallet_positions(wallet_address)                -- Open positions
    get_wallet_activity(wallet_address, limit=100)      -- Recent activity
    get_leaderboard(limit=100)                          -- Top traders
    get_wallet_profit(wallet_address)                   -- PnL summary
    get_market_trades(market_id, limit=500)             -- Market trade feed
    get_wallet_trades(wallet_address, market_id=None)   -- Wallet trade feed

Error handling:
    - Rate limits:  Exponential backoff (1s, 2s, 4s, 8s, 16s, max 60s)
    - Timeouts:     30s per request, retry up to 3 times
    - HTTP errors:  Logged with status code and body excerpt

Usage:
    python execution/connectors/polymarket_data_api.py          # Live demo
    python execution/connectors/polymarket_data_api.py --test   # Fixture mode

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
SCRIPT_DIR = Path(__file__).resolve().parent                # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                           # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                         # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "polymarket_data_api.json"

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
BASE_URL = "https://data-api.polymarket.com"

# Endpoint paths (relative to BASE_URL)
EP_POSITIONS = "/positions"
EP_ACTIVITY = "/activity"
EP_TRADES = "/trades"
EP_LEADERBOARD = "/v1/leaderboard"
EP_VALUE = "/value"
EP_CLOSED_POSITIONS = "/closed-positions"

# ---------------------------------------------------------------------------
# Retry / backoff configuration
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT_S = 30
MAX_RETRIES = 3
BACKOFF_BASE_S = 1
BACKOFF_MAX_S = 60
RATE_LIMIT_STATUS = 429


# ---------------------------------------------------------------------------
# PolymarketDataClient
# ---------------------------------------------------------------------------

class PolymarketDataClient:
    """
    Async client for the Polymarket Data API (data-api.polymarket.com).

    All methods are async and return parsed JSON (lists or dicts).
    The client manages its own aiohttp.ClientSession; call ``close()``
    when done, or use as an async context manager::

        async with PolymarketDataClient() as client:
            positions = await client.get_wallet_positions("0xabc...")

    In test mode (test_mode=True), all methods return fixture data
    from .tmp/fixtures/polymarket_data_api.json without making any
    network calls.
    """

    def __init__(self, test_mode: bool = False) -> None:
        self._test_mode = test_mode
        self._session: Optional[Any] = None  # aiohttp.ClientSession
        self._fixtures: Optional[dict[str, Any]] = None

        if test_mode:
            self._load_fixtures()
            logger.info("PolymarketDataClient initialised in TEST mode.")
        else:
            logger.info("PolymarketDataClient initialised (live).")

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "PolymarketDataClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _get_session(self):
        """
        Return the shared aiohttp.ClientSession, creating it lazily.

        Late import so that --test mode never requires aiohttp installed.
        """
        if self._session is not None:
            return self._session

        try:
            import aiohttp
        except ImportError:
            logger.error(
                "aiohttp is not installed. "
                "Install with: pip install aiohttp"
            )
            sys.exit(1)

        import aiohttp

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S)
        self._session = aiohttp.ClientSession(
            base_url=BASE_URL,
            timeout=timeout,
            headers={"Accept": "application/json"},
        )
        logger.info("aiohttp session created (timeout=%ds).", REQUEST_TIMEOUT_S)
        return self._session

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.info("aiohttp session closed.")

    # ------------------------------------------------------------------
    # Fixture loading (--test mode)
    # ------------------------------------------------------------------

    def _load_fixtures(self) -> None:
        """Load fixture data from disk, or generate defaults if absent."""
        if FIXTURE_FILE.exists():
            raw = FIXTURE_FILE.read_text(encoding="utf-8")
            self._fixtures = json.loads(raw)
            logger.info("Loaded fixtures from %s", FIXTURE_FILE)
        else:
            logger.info(
                "Fixture file not found at %s -- using built-in defaults.",
                FIXTURE_FILE,
            )
            self._fixtures = _generate_default_fixtures()

    def _get_fixture(self, key: str) -> Any:
        """Return fixture data for *key*, or an empty list/dict."""
        if self._fixtures is None:
            return []
        return self._fixtures.get(key, [])

    # ------------------------------------------------------------------
    # Core HTTP helper with retry + exponential backoff
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """
        Make an HTTP request with retry and exponential backoff.

        Handles rate limiting (HTTP 429) and transient errors (5xx,
        timeouts, connection errors) with up to MAX_RETRIES attempts.

        Returns parsed JSON on success.  Raises on exhausted retries.
        """
        import aiohttp

        session = await self._get_session()

        # Strip None values from params to keep URLs clean.
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        backoff = BACKOFF_BASE_S
        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug(
                    "Request attempt %d/%d: %s %s params=%s",
                    attempt, MAX_RETRIES, method, path, params,
                )
                async with session.request(method, path, params=params) as resp:
                    # -- Rate limited ----------------------------------------
                    if resp.status == RATE_LIMIT_STATUS:
                        retry_after = resp.headers.get("Retry-After")
                        wait = (
                            float(retry_after)
                            if retry_after
                            else min(backoff, BACKOFF_MAX_S)
                        )
                        logger.warning(
                            "Rate limited (429) on %s. "
                            "Retry-After=%s. Waiting %.1fs (attempt %d/%d).",
                            path, retry_after, wait, attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(wait)
                        backoff = min(backoff * 2, BACKOFF_MAX_S)
                        continue

                    # -- Server errors (5xx) ---------------------------------
                    if resp.status >= 500:
                        body_preview = (await resp.text())[:200]
                        logger.warning(
                            "Server error %d on %s: %s. "
                            "Retrying in %.1fs (attempt %d/%d).",
                            resp.status, path, body_preview,
                            backoff, attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, BACKOFF_MAX_S)
                        continue

                    # -- Client errors (4xx, not 429) ------------------------
                    if resp.status >= 400:
                        body_preview = (await resp.text())[:300]
                        logger.error(
                            "Client error %d on %s: %s",
                            resp.status, path, body_preview,
                        )
                        raise ValueError(
                            f"HTTP {resp.status} on {path}: {body_preview}"
                        )

                    # -- Success ---------------------------------------------
                    data = await resp.json()
                    logger.debug(
                        "Response OK from %s (status=%d).", path, resp.status,
                    )
                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                logger.warning(
                    "Network error on %s: %s. "
                    "Retrying in %.1fs (attempt %d/%d).",
                    path, exc, backoff, attempt, MAX_RETRIES,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_S)

        # All retries exhausted.
        msg = (
            f"Request to {path} failed after {MAX_RETRIES} attempts. "
            f"Last error: {last_error}"
        )
        logger.error(msg)
        raise ConnectionError(msg)

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def get_wallet_positions(
        self,
        wallet_address: str,
        limit: int = 100,
        sort_by: str = "CURRENT",
    ) -> list[dict]:
        """
        Fetch current open positions for a wallet.

        Args:
            wallet_address:  0x-prefixed Ethereum address.
            limit:           Maximum number of positions (1-500).
            sort_by:         Sort field: TOKENS, CURRENT, INITIAL, CASHPNL,
                             PERCENTPNL, TITLE, RESOLVING, PRICE, AVGPRICE.

        Returns:
            List of position dicts with keys including: asset, conditionId,
            size, curPrice, avgPrice, cashPnl, percentPnl, title, etc.
        """
        if self._test_mode:
            logger.info("TEST: get_wallet_positions(%s)", wallet_address)
            return self._get_fixture("positions")

        logger.info(
            "Fetching positions for wallet %s (limit=%d, sort=%s).",
            wallet_address, limit, sort_by,
        )
        result = await self._request("GET", EP_POSITIONS, params={
            "user": wallet_address,
            "limit": limit,
            "sortBy": sort_by,
            "sortDirection": "DESC",
            "sizeThreshold": 0,
        })
        positions = result if isinstance(result, list) else []
        logger.info(
            "Fetched %d positions for %s.", len(positions), wallet_address,
        )
        return positions

    async def get_wallet_activity(
        self,
        wallet_address: str,
        limit: int = 100,
        activity_type: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch recent on-chain activity for a wallet.

        Activity types include: TRADE, SPLIT, MERGE, REDEEM, REWARD,
        CONVERSION, MAKER_REBATE.

        Args:
            wallet_address:  0x-prefixed Ethereum address.
            limit:           Maximum number of activities (1-500).
            activity_type:   Optional filter, e.g. "TRADE" or "TRADE,REDEEM".

        Returns:
            List of activity dicts ordered by timestamp DESC, with keys
            including: timestamp, conditionId, type, size, usdcSize,
            price, side, title, transactionHash, etc.
        """
        if self._test_mode:
            logger.info("TEST: get_wallet_activity(%s)", wallet_address)
            return self._get_fixture("activity")

        logger.info(
            "Fetching activity for wallet %s (limit=%d, type=%s).",
            wallet_address, limit, activity_type,
        )
        result = await self._request("GET", EP_ACTIVITY, params={
            "user": wallet_address,
            "limit": limit,
            "type": activity_type,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        })
        activities = result if isinstance(result, list) else []
        logger.info(
            "Fetched %d activity records for %s.",
            len(activities), wallet_address,
        )
        return activities

    async def get_leaderboard(
        self,
        limit: int = 100,
        time_period: str = "ALL",
        order_by: str = "PNL",
        category: str = "OVERALL",
    ) -> list[dict]:
        """
        Fetch the trader leaderboard.

        Args:
            limit:        Number of traders to return (1-50 per page; this
                          method auto-paginates up to *limit* total).
            time_period:  DAY, WEEK, MONTH, or ALL.
            order_by:     PNL or VOL.
            category:     OVERALL, POLITICS, SPORTS, CRYPTO, etc.

        Returns:
            List of trader dicts with keys including: rank, proxyWallet,
            userName, vol, pnl, profileImage, xUsername, verifiedBadge.
        """
        if self._test_mode:
            logger.info("TEST: get_leaderboard(limit=%d)", limit)
            return self._get_fixture("leaderboard")

        logger.info(
            "Fetching leaderboard (limit=%d, period=%s, order=%s, cat=%s).",
            limit, time_period, order_by, category,
        )

        # The leaderboard endpoint caps at 50 per page, so paginate.
        page_size = min(limit, 50)
        all_traders: list[dict] = []
        offset = 0

        while len(all_traders) < limit:
            page = await self._request("GET", EP_LEADERBOARD, params={
                "limit": page_size,
                "offset": offset,
                "timePeriod": time_period,
                "orderBy": order_by,
                "category": category,
            })
            if not isinstance(page, list) or len(page) == 0:
                break
            all_traders.extend(page)
            offset += len(page)
            # If the page returned fewer than requested, no more data.
            if len(page) < page_size:
                break

        # Trim to the exact requested limit.
        all_traders = all_traders[:limit]
        logger.info("Fetched %d leaderboard entries.", len(all_traders))
        return all_traders

    async def get_wallet_profit(self, wallet_address: str) -> dict:
        """
        Build a PnL summary for a wallet by combining portfolio value,
        open positions, and closed (resolved) positions.

        Returns a dict with:
            wallet:               The queried address.
            portfolio_value:      Current portfolio value (from /value).
            open_positions_count: Number of open positions.
            open_unrealized_pnl:  Sum of cashPnl across open positions.
            closed_positions_count: Number of resolved positions.
            closed_realized_pnl:  Sum of realizedPnl across closed positions.
            total_pnl:            open_unrealized_pnl + closed_realized_pnl.
            fetched_at:           ISO-8601 timestamp.
        """
        if self._test_mode:
            logger.info("TEST: get_wallet_profit(%s)", wallet_address)
            return self._get_fixture("profit")

        logger.info("Building PnL summary for wallet %s.", wallet_address)

        # Fetch portfolio value, open positions, and closed positions
        # concurrently for speed.
        value_task = self._request("GET", EP_VALUE, params={
            "user": wallet_address,
        })
        open_task = self.get_wallet_positions(
            wallet_address, limit=500, sort_by="CASHPNL",
        )
        closed_task = self._request("GET", EP_CLOSED_POSITIONS, params={
            "user": wallet_address,
            "limit": 50,
            "sortBy": "REALIZEDPNL",
            "sortDirection": "DESC",
        })

        value_result, open_positions, closed_result = await asyncio.gather(
            value_task, open_task, closed_task,
        )

        # Parse portfolio value.
        portfolio_value = 0.0
        if isinstance(value_result, list) and len(value_result) > 0:
            portfolio_value = float(value_result[0].get("value", 0))
        elif isinstance(value_result, dict):
            portfolio_value = float(value_result.get("value", 0))

        # Sum unrealized PnL from open positions.
        unrealized_pnl = 0.0
        for pos in open_positions:
            try:
                unrealized_pnl += float(pos.get("cashPnl", 0))
            except (ValueError, TypeError):
                pass

        # Sum realized PnL from closed positions.
        closed_positions = closed_result if isinstance(closed_result, list) else []
        realized_pnl = 0.0
        for pos in closed_positions:
            try:
                realized_pnl += float(pos.get("realizedPnl", 0))
            except (ValueError, TypeError):
                pass

        summary = {
            "wallet": wallet_address,
            "portfolio_value": round(portfolio_value, 2),
            "open_positions_count": len(open_positions),
            "open_unrealized_pnl": round(unrealized_pnl, 2),
            "closed_positions_count": len(closed_positions),
            "closed_realized_pnl": round(realized_pnl, 2),
            "total_pnl": round(unrealized_pnl + realized_pnl, 2),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "PnL summary for %s: total_pnl=%.2f (unrealized=%.2f, realized=%.2f).",
            wallet_address, summary["total_pnl"],
            summary["open_unrealized_pnl"], summary["closed_realized_pnl"],
        )
        return summary

    async def get_market_trades(
        self,
        market_id: str,
        limit: int = 500,
    ) -> list[dict]:
        """
        Fetch recent trades for a specific market (condition ID).

        Args:
            market_id:  The market's condition ID (0x-prefixed hash).
            limit:      Maximum number of trades (1-10000).

        Returns:
            List of trade dicts with keys including: proxyWallet, side,
            asset, conditionId, size, price, timestamp, title, outcome,
            transactionHash, etc.
        """
        if self._test_mode:
            logger.info("TEST: get_market_trades(%s)", market_id)
            return self._get_fixture("market_trades")

        logger.info(
            "Fetching trades for market %s (limit=%d).", market_id, limit,
        )
        result = await self._request("GET", EP_TRADES, params={
            "market": market_id,
            "limit": limit,
        })
        trades = result if isinstance(result, list) else []
        logger.info(
            "Fetched %d trades for market %s.", len(trades), market_id,
        )
        return trades

    async def get_wallet_trades(
        self,
        wallet_address: str,
        market_id: Optional[str] = None,
        limit: int = 500,
    ) -> list[dict]:
        """
        Fetch trades made by a specific wallet, optionally filtered to
        a single market.

        Args:
            wallet_address:  0x-prefixed Ethereum address.
            market_id:       Optional condition ID to filter by market.
            limit:           Maximum number of trades (1-10000).

        Returns:
            List of trade dicts with keys including: proxyWallet, side,
            asset, conditionId, size, price, timestamp, title, outcome,
            transactionHash, etc.
        """
        if self._test_mode:
            logger.info(
                "TEST: get_wallet_trades(%s, market=%s)",
                wallet_address, market_id,
            )
            return self._get_fixture("wallet_trades")

        logger.info(
            "Fetching trades for wallet %s (market=%s, limit=%d).",
            wallet_address, market_id, limit,
        )
        result = await self._request("GET", EP_TRADES, params={
            "user": wallet_address,
            "market": market_id,
            "limit": limit,
        })
        trades = result if isinstance(result, list) else []
        logger.info(
            "Fetched %d trades for wallet %s.", len(trades), wallet_address,
        )
        return trades


# ---------------------------------------------------------------------------
# Fixture data generation
# ---------------------------------------------------------------------------

def _generate_default_fixtures() -> dict[str, Any]:
    """
    Return a dict of fixture data keyed by method name.

    Used when no .tmp/fixtures/polymarket_data_api.json exists.
    Provides realistic-looking but completely synthetic data so that
    --test mode works without any network calls or fixture files.
    """
    now = datetime.now(timezone.utc)
    ts = now.isoformat()

    fixtures: dict[str, Any] = {
        "positions": [
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "0xaaa111",
                "conditionId": "0xcondition_btc_100k",
                "size": "150.0",
                "curPrice": "0.72",
                "avgPrice": "0.55",
                "cashPnl": "25.50",
                "percentPnl": "30.91",
                "title": "Will BTC exceed $100k by March 2026?",
                "slug": "will-btc-exceed-100k-by-march-2026",
                "outcome": "Yes",
                "outcomeIndex": 0,
            },
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "0xbbb222",
                "conditionId": "0xcondition_eth_5k",
                "size": "200.0",
                "curPrice": "0.38",
                "avgPrice": "0.45",
                "cashPnl": "-14.00",
                "percentPnl": "-15.56",
                "title": "Will ETH reach $5k by Q2 2026?",
                "slug": "will-eth-reach-5k-by-q2-2026",
                "outcome": "Yes",
                "outcomeIndex": 0,
            },
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "0xccc333",
                "conditionId": "0xcondition_fed_rate",
                "size": "500.0",
                "curPrice": "0.81",
                "avgPrice": "0.60",
                "cashPnl": "105.00",
                "percentPnl": "35.00",
                "title": "Fed rate cut in March 2026?",
                "slug": "fed-rate-cut-march-2026",
                "outcome": "Yes",
                "outcomeIndex": 0,
            },
        ],
        "activity": [
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "timestamp": ts,
                "conditionId": "0xcondition_btc_100k",
                "type": "TRADE",
                "size": "50.0",
                "usdcSize": "36.00",
                "price": "0.72",
                "side": "BUY",
                "title": "Will BTC exceed $100k by March 2026?",
                "outcome": "Yes",
                "transactionHash": "0xtxhash_001",
            },
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "timestamp": ts,
                "conditionId": "0xcondition_fed_rate",
                "type": "TRADE",
                "size": "100.0",
                "usdcSize": "81.00",
                "price": "0.81",
                "side": "SELL",
                "title": "Fed rate cut in March 2026?",
                "outcome": "Yes",
                "transactionHash": "0xtxhash_002",
            },
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "timestamp": ts,
                "conditionId": "0xcondition_eth_5k",
                "type": "REDEEM",
                "size": "75.0",
                "usdcSize": "75.00",
                "price": "1.00",
                "side": "BUY",
                "title": "Will ETH reach $5k by Q2 2026?",
                "outcome": "No",
                "transactionHash": "0xtxhash_003",
            },
        ],
        "leaderboard": [
            {
                "rank": 1,
                "proxyWallet": "0xleader_wallet_001",
                "userName": "whale_alpha",
                "vol": "2450000.00",
                "pnl": "185000.00",
                "profileImage": "https://example.com/avatar1.png",
                "xUsername": "whale_alpha",
                "verifiedBadge": True,
            },
            {
                "rank": 2,
                "proxyWallet": "0xleader_wallet_002",
                "userName": "smart_money_42",
                "vol": "1800000.00",
                "pnl": "142000.00",
                "profileImage": "https://example.com/avatar2.png",
                "xUsername": "smartmoney42",
                "verifiedBadge": False,
            },
            {
                "rank": 3,
                "proxyWallet": "0xleader_wallet_003",
                "userName": "prediction_pro",
                "vol": "950000.00",
                "pnl": "98500.00",
                "profileImage": "https://example.com/avatar3.png",
                "xUsername": "predictionpro",
                "verifiedBadge": True,
            },
        ],
        "profit": {
            "wallet": "0x1234567890abcdef1234567890abcdef12345678",
            "portfolio_value": 1250.00,
            "open_positions_count": 3,
            "open_unrealized_pnl": 116.50,
            "closed_positions_count": 5,
            "closed_realized_pnl": 340.75,
            "total_pnl": 457.25,
            "fetched_at": ts,
        },
        "market_trades": [
            {
                "proxyWallet": "0xtrader_aaa",
                "side": "BUY",
                "asset": "0xaaa111",
                "conditionId": "0xcondition_btc_100k",
                "size": "250.0",
                "price": "0.71",
                "timestamp": ts,
                "title": "Will BTC exceed $100k by March 2026?",
                "outcome": "Yes",
                "outcomeIndex": 0,
                "transactionHash": "0xtxhash_market_001",
            },
            {
                "proxyWallet": "0xtrader_bbb",
                "side": "SELL",
                "asset": "0xaaa111",
                "conditionId": "0xcondition_btc_100k",
                "size": "100.0",
                "price": "0.72",
                "timestamp": ts,
                "title": "Will BTC exceed $100k by March 2026?",
                "outcome": "Yes",
                "outcomeIndex": 0,
                "transactionHash": "0xtxhash_market_002",
            },
        ],
        "wallet_trades": [
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "side": "BUY",
                "asset": "0xaaa111",
                "conditionId": "0xcondition_btc_100k",
                "size": "50.0",
                "price": "0.55",
                "timestamp": ts,
                "title": "Will BTC exceed $100k by March 2026?",
                "outcome": "Yes",
                "outcomeIndex": 0,
                "transactionHash": "0xtxhash_wallet_001",
            },
            {
                "proxyWallet": "0x1234567890abcdef1234567890abcdef12345678",
                "side": "BUY",
                "asset": "0xccc333",
                "conditionId": "0xcondition_fed_rate",
                "size": "500.0",
                "price": "0.60",
                "timestamp": ts,
                "title": "Fed rate cut in March 2026?",
                "outcome": "Yes",
                "outcomeIndex": 0,
                "transactionHash": "0xtxhash_wallet_002",
            },
        ],
    }

    return fixtures


def write_fixture_file() -> None:
    """
    Write default fixture data to .tmp/fixtures/polymarket_data_api.json.

    Called in --test mode so that future runs can load from disk, and so
    the fixture data can be inspected or modified by hand.
    """
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    fixtures = _generate_default_fixtures()
    FIXTURE_FILE.write_text(
        json.dumps(fixtures, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Fixture data written to %s", FIXTURE_FILE)


# ---------------------------------------------------------------------------
# Demo / self-test
# ---------------------------------------------------------------------------

async def _demo(test_mode: bool) -> None:
    """
    Run a demonstration of all PolymarketDataClient methods.

    In test mode, uses fixture data.  In live mode, queries a known
    active wallet (Polymarket deployer) for real results.
    """
    print()
    print("=" * 72)
    print("  Polymarket Data API Connector -- Demo")
    print("  Mode: %s" % ("TEST (fixtures)" if test_mode else "LIVE"))
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # In test mode, ensure fixture file exists on disk for inspection.
    if test_mode:
        write_fixture_file()

    # Use a well-known wallet for live demos; fixture wallet for test.
    demo_wallet = "0x1234567890abcdef1234567890abcdef12345678"
    demo_market = "0xcondition_btc_100k"

    async with PolymarketDataClient(test_mode=test_mode) as client:

        # -- 1. Wallet positions ------------------------------------------
        print("\n--- Wallet Positions ---")
        positions = await client.get_wallet_positions(demo_wallet)
        print(f"  Found {len(positions)} position(s):")
        for p in positions[:5]:
            title = p.get("title", "<untitled>")[:50]
            pnl = p.get("cashPnl", "?")
            size = p.get("size", "?")
            print(f"    {title} | size={size} | pnl={pnl}")

        # -- 2. Wallet activity -------------------------------------------
        print("\n--- Wallet Activity ---")
        activity = await client.get_wallet_activity(demo_wallet, limit=5)
        print(f"  Found {len(activity)} activity record(s):")
        for a in activity[:5]:
            atype = a.get("type", "?")
            title = a.get("title", "<untitled>")[:40]
            side = a.get("side", "?")
            size = a.get("usdcSize", "?")
            print(f"    [{atype}] {title} | {side} ${size}")

        # -- 3. Leaderboard -----------------------------------------------
        print("\n--- Leaderboard (Top 3) ---")
        leaders = await client.get_leaderboard(limit=3)
        print(f"  Found {len(leaders)} leader(s):")
        for lb in leaders:
            rank = lb.get("rank", "?")
            name = lb.get("userName", "anon")
            pnl = lb.get("pnl", "?")
            vol = lb.get("vol", "?")
            print(f"    #{rank} {name} | pnl=${pnl} | vol=${vol}")

        # -- 4. Wallet profit summary -------------------------------------
        print("\n--- Wallet Profit Summary ---")
        profit = await client.get_wallet_profit(demo_wallet)
        print(f"  Wallet:          {profit.get('wallet', '?')}")
        print(f"  Portfolio value: ${profit.get('portfolio_value', '?')}")
        print(f"  Open positions:  {profit.get('open_positions_count', '?')}")
        print(f"  Unrealized PnL:  ${profit.get('open_unrealized_pnl', '?')}")
        print(f"  Closed positions:{profit.get('closed_positions_count', '?')}")
        print(f"  Realized PnL:    ${profit.get('closed_realized_pnl', '?')}")
        print(f"  Total PnL:       ${profit.get('total_pnl', '?')}")

        # -- 5. Market trades ---------------------------------------------
        print("\n--- Market Trades ---")
        market_trades = await client.get_market_trades(demo_market, limit=5)
        print(f"  Found {len(market_trades)} trade(s) for market {demo_market[:20]}...:")
        for t in market_trades[:5]:
            side = t.get("side", "?")
            size = t.get("size", "?")
            price = t.get("price", "?")
            wallet = t.get("proxyWallet", "?")[:12]
            print(f"    {side} {size} @ {price} by {wallet}...")

        # -- 6. Wallet trades ---------------------------------------------
        print("\n--- Wallet Trades ---")
        wallet_trades = await client.get_wallet_trades(demo_wallet, limit=5)
        print(f"  Found {len(wallet_trades)} trade(s) for wallet:")
        for t in wallet_trades[:5]:
            side = t.get("side", "?")
            title = t.get("title", "<untitled>")[:40]
            size = t.get("size", "?")
            price = t.get("price", "?")
            print(f"    {side} {size} @ {price} | {title}")

    print()
    print("=" * 72)
    print("  Demo complete.  All methods executed successfully.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse --test flag and run the demo.

    In --test mode, uses fixture data (no network calls).
    In live mode, queries the Polymarket Data API directly.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Polymarket Data API connector. "
            "Fetches wallet positions, activity, leaderboard, and trades."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use fixture data instead of making live API calls.",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode with fixture data.")
    else:
        logger.info("Running in live mode against data-api.polymarket.com.")

    asyncio.run(_demo(test_mode=args.test))


if __name__ == "__main__":
    main()
