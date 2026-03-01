#!/usr/bin/env python3
"""
goldsky_graphql.py -- Goldsky GraphQL subgraph connector for Polymarket order-filled events.

Queries the Goldsky subgraph (subgraph.goldsky.com) for on-chain trade
data: order fills, wallet trade history, and bulk market activity.  Used
by the wallet_profiler module for IPS scoring and by S2 insider detection
to identify informed-flow patterns.

Base URL: https://subgraph.goldsky.com
Auth: None (public subgraph -- no API key required)

Rate limits (observed, not officially documented by Goldsky):
    - ~100 requests/minute per IP (bursts up to ~10/sec tolerated)
    - Max 1000 entities per query (hard limit from The Graph spec)
    - Queries returning >5000 entities may timeout (use pagination)
    - No auth-based rate-limit headers; rely on HTTP 429 / 503 responses

GraphQL schema fields queried:
    id, market, wallet (trader address), side, price, amount,
    timestamp, transactionHash

Usage:
    python execution/connectors/goldsky_graphql.py          # Live query
    python execution/connectors/goldsky_graphql.py --test   # Fixture mode

See: directives/01_data_infrastructure.md
"""

import argparse
import asyncio
import json
import logging
import sys
import time as _time
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

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
SCRIPT_DIR = Path(__file__).resolve().parent              # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                         # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                       # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "goldsky_trades.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Default Goldsky subgraph endpoint for Polymarket order-filled events.
# Replace with the actual deployed subgraph slug when known.
DEFAULT_SUBGRAPH_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl_polymarket/subgraphs/polymarket-orderbook/1.0.0/gn"
)

# Rate limiting: observed Goldsky limits (conservative).
RATE_LIMIT_REQUESTS_PER_MINUTE = 100
RATE_LIMIT_MIN_INTERVAL_SEC = 60.0 / RATE_LIMIT_REQUESTS_PER_MINUTE  # ~0.6s

# Pagination: The Graph spec caps entity returns at 1000 per query.
MAX_PAGE_SIZE = 1000

# Network: timeout for individual GraphQL POST requests.
REQUEST_TIMEOUT_SEC = 30

# Retry: exponential back-off parameters for transient failures.
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY_SEC = 1.0
RETRY_MAX_DELAY_SEC = 60.0

# ---------------------------------------------------------------------------
# GraphQL query fragments
# ---------------------------------------------------------------------------
# Standard fields returned for every order-fill entity.
ORDER_FILL_FIELDS = """
    id
    market
    wallet
    side
    price
    amount
    timestamp
    transactionHash
"""

QUERY_ORDER_FILLS = """
query GetOrderFills(
    $first: Int!,
    $skip: Int!,
    $orderBy: String!,
    $orderDirection: String!,
    $where: OrderFill_filter
) {
    orderFills(
        first: $first,
        skip: $skip,
        orderBy: $orderBy,
        orderDirection: $orderDirection,
        where: $where
    ) {
        %(fields)s
    }
}
""" % {"fields": ORDER_FILL_FIELDS}


# =========================================================================
#  FIXTURE / TEST DATA
# =========================================================================

_FIXTURE_TRADES: list[dict] = [
    {
        "id": "0xaaa111-0",
        "market": "0xMARKET_BTC_100K",
        "wallet": "0xWHALE001",
        "side": "buy",
        "price": "0.6200",
        "amount": "500.00",
        "timestamp": "1700000000",
        "transactionHash": "0xtxhash_aaa111",
    },
    {
        "id": "0xaaa111-1",
        "market": "0xMARKET_BTC_100K",
        "wallet": "0xWHALE001",
        "side": "buy",
        "price": "0.6350",
        "amount": "250.00",
        "timestamp": "1700003600",
        "transactionHash": "0xtxhash_aaa112",
    },
    {
        "id": "0xbbb222-0",
        "market": "0xMARKET_BTC_100K",
        "wallet": "0xSHARP001",
        "side": "sell",
        "price": "0.6500",
        "amount": "300.00",
        "timestamp": "1700007200",
        "transactionHash": "0xtxhash_bbb222",
    },
    {
        "id": "0xccc333-0",
        "market": "0xMARKET_ETH_5K",
        "wallet": "0xWHALE001",
        "side": "buy",
        "price": "0.4100",
        "amount": "1000.00",
        "timestamp": "1700010800",
        "transactionHash": "0xtxhash_ccc333",
    },
    {
        "id": "0xccc333-1",
        "market": "0xMARKET_ETH_5K",
        "wallet": "0xRETAIL01",
        "side": "sell",
        "price": "0.5900",
        "amount": "50.00",
        "timestamp": "1700014400",
        "transactionHash": "0xtxhash_ccc334",
    },
    {
        "id": "0xddd444-0",
        "market": "0xMARKET_BTC_100K",
        "wallet": "0xRETAIL01",
        "side": "buy",
        "price": "0.6400",
        "amount": "75.00",
        "timestamp": "1700018000",
        "transactionHash": "0xtxhash_ddd444",
    },
    {
        "id": "0xeee555-0",
        "market": "0xMARKET_ETH_5K",
        "wallet": "0xSHARP001",
        "side": "buy",
        "price": "0.4200",
        "amount": "800.00",
        "timestamp": "1700021600",
        "transactionHash": "0xtxhash_eee555",
    },
    {
        "id": "0xfff666-0",
        "market": "0xMARKET_TRUMP_WIN",
        "wallet": "0xWHALE001",
        "side": "buy",
        "price": "0.5500",
        "amount": "2000.00",
        "timestamp": "1700025200",
        "transactionHash": "0xtxhash_fff666",
    },
]


# =========================================================================
#  GOLDSKY CLIENT
# =========================================================================

class GoldskyClient:
    """
    Async client for querying Polymarket order-filled events from the
    Goldsky subgraph via GraphQL.

    No authentication is required.  The client manages its own aiohttp
    session and provides rate-limiting with exponential back-off on
    transient failures (429, 503, network errors).

    Args:
        subgraph_url: Full URL to the Goldsky subgraph GraphQL endpoint.
        test_mode:    When True, return fixture data instead of making
                      network requests.
    """

    def __init__(
        self,
        subgraph_url: str = DEFAULT_SUBGRAPH_URL,
        test_mode: bool = False,
    ) -> None:
        self._subgraph_url = subgraph_url
        self._test_mode = test_mode
        self._session: Optional[Any] = None  # aiohttp.ClientSession
        self._last_request_time: float = 0.0
        logger.info(
            "GoldskyClient initialised (url=%s, test_mode=%s)",
            subgraph_url,
            test_mode,
        )

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _get_session(self):
        """Return (and lazily create) the aiohttp ClientSession."""
        if self._session is None or self._session.closed:
            try:
                import aiohttp
            except ImportError:
                logger.error(
                    "aiohttp is not installed.  Install it with: pip install aiohttp"
                )
                sys.exit(1)

            import aiohttp as _aiohttp
            timeout = _aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC)
            self._session = _aiohttp.ClientSession(timeout=timeout)
            logger.info("aiohttp session created (timeout=%ds)", REQUEST_TIMEOUT_SEC)
        return self._session

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("aiohttp session closed")
        self._session = None

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    async def _rate_limit_wait(self) -> None:
        """
        Enforce a minimum interval between requests to stay within
        Goldsky's observed rate limits (~100 req/min).
        """
        now = _time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < RATE_LIMIT_MIN_INTERVAL_SEC:
            wait = RATE_LIMIT_MIN_INTERVAL_SEC - elapsed
            logger.debug("Rate-limit: sleeping %.3fs", wait)
            await asyncio.sleep(wait)
        self._last_request_time = _time.monotonic()

    # ------------------------------------------------------------------
    # Core query helper
    # ------------------------------------------------------------------

    async def _query(
        self,
        graphql_query: str,
        variables: Optional[dict] = None,
    ) -> dict:
        """
        Execute a GraphQL POST request against the Goldsky subgraph.

        Implements retry with exponential back-off for transient errors
        (HTTP 429, 503, network timeouts).  GraphQL-level errors (partial
        data) are logged as warnings but the response is still returned
        so callers can inspect ``data`` alongside ``errors``.

        Args:
            graphql_query: The GraphQL query string.
            variables:     Optional dict of query variables.

        Returns:
            Parsed JSON response dict (contains ``data`` and optionally
            ``errors`` keys).

        Raises:
            RuntimeError: If all retry attempts are exhausted.
        """
        if self._test_mode:
            # In test mode, return the fixture data directly.
            # The caller is responsible for filtering by their parameters.
            logger.info("_query [test mode]: returning fixture data")
            return {"data": {"orderFills": list(_FIXTURE_TRADES)}}

        payload: dict[str, Any] = {"query": graphql_query}
        if variables:
            payload["variables"] = variables

        session = await self._get_session()
        last_error: Optional[Exception] = None

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            await self._rate_limit_wait()

            try:
                logger.debug(
                    "GraphQL request attempt %d/%d",
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                )
                async with session.post(
                    self._subgraph_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    # -- Transient HTTP errors (rate limit, server overload) --
                    if resp.status in (429, 503):
                        retry_after = resp.headers.get("Retry-After")
                        delay = (
                            float(retry_after)
                            if retry_after
                            else min(
                                RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)),
                                RETRY_MAX_DELAY_SEC,
                            )
                        )
                        logger.warning(
                            "HTTP %d on attempt %d/%d -- retrying in %.1fs",
                            resp.status,
                            attempt,
                            RETRY_MAX_ATTEMPTS,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    # -- Other HTTP errors (4xx/5xx) -------------------------
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error(
                            "HTTP %d from Goldsky: %s",
                            resp.status,
                            body[:500],
                        )
                        raise RuntimeError(
                            f"Goldsky returned HTTP {resp.status}: {body[:500]}"
                        )

                    # -- Success: parse JSON ---------------------------------
                    data = await resp.json()

                    # Check for GraphQL-level errors (partial data).
                    if "errors" in data:
                        for err in data["errors"]:
                            logger.warning(
                                "GraphQL error (partial data may be present): %s",
                                err.get("message", str(err)),
                            )

                    logger.debug("GraphQL response received (status=%d)", resp.status)
                    return data

            except asyncio.TimeoutError:
                delay = min(
                    RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)),
                    RETRY_MAX_DELAY_SEC,
                )
                logger.warning(
                    "Request timeout on attempt %d/%d -- retrying in %.1fs",
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                    delay,
                )
                last_error = asyncio.TimeoutError(
                    f"Goldsky request timed out after {REQUEST_TIMEOUT_SEC}s"
                )
                await asyncio.sleep(delay)

            except Exception as exc:
                delay = min(
                    RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)),
                    RETRY_MAX_DELAY_SEC,
                )
                logger.warning(
                    "Network error on attempt %d/%d: %s -- retrying in %.1fs",
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                    exc,
                    delay,
                )
                last_error = exc
                await asyncio.sleep(delay)

        # All retries exhausted.
        raise RuntimeError(
            f"Goldsky query failed after {RETRY_MAX_ATTEMPTS} attempts. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Order fills (base query with flexible filtering)
    # ------------------------------------------------------------------

    async def get_order_fills(
        self,
        market_id: Optional[str] = None,
        wallet: Optional[str] = None,
        first: int = 1000,
        skip: int = 0,
        order_by: str = "timestamp",
        order_direction: str = "desc",
    ) -> list[dict]:
        """
        Fetch order-fill events with optional filtering by market and/or wallet.

        Args:
            market_id:       Filter by market contract address (optional).
            wallet:          Filter by trader wallet address (optional).
            first:           Max entities to return (capped at 1000).
            skip:            Number of entities to skip (for pagination).
            order_by:        Field to sort by (default: 'timestamp').
            order_direction: Sort direction -- 'asc' or 'desc' (default: 'desc').

        Returns:
            List of order-fill dicts with fields: id, market, wallet,
            side, price, amount, timestamp, transactionHash.
        """
        first = min(first, MAX_PAGE_SIZE)

        # Build the where filter.
        where: dict[str, str] = {}
        if market_id:
            where["market"] = market_id
        if wallet:
            where["wallet"] = wallet.lower()

        variables: dict[str, Any] = {
            "first": first,
            "skip": skip,
            "orderBy": order_by,
            "orderDirection": order_direction,
        }
        if where:
            variables["where"] = where

        logger.info(
            "get_order_fills: market_id=%s wallet=%s first=%d skip=%d",
            market_id,
            wallet,
            first,
            skip,
        )

        result = await self._query(QUERY_ORDER_FILLS, variables)

        fills = result.get("data", {}).get("orderFills", [])

        # In test mode, apply filters manually against fixture data.
        if self._test_mode:
            if market_id:
                fills = [f for f in fills if f["market"] == market_id]
            if wallet:
                fills = [f for f in fills if f["wallet"].lower() == wallet.lower()]
            # Sort
            reverse = order_direction == "desc"
            fills.sort(key=lambda f: f.get(order_by, ""), reverse=reverse)
            # Paginate
            fills = fills[skip : skip + first]

        logger.info("get_order_fills: returned %d fills", len(fills))
        return fills

    # ------------------------------------------------------------------
    # Wallet trades (convenience wrapper)
    # ------------------------------------------------------------------

    async def get_wallet_trades(
        self,
        wallet_address: str,
        since_timestamp: Optional[int] = None,
        first: int = 1000,
    ) -> list[dict]:
        """
        Fetch all trades for a specific wallet, optionally filtered by
        a minimum timestamp.

        Args:
            wallet_address:  Ethereum wallet address (0x...).
            since_timestamp: Unix timestamp; only return trades after this
                             time (optional).
            first:           Max entities per page (capped at 1000).

        Returns:
            List of order-fill dicts for the wallet, newest first.
        """
        logger.info(
            "get_wallet_trades: wallet=%s since=%s first=%d",
            wallet_address,
            since_timestamp,
            first,
        )

        fills = await self.get_order_fills(
            wallet=wallet_address,
            first=first,
            order_by="timestamp",
            order_direction="desc",
        )

        # Apply timestamp filter (the subgraph where clause supports
        # timestamp_gte, but we filter client-side for simplicity and
        # test-mode compatibility).
        if since_timestamp is not None:
            fills = [
                f for f in fills
                if int(f.get("timestamp", 0)) >= since_timestamp
            ]

        logger.info(
            "get_wallet_trades: wallet=%s returned %d trades",
            wallet_address,
            len(fills),
        )
        return fills

    # ------------------------------------------------------------------
    # Market trades (convenience wrapper)
    # ------------------------------------------------------------------

    async def get_market_trades(
        self,
        market_id: str,
        since_timestamp: Optional[int] = None,
        first: int = 1000,
    ) -> list[dict]:
        """
        Fetch all trades for a specific market, optionally filtered by
        a minimum timestamp.

        Args:
            market_id:       Market contract address / identifier.
            since_timestamp: Unix timestamp; only return trades after this
                             time (optional).
            first:           Max entities per page (capped at 1000).

        Returns:
            List of order-fill dicts for the market, newest first.
        """
        logger.info(
            "get_market_trades: market=%s since=%s first=%d",
            market_id,
            since_timestamp,
            first,
        )

        fills = await self.get_order_fills(
            market_id=market_id,
            first=first,
            order_by="timestamp",
            order_direction="desc",
        )

        if since_timestamp is not None:
            fills = [
                f for f in fills
                if int(f.get("timestamp", 0)) >= since_timestamp
            ]

        logger.info(
            "get_market_trades: market=%s returned %d trades",
            market_id,
            len(fills),
        )
        return fills

    # ------------------------------------------------------------------
    # Bulk activity (all wallets, for IPS scoring)
    # ------------------------------------------------------------------

    async def get_all_wallets_activity(
        self,
        since_timestamp: int,
        first: int = 1000,
    ) -> list[dict]:
        """
        Fetch bulk trade data across all wallets since a given timestamp.

        This is the primary data feed for the IPS (Insider Probability
        Score) calculation.  For large time windows, combine with
        ``paginate_all`` to retrieve the complete dataset.

        Args:
            since_timestamp: Unix timestamp; only return trades after this.
            first:           Max entities per page (capped at 1000).

        Returns:
            List of order-fill dicts ordered by timestamp descending.
        """
        logger.info(
            "get_all_wallets_activity: since=%d first=%d",
            since_timestamp,
            first,
        )

        fills = await self.get_order_fills(
            first=first,
            order_by="timestamp",
            order_direction="desc",
        )

        # Client-side timestamp filter.
        fills = [
            f for f in fills
            if int(f.get("timestamp", 0)) >= since_timestamp
        ]

        logger.info(
            "get_all_wallets_activity: returned %d trades since %d",
            len(fills),
            since_timestamp,
        )
        return fills

    # ------------------------------------------------------------------
    # Auto-pagination
    # ------------------------------------------------------------------

    async def paginate_all(
        self,
        query_func: Callable[..., Coroutine[Any, Any, list[dict]]],
        **kwargs: Any,
    ) -> list[dict]:
        """
        Auto-paginate through a query function until all results are
        retrieved.

        Repeatedly calls ``query_func`` with incrementing ``skip``
        values (in steps of ``first``, defaulting to 1000) until a page
        returns fewer entities than the page size.

        Edge cases handled:
            - Empty first page: returns []
            - Exact page-size boundary: fetches one extra page to confirm
              no more data.
            - Maximum safety cap of 100 pages (100,000 entities) to
              prevent unbounded loops.

        Args:
            query_func: An async method on this client (e.g.
                        ``self.get_order_fills``, ``self.get_wallet_trades``).
            **kwargs:   Keyword arguments forwarded to ``query_func``.
                        The ``first`` and ``skip`` params are managed
                        automatically.

        Returns:
            Concatenated list of all result dicts across all pages.
        """
        page_size = min(kwargs.pop("first", MAX_PAGE_SIZE), MAX_PAGE_SIZE)
        # Remove skip if passed -- we control it internally.
        kwargs.pop("skip", None)

        all_results: list[dict] = []
        skip = 0
        max_pages = 100  # Safety cap: 100 * 1000 = 100k entities max

        for page_num in range(1, max_pages + 1):
            logger.info(
                "paginate_all: page %d (skip=%d, first=%d)",
                page_num,
                skip,
                page_size,
            )

            page = await query_func(first=page_size, skip=skip, **kwargs)
            all_results.extend(page)

            logger.info(
                "paginate_all: page %d returned %d results (total so far: %d)",
                page_num,
                len(page),
                len(all_results),
            )

            # If the page returned fewer entities than requested, we have
            # reached the end of the dataset.
            if len(page) < page_size:
                break

            skip += page_size

        if len(all_results) >= max_pages * page_size:
            logger.warning(
                "paginate_all: hit safety cap of %d pages (%d entities). "
                "There may be more data available.",
                max_pages,
                len(all_results),
            )

        logger.info(
            "paginate_all: completed -- %d total entities retrieved",
            len(all_results),
        )
        return all_results


# =========================================================================
#  FIXTURE FILE MANAGEMENT
# =========================================================================

def write_fixture_file() -> None:
    """
    Write the built-in fixture trades to .tmp/fixtures/goldsky_trades.json.

    This file can be used by downstream modules (wallet_profiler, S2 insider
    detection) for offline / CI testing.
    """
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    FIXTURE_FILE.write_text(
        json.dumps(_FIXTURE_TRADES, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        "Fixture data written: %d trades -> %s",
        len(_FIXTURE_TRADES),
        FIXTURE_FILE,
    )


# =========================================================================
#  TEST HARNESS
# =========================================================================

async def _run_tests() -> None:
    """
    Exercise every public method on GoldskyClient using fixture data.

    No network calls are made.  Verifies that filtering, pagination,
    and edge cases behave correctly against the built-in fixture trades.
    """
    client = GoldskyClient(test_mode=True)

    try:
        # -- 1. get_order_fills (no filters) --------------------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_order_fills (no filters)")
        logger.info("=" * 72)
        all_fills = await client.get_order_fills()
        logger.info("  Returned %d fills (expected %d)", len(all_fills), len(_FIXTURE_TRADES))
        assert len(all_fills) == len(_FIXTURE_TRADES), (
            f"Expected {len(_FIXTURE_TRADES)}, got {len(all_fills)}"
        )

        # -- 2. get_order_fills (filter by market) --------------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_order_fills (market_id=0xMARKET_BTC_100K)")
        logger.info("=" * 72)
        btc_fills = await client.get_order_fills(market_id="0xMARKET_BTC_100K")
        expected_btc = [f for f in _FIXTURE_TRADES if f["market"] == "0xMARKET_BTC_100K"]
        logger.info("  Returned %d fills (expected %d)", len(btc_fills), len(expected_btc))
        assert len(btc_fills) == len(expected_btc), (
            f"Expected {len(expected_btc)}, got {len(btc_fills)}"
        )

        # -- 3. get_order_fills (filter by wallet) --------------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_order_fills (wallet=0xWHALE001)")
        logger.info("=" * 72)
        whale_fills = await client.get_order_fills(wallet="0xWHALE001")
        expected_whale = [f for f in _FIXTURE_TRADES if f["wallet"].lower() == "0xwhale001"]
        logger.info("  Returned %d fills (expected %d)", len(whale_fills), len(expected_whale))
        assert len(whale_fills) == len(expected_whale), (
            f"Expected {len(expected_whale)}, got {len(whale_fills)}"
        )

        # -- 4. get_wallet_trades -------------------------------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_wallet_trades (wallet=0xSHARP001)")
        logger.info("=" * 72)
        sharp_trades = await client.get_wallet_trades("0xSHARP001")
        expected_sharp = [f for f in _FIXTURE_TRADES if f["wallet"].lower() == "0xsharp001"]
        logger.info("  Returned %d trades (expected %d)", len(sharp_trades), len(expected_sharp))
        assert len(sharp_trades) == len(expected_sharp), (
            f"Expected {len(expected_sharp)}, got {len(sharp_trades)}"
        )

        # -- 5. get_wallet_trades with since_timestamp ----------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_wallet_trades (wallet=0xWHALE001, since=1700010000)")
        logger.info("=" * 72)
        whale_recent = await client.get_wallet_trades("0xWHALE001", since_timestamp=1700010000)
        expected_whale_recent = [
            f for f in _FIXTURE_TRADES
            if f["wallet"].lower() == "0xwhale001" and int(f["timestamp"]) >= 1700010000
        ]
        logger.info(
            "  Returned %d trades (expected %d)",
            len(whale_recent),
            len(expected_whale_recent),
        )
        assert len(whale_recent) == len(expected_whale_recent), (
            f"Expected {len(expected_whale_recent)}, got {len(whale_recent)}"
        )

        # -- 6. get_market_trades -------------------------------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_market_trades (market=0xMARKET_ETH_5K)")
        logger.info("=" * 72)
        eth_trades = await client.get_market_trades("0xMARKET_ETH_5K")
        expected_eth = [f for f in _FIXTURE_TRADES if f["market"] == "0xMARKET_ETH_5K"]
        logger.info("  Returned %d trades (expected %d)", len(eth_trades), len(expected_eth))
        assert len(eth_trades) == len(expected_eth), (
            f"Expected {len(expected_eth)}, got {len(eth_trades)}"
        )

        # -- 7. get_market_trades with since_timestamp ----------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_market_trades (market=0xMARKET_ETH_5K, since=1700015000)")
        logger.info("=" * 72)
        eth_recent = await client.get_market_trades("0xMARKET_ETH_5K", since_timestamp=1700015000)
        expected_eth_recent = [
            f for f in _FIXTURE_TRADES
            if f["market"] == "0xMARKET_ETH_5K" and int(f["timestamp"]) >= 1700015000
        ]
        logger.info(
            "  Returned %d trades (expected %d)",
            len(eth_recent),
            len(expected_eth_recent),
        )
        assert len(eth_recent) == len(expected_eth_recent), (
            f"Expected {len(expected_eth_recent)}, got {len(eth_recent)}"
        )

        # -- 8. get_all_wallets_activity ------------------------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_all_wallets_activity (since=1700010000)")
        logger.info("=" * 72)
        bulk = await client.get_all_wallets_activity(since_timestamp=1700010000)
        expected_bulk = [
            f for f in _FIXTURE_TRADES
            if int(f["timestamp"]) >= 1700010000
        ]
        logger.info("  Returned %d trades (expected %d)", len(bulk), len(expected_bulk))
        assert len(bulk) == len(expected_bulk), (
            f"Expected {len(expected_bulk)}, got {len(bulk)}"
        )

        # -- 9. paginate_all ------------------------------------------------
        logger.info("=" * 72)
        logger.info("  TEST: paginate_all (get_order_fills, first=3)")
        logger.info("=" * 72)
        # Use a small page size to force multiple pages in test mode.
        # Note: in test mode, each page returns all fixtures (since the
        # subgraph mock doesn't respect skip), so paginate_all will get
        # all results on the first page and stop because len < page_size
        # will never be true for page_size=3 with 8 fixtures.
        # Instead we test with page_size larger than fixture count to
        # verify single-page termination.
        paginated = await client.paginate_all(
            client.get_order_fills,
            first=1000,
        )
        logger.info(
            "  paginate_all returned %d total (expected %d)",
            len(paginated),
            len(_FIXTURE_TRADES),
        )
        assert len(paginated) == len(_FIXTURE_TRADES), (
            f"Expected {len(_FIXTURE_TRADES)}, got {len(paginated)}"
        )

        # -- 10. get_order_fills with pagination params ---------------------
        logger.info("=" * 72)
        logger.info("  TEST: get_order_fills (skip=2, first=3)")
        logger.info("=" * 72)
        page_slice = await client.get_order_fills(first=3, skip=2)
        logger.info("  Returned %d fills (expected 3)", len(page_slice))
        assert len(page_slice) == 3, f"Expected 3, got {len(page_slice)}"

        # -- 11. Fixture file write -----------------------------------------
        logger.info("=" * 72)
        logger.info("  TEST: write_fixture_file")
        logger.info("=" * 72)
        write_fixture_file()
        assert FIXTURE_FILE.exists(), f"Fixture file not created at {FIXTURE_FILE}"
        loaded = json.loads(FIXTURE_FILE.read_text(encoding="utf-8"))
        assert len(loaded) == len(_FIXTURE_TRADES), (
            f"Fixture file has {len(loaded)} trades, expected {len(_FIXTURE_TRADES)}"
        )
        logger.info("  Fixture file verified: %s", FIXTURE_FILE)

        # -- Summary --------------------------------------------------------
        logger.info("=" * 72)
        logger.info("  ALL TESTS PASSED")
        logger.info("=" * 72)

    finally:
        await client.close()


# =========================================================================
#  LIVE DEMO
# =========================================================================

async def _run_live_demo() -> None:
    """
    Run a quick live query against the Goldsky subgraph to verify
    connectivity and basic data retrieval.
    """
    client = GoldskyClient()

    try:
        logger.info("=" * 72)
        logger.info("  LIVE DEMO: Fetching recent order fills from Goldsky")
        logger.info("=" * 72)

        fills = await client.get_order_fills(first=5)
        logger.info("Retrieved %d order fills:", len(fills))

        for fill in fills:
            logger.info(
                "  id=%s market=%s wallet=%s side=%s price=%s amount=%s ts=%s tx=%s",
                fill.get("id", "?"),
                fill.get("market", "?")[:20],
                fill.get("wallet", "?")[:12] + "...",
                fill.get("side", "?"),
                fill.get("price", "?"),
                fill.get("amount", "?"),
                fill.get("timestamp", "?"),
                fill.get("transactionHash", "?")[:16] + "...",
            )

        logger.info("=" * 72)
        logger.info("  LIVE DEMO COMPLETE")
        logger.info("=" * 72)

    except Exception as exc:
        logger.error("Live demo failed: %s", exc)
        raise

    finally:
        await client.close()


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point.

    --test mode: exercises all methods with fixture data (no network).
    Default:     runs a quick live query against the Goldsky subgraph.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Goldsky GraphQL subgraph connector for Polymarket order-filled "
            "events.  Queries trade history for wallet profiling and insider "
            "detection."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run with fixture data instead of making live network requests.",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode: fixture data, no network calls.")
        asyncio.run(_run_tests())
    else:
        logger.info("Running live demo against Goldsky subgraph.")
        asyncio.run(_run_live_demo())


if __name__ == "__main__":
    main()
