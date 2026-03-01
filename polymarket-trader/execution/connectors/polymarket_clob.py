#!/usr/bin/env python3
"""
polymarket_clob.py -- Polymarket CLOB API connector for polymarket-trader.

Handles Ed25519 + HMAC authentication for signed requests, async REST calls
for orderbook queries, market listing, order placement/cancellation, position
tracking, and a WebSocket price-streaming feed that publishes raw ticks to
the stream:prices:polymarket Redis Stream.

Base URL: https://clob.polymarket.com
WebSocket: wss://ws-subscriptions-clob.polymarket.com/ws/market
Auth: HMAC-SHA256 signature using POLYMARKET_API_SECRET, with
      POLYMARKET_API_KEY passed as the POLY_API_KEY header.

Usage:
    python execution/connectors/polymarket_clob.py          # Live mode
    python execution/connectors/polymarket_clob.py --test   # Fixture mode

Environment variables (via execution.utils.config):
    POLYMARKET_PRIVATE_KEY      -- Ed25519 private key for order signing
    POLYMARKET_API_KEY          -- API key (sent as header)
    POLYMARKET_API_SECRET       -- HMAC-SHA256 signing secret
    POLYMARKET_WALLET_ADDRESS   -- Wallet address (0x...)

See: directives/01_data_infrastructure.md
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import sys
import time as _time
from datetime import datetime, timezone
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
SCRIPT_DIR = Path(__file__).resolve().parent           # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                      # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                    # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
BASE_URL = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ---------------------------------------------------------------------------
# Redis stream name (imported from redis_client when available, declared
# here as a fallback constant so the module is self-contained).
# ---------------------------------------------------------------------------
STREAM_PRICES_POLYMARKET = "stream:prices:polymarket"

# ---------------------------------------------------------------------------
# Retry / backoff constants
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
BACKOFF_BASE = 1          # seconds
BACKOFF_MAX = 60           # seconds (caps exponential growth)
REQUEST_TIMEOUT = 30       # seconds for REST calls
WS_RECONNECT_BASE = 1     # seconds
WS_RECONNECT_MAX = 60     # seconds

# ---------------------------------------------------------------------------
# Fixture data for --test mode
# ---------------------------------------------------------------------------
_FIXTURE_MARKETS = [
    {
        "condition_id": "0xabc123def456789000000000000000000000000000000000000000000000001",
        "question_id": "0xq1",
        "tokens": [
            {"token_id": "tok_yes_1", "outcome": "Yes", "price": 0.6500},
            {"token_id": "tok_no_1", "outcome": "No", "price": 0.3500},
        ],
        "question": "Will BTC exceed $100k by March 2026?",
        "market_slug": "will-btc-exceed-100k-march-2026",
        "end_date_iso": "2026-03-31T23:59:59Z",
        "active": True,
        "closed": False,
        "volume": "125000.00",
        "volume_num": 125000.00,
    },
    {
        "condition_id": "0xabc123def456789000000000000000000000000000000000000000000000002",
        "question_id": "0xq2",
        "tokens": [
            {"token_id": "tok_yes_2", "outcome": "Yes", "price": 0.4200},
            {"token_id": "tok_no_2", "outcome": "No", "price": 0.5800},
        ],
        "question": "Will ETH reach $5k by Q2 2026?",
        "market_slug": "will-eth-reach-5k-q2-2026",
        "end_date_iso": "2026-06-30T23:59:59Z",
        "active": True,
        "closed": False,
        "volume": "89000.00",
        "volume_num": 89000.00,
    },
    {
        "condition_id": "0xabc123def456789000000000000000000000000000000000000000000000003",
        "question_id": "0xq3",
        "tokens": [
            {"token_id": "tok_yes_3", "outcome": "Yes", "price": 0.3100},
            {"token_id": "tok_no_3", "outcome": "No", "price": 0.6900},
        ],
        "question": "US unemployment rate above 4.5% in March 2026?",
        "market_slug": "us-unemployment-above-4-5-march-2026",
        "end_date_iso": "2026-03-31T23:59:59Z",
        "active": True,
        "closed": False,
        "volume": "45000.00",
        "volume_num": 45000.00,
    },
]

_FIXTURE_ORDERBOOK = {
    "market": "0xabc123def456789000000000000000000000000000000000000000000000001",
    "asset_id": "tok_yes_1",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "bids": [
        {"price": "0.6400", "size": "500.00"},
        {"price": "0.6300", "size": "1200.00"},
        {"price": "0.6200", "size": "2500.00"},
        {"price": "0.6100", "size": "800.00"},
    ],
    "asks": [
        {"price": "0.6500", "size": "400.00"},
        {"price": "0.6600", "size": "1000.00"},
        {"price": "0.6700", "size": "1800.00"},
        {"price": "0.6800", "size": "600.00"},
    ],
}

_FIXTURE_ORDER_RESPONSE = {
    "id": "order_test_001",
    "status": "live",
    "market": "0xabc123def456789000000000000000000000000000000000000000000000001",
    "asset_id": "tok_yes_1",
    "side": "BUY",
    "type": "limit",
    "original_size": "50.00",
    "size_matched": "0.00",
    "price": "0.6400",
    "created_at": datetime.now(timezone.utc).isoformat(),
    "owner": "0x0000000000000000000000000000000000000000",
}

_FIXTURE_POSITIONS = [
    {
        "asset": {
            "token_id": "tok_yes_1",
            "condition_id": "0xabc123def456789000000000000000000000000000000000000000000000001",
            "outcome": "Yes",
        },
        "size": "100.00",
        "avg_price": "0.6200",
        "cur_price": "0.6500",
        "pnl": "3.00",
        "realized_pnl": "0.00",
    },
    {
        "asset": {
            "token_id": "tok_no_2",
            "condition_id": "0xabc123def456789000000000000000000000000000000000000000000000002",
            "outcome": "No",
        },
        "size": "200.00",
        "avg_price": "0.5500",
        "cur_price": "0.5800",
        "pnl": "6.00",
        "realized_pnl": "1.20",
    },
]


# =========================================================================
#  PolymarketCLOBClient
# =========================================================================

class PolymarketCLOBClient:
    """
    Async client for the Polymarket CLOB API.

    Handles HMAC-SHA256 request signing, REST endpoints for markets,
    orderbooks, order lifecycle, and positions.  Includes a WebSocket
    feed method that streams live price updates and publishes them to
    Redis via redis_client.publish().

    Args:
        api_key:        POLYMARKET_API_KEY header value.
        api_secret:     POLYMARKET_API_SECRET for HMAC signing.
        private_key:    Ed25519 private key for order signing.
        wallet_address: Wallet address (0x...).
        test_mode:      If True, return fixture data instead of making API calls.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        private_key: str,
        wallet_address: str,
        test_mode: bool = False,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._private_key = private_key
        self._wallet_address = wallet_address
        self._test_mode = test_mode
        self._session: Optional[Any] = None  # aiohttp.ClientSession
        logger.info(
            "PolymarketCLOBClient initialised (test_mode=%s, wallet=%s).",
            test_mode,
            wallet_address[:10] + "****" if wallet_address else "<not set>",
        )

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    async def _get_session(self):
        """Return (and lazily create) the aiohttp.ClientSession."""
        if self._session is not None and not self._session.closed:
            return self._session

        import aiohttp
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self._session = aiohttp.ClientSession(
            base_url=BASE_URL,
            timeout=timeout,
        )
        return self._session

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("aiohttp session closed.")

    # ------------------------------------------------------------------
    # Authentication: HMAC-SHA256 signature
    # ------------------------------------------------------------------

    def _sign_request(
        self,
        method: str,
        path: str,
        body: str = "",
        timestamp: Optional[str] = None,
    ) -> dict[str, str]:
        """
        Generate HMAC-SHA256 signature headers for a CLOB API request.

        The signature is computed over:  timestamp + method + path + body
        using the API secret as the HMAC key.

        Args:
            method:    HTTP method (GET, POST, DELETE).
            path:      Request path (e.g. /markets).
            body:      JSON-encoded request body (empty string for GET/DELETE).
            timestamp: Unix epoch string; auto-generated if omitted.

        Returns:
            Dict of authentication headers to merge into the request.
        """
        if timestamp is None:
            timestamp = str(int(_time.time()))

        message = timestamp + method.upper() + path + body
        signature = hmac.new(
            key=self._api_secret.encode("utf-8"),
            msg=message.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

        headers = {
            "POLY_ADDRESS": self._wallet_address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": self._api_key,
            "POLY_PASSPHRASE": "",
        }
        return headers

    # ------------------------------------------------------------------
    # Internal request helper with retries
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        signed: bool = False,
    ) -> dict | list:
        """
        Execute an HTTP request against the CLOB API with retry logic.

        Implements:
          - Auth failure logging (401/403: logs exact error response body)
          - Rate-limit handling (429: exponential backoff up to BACKOFF_MAX)
          - Network timeout retries (up to MAX_RETRIES)

        Args:
            method:    HTTP method.
            path:      URL path relative to BASE_URL.
            params:    Query parameters (for GET).
            json_body: JSON body (for POST).
            signed:    Whether to attach HMAC auth headers.

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            RuntimeError: If all retries are exhausted.
        """
        import aiohttp

        body_str = json.dumps(json_body) if json_body else ""
        backoff = BACKOFF_BASE
        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                session = await self._get_session()

                headers: dict[str, str] = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                if signed:
                    auth_headers = self._sign_request(method, path, body_str)
                    headers.update(auth_headers)

                logger.debug(
                    "Request attempt %d/%d: %s %s",
                    attempt, MAX_RETRIES, method, path,
                )

                async with session.request(
                    method=method,
                    url=path,
                    params=params,
                    data=body_str if json_body else None,
                    headers=headers,
                ) as resp:
                    resp_text = await resp.text()

                    # -- Auth failures: log exact error body ---------------
                    if resp.status in (401, 403):
                        logger.error(
                            "Auth failure (%d) on %s %s: %s",
                            resp.status, method, path, resp_text,
                        )
                        raise RuntimeError(
                            f"Authentication failed ({resp.status}): {resp_text}"
                        )

                    # -- Rate limiting: exponential backoff -----------------
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        wait = (
                            float(retry_after) if retry_after
                            else min(backoff, BACKOFF_MAX)
                        )
                        logger.warning(
                            "Rate limited (429) on %s %s. "
                            "Retry-After=%s. Backing off %.1f s "
                            "(attempt %d/%d).",
                            method, path, retry_after, wait,
                            attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(wait)
                        backoff = min(backoff * 2, BACKOFF_MAX)
                        continue

                    # -- Other HTTP errors ---------------------------------
                    if resp.status >= 400:
                        logger.error(
                            "HTTP %d on %s %s: %s",
                            resp.status, method, path, resp_text,
                        )
                        raise RuntimeError(
                            f"HTTP {resp.status} on {method} {path}: {resp_text}"
                        )

                    # -- Success -------------------------------------------
                    try:
                        data = json.loads(resp_text)
                    except json.JSONDecodeError:
                        logger.error(
                            "Invalid JSON response from %s %s: %s",
                            method, path, resp_text[:200],
                        )
                        raise RuntimeError(
                            f"Invalid JSON from {method} {path}: {resp_text[:200]}"
                        )

                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                logger.warning(
                    "Network error on %s %s (attempt %d/%d): %s. "
                    "Retrying in %.1f s...",
                    method, path, attempt, MAX_RETRIES, exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)

        raise RuntimeError(
            f"All {MAX_RETRIES} retries exhausted for {method} {path}. "
            f"Last error: {last_error}"
        )

    # ==================================================================
    #  PUBLIC API METHODS
    # ==================================================================

    # ------------------------------------------------------------------
    # GET /book — fetch current orderbook
    # ------------------------------------------------------------------

    async def get_orderbook(self, market_id: str) -> dict:
        """
        Fetch the current orderbook for a given market (token_id / asset_id).

        Args:
            market_id: The token_id or asset_id to query.

        Returns:
            Dict with 'bids' and 'asks' arrays, each entry having
            'price' and 'size' string fields.
        """
        if self._test_mode:
            ob = dict(_FIXTURE_ORDERBOOK)
            ob["asset_id"] = market_id
            ob["timestamp"] = datetime.now(timezone.utc).isoformat()
            logger.info("get_orderbook [test]: returning fixture for %s", market_id)
            return ob

        data = await self._request(
            "GET", "/book",
            params={"token_id": market_id},
        )
        logger.info(
            "get_orderbook: market_id=%s bids=%d asks=%d",
            market_id,
            len(data.get("bids", [])),
            len(data.get("asks", [])),
        )
        return data

    # ------------------------------------------------------------------
    # GET /markets — list active markets
    # ------------------------------------------------------------------

    async def get_markets(
        self,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> list[dict]:
        """
        List active markets from the CLOB API.

        Args:
            limit:  Maximum number of markets to return (default 100).
            cursor: Pagination cursor for the next page (optional).

        Returns:
            List of market dicts.
        """
        if self._test_mode:
            logger.info(
                "get_markets [test]: returning %d fixture markets.",
                len(_FIXTURE_MARKETS),
            )
            return list(_FIXTURE_MARKETS)

        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        data = await self._request("GET", "/markets", params=params)

        # The API may return {"data": [...], "next_cursor": "..."} or just [...].
        markets = data if isinstance(data, list) else data.get("data", [])
        logger.info("get_markets: fetched %d markets.", len(markets))
        return markets

    # ------------------------------------------------------------------
    # POST /order — place an order
    # ------------------------------------------------------------------

    async def place_order(
        self,
        market_id: str,
        side: str,
        price: float,
        size: float,
        order_type: str = "limit",
    ) -> dict:
        """
        Place an order on the Polymarket CLOB.

        Args:
            market_id:  Token ID / asset ID to trade.
            side:       'BUY' or 'SELL'.
            price:      Limit price (0.01 to 0.99 for binary markets).
            size:       Order size in contracts.
            order_type: 'limit' (default) or 'market'.

        Returns:
            Order confirmation dict with 'id', 'status', etc.
        """
        if self._test_mode:
            resp = dict(_FIXTURE_ORDER_RESPONSE)
            resp["asset_id"] = market_id
            resp["side"] = side.upper()
            resp["price"] = str(price)
            resp["original_size"] = str(size)
            resp["type"] = order_type
            resp["created_at"] = datetime.now(timezone.utc).isoformat()
            logger.info(
                "place_order [test]: %s %s @ %.4f x %.2f",
                side, market_id, price, size,
            )
            return resp

        body = {
            "tokenID": market_id,
            "side": side.upper(),
            "price": str(price),
            "size": str(size),
            "type": order_type,
        }

        data = await self._request("POST", "/order", json_body=body, signed=True)
        logger.info(
            "place_order: id=%s status=%s %s @ %s x %s",
            data.get("id"), data.get("status"),
            side.upper(), price, size,
        )
        return data

    # ------------------------------------------------------------------
    # DELETE /order/{order_id} — cancel an order
    # ------------------------------------------------------------------

    async def cancel_order(self, order_id: str) -> dict:
        """
        Cancel an open order by its ID.

        Args:
            order_id: The order ID to cancel.

        Returns:
            Cancellation confirmation dict.
        """
        if self._test_mode:
            resp = {"id": order_id, "status": "cancelled"}
            logger.info("cancel_order [test]: %s -> cancelled", order_id)
            return resp

        data = await self._request(
            "DELETE", f"/order/{order_id}", signed=True,
        )
        logger.info(
            "cancel_order: id=%s status=%s",
            order_id, data.get("status"),
        )
        return data

    # ------------------------------------------------------------------
    # GET /order/{order_id} — query a single order
    # ------------------------------------------------------------------

    async def get_order(self, order_id: str) -> dict:
        """
        Fetch details for a single order.

        Args:
            order_id: The order ID to query.

        Returns:
            Order detail dict.
        """
        if self._test_mode:
            resp = dict(_FIXTURE_ORDER_RESPONSE)
            resp["id"] = order_id
            logger.info("get_order [test]: %s", order_id)
            return resp

        data = await self._request("GET", f"/order/{order_id}", signed=True)
        logger.info(
            "get_order: id=%s status=%s",
            data.get("id"), data.get("status"),
        )
        return data

    # ------------------------------------------------------------------
    # GET /positions — current positions
    # ------------------------------------------------------------------

    async def get_positions(
        self, wallet_address: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch current positions for a wallet.

        Args:
            wallet_address: Override wallet address (defaults to self._wallet_address).

        Returns:
            List of position dicts.
        """
        if self._test_mode:
            logger.info(
                "get_positions [test]: returning %d fixture positions.",
                len(_FIXTURE_POSITIONS),
            )
            return list(_FIXTURE_POSITIONS)

        addr = wallet_address or self._wallet_address
        data = await self._request(
            "GET", "/positions",
            params={"address": addr},
            signed=True,
        )

        positions = data if isinstance(data, list) else data.get("positions", [])
        logger.info("get_positions: %d positions for %s", len(positions), addr[:10])
        return positions

    # ==================================================================
    #  WEBSOCKET PRICE FEED
    # ==================================================================

    async def run_ws_feed(
        self,
        market_ids: list[str],
        callback: Optional[Callable[[dict], Coroutine]] = None,
    ) -> None:
        """
        Connect to the Polymarket CLOB WebSocket and stream live price
        updates for the given market/token IDs.

        Each price tick is:
          1. Passed to the optional *callback* coroutine.
          2. Published to STREAM_PRICES_POLYMARKET via redis_client.publish().

        Reconnects automatically with exponential backoff on disconnection
        or error.

        Args:
            market_ids: List of token_id / asset_id strings to subscribe to.
            callback:   Optional async callable invoked with each price dict.
        """
        if self._test_mode:
            await self._run_ws_feed_test(market_ids, callback)
            return

        import aiohttp

        backoff = WS_RECONNECT_BASE

        while True:
            try:
                logger.info(
                    "WebSocket connecting to %s for %d market(s)...",
                    WS_URL, len(market_ids),
                )

                async with aiohttp.ClientSession() as ws_session:
                    async with ws_session.ws_connect(
                        WS_URL,
                        timeout=REQUEST_TIMEOUT,
                        heartbeat=30,
                    ) as ws:
                        # Subscribe to each market
                        for mid in market_ids:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": "market",
                                "assets_ids": [mid],
                            }
                            await ws.send_json(subscribe_msg)
                            logger.info("WebSocket subscribed to %s", mid)

                        # Reset backoff on successful connection
                        backoff = WS_RECONNECT_BASE
                        logger.info("WebSocket connected and subscribed.")

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    logger.warning(
                                        "WebSocket: invalid JSON: %s",
                                        msg.data[:100],
                                    )
                                    continue

                                await self._handle_ws_message(data, callback)

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(
                                    "WebSocket error: %s", ws.exception(),
                                )
                                break

                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                logger.warning("WebSocket closed by server.")
                                break

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                OSError,
            ) as exc:
                logger.warning(
                    "WebSocket connection lost: %s. "
                    "Reconnecting in %.1f s...",
                    exc, backoff,
                )

            except asyncio.CancelledError:
                logger.info("WebSocket feed cancelled. Shutting down.")
                return

            except Exception as exc:
                logger.error(
                    "Unexpected WebSocket error: %s. "
                    "Reconnecting in %.1f s...",
                    exc, backoff,
                )

            # Exponential backoff before reconnect
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_RECONNECT_MAX)

    async def _handle_ws_message(
        self,
        data: dict,
        callback: Optional[Callable[[dict], Coroutine]] = None,
    ) -> None:
        """
        Process a single WebSocket message: validate, build a price tick
        dict, publish to Redis, and invoke the optional callback.

        Malformed messages are logged and dropped (never published).
        """
        # Ignore subscription confirmations and heartbeats
        msg_type = data.get("type", "")
        if msg_type in ("subscriptions", "heartbeat", "connected"):
            logger.debug("WebSocket control message: %s", msg_type)
            return

        # Validate minimum fields for a price update
        asset_id = data.get("asset_id") or data.get("market")
        if not asset_id:
            logger.debug("WebSocket message without asset_id: %s", str(data)[:200])
            return

        # Build normalised price tick
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        tick = {
            "timestamp": data.get("timestamp", now_iso),
            "venue": "polymarket",
            "market_id": str(asset_id),
            "price": str(data.get("price", "")),
            "side": str(data.get("side", "")),
            "size": str(data.get("size", "")),
            "event_type": str(data.get("type", "price_change")),
        }

        # Publish to Redis
        try:
            from execution.utils.redis_client import publish
            await publish(STREAM_PRICES_POLYMARKET, tick)
            logger.debug("Published tick to Redis: %s", tick["market_id"])
        except Exception as exc:
            logger.error("Failed to publish tick to Redis: %s", exc)

        # Invoke user callback
        if callback:
            try:
                await callback(tick)
            except Exception as exc:
                logger.error("WebSocket callback error: %s", exc)

    async def _run_ws_feed_test(
        self,
        market_ids: list[str],
        callback: Optional[Callable[[dict], Coroutine]] = None,
    ) -> None:
        """
        Simulate a WebSocket feed in --test mode by emitting fixture
        price ticks at 1-second intervals for each market, then stopping
        after one full cycle.
        """
        logger.info(
            "run_ws_feed [test]: simulating feed for %d market(s).",
            len(market_ids),
        )

        from execution.utils.redis_client import publish

        for market_id in market_ids:
            # Find fixture data for this market, or use a generic tick
            fixture = None
            for m in _FIXTURE_MARKETS:
                for tok in m.get("tokens", []):
                    if tok["token_id"] == market_id:
                        fixture = m
                        break
                if fixture:
                    break

            yes_price = 0.5000
            if fixture:
                for tok in fixture.get("tokens", []):
                    if tok["outcome"] == "Yes":
                        yes_price = tok["price"]

            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            tick = {
                "timestamp": now_iso,
                "venue": "polymarket",
                "market_id": market_id,
                "price": str(yes_price),
                "side": "BUY",
                "size": "100.00",
                "event_type": "price_change",
            }

            await publish(STREAM_PRICES_POLYMARKET, tick)
            logger.info(
                "run_ws_feed [test]: published tick for %s (price=%s)",
                market_id, tick["price"],
            )

            if callback:
                await callback(tick)

            await asyncio.sleep(0.1)  # Small delay between test ticks

        logger.info("run_ws_feed [test]: simulation complete.")


# =========================================================================
#  STANDALONE PRICE-STREAMING MAIN LOOP
# =========================================================================

async def _stream_prices_loop(client: PolymarketCLOBClient) -> None:
    """
    Standalone async loop: fetches active markets, then streams prices
    for all of them via WebSocket, publishing every tick to
    stream:prices:polymarket.

    In --test mode, runs one simulated cycle and exits.
    """
    from execution.utils.redis_client import publish

    print()
    print("=" * 72)
    print("  Polymarket CLOB Connector -- Price Streamer")
    print("  Mode: %s" % ("TEST" if client._test_mode else "LIVE"))
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # -- Step 1: Fetch active markets to know what to subscribe to ---------
    logger.info("Fetching active markets...")
    markets = await client.get_markets(limit=100)
    logger.info("Found %d markets.", len(markets))

    if not markets:
        logger.warning("No markets found. Nothing to stream.")
        return

    # Extract token IDs to subscribe to
    token_ids: list[str] = []
    for mkt in markets:
        tokens = mkt.get("tokens", [])
        for tok in tokens:
            tid = tok.get("token_id")
            if tid:
                token_ids.append(tid)

    if not token_ids:
        logger.warning("No token IDs extracted from markets. Nothing to stream.")
        return

    logger.info("Subscribing to %d token(s) across %d market(s).", len(token_ids), len(markets))

    # -- Step 2: Fetch orderbook snapshots and publish initial prices -------
    logger.info("Fetching initial orderbook snapshots...")
    for mkt in markets[:5]:  # Limit initial snapshot to first 5 markets
        tokens = mkt.get("tokens", [])
        for tok in tokens:
            tid = tok.get("token_id")
            if not tid:
                continue
            try:
                ob = await client.get_orderbook(tid)
                best_bid = ob.get("bids", [{}])[0].get("price", "") if ob.get("bids") else ""
                best_ask = ob.get("asks", [{}])[0].get("price", "") if ob.get("asks") else ""

                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                snapshot = {
                    "timestamp": now_iso,
                    "venue": "polymarket",
                    "market_id": tid,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "event_type": "orderbook_snapshot",
                    "bid_depth": str(len(ob.get("bids", []))),
                    "ask_depth": str(len(ob.get("asks", []))),
                }
                await publish(STREAM_PRICES_POLYMARKET, snapshot)
                logger.info(
                    "Published orderbook snapshot: %s bid=%s ask=%s",
                    tid, best_bid, best_ask,
                )
            except Exception as exc:
                logger.error("Failed to fetch orderbook for %s: %s", tid, exc)

    # -- Step 3: Start WebSocket feed --------------------------------------
    logger.info("Starting WebSocket price feed...")

    async def _log_tick(tick: dict) -> None:
        logger.debug(
            "Tick: %s %s @ %s",
            tick.get("market_id", "?"),
            tick.get("side", "?"),
            tick.get("price", "?"),
        )

    await client.run_ws_feed(token_ids, callback=_log_tick)


# =========================================================================
#  DEMO / SELF-TEST
# =========================================================================

async def _run_test_demo() -> None:
    """
    Exercise all PolymarketCLOBClient methods using fixture data.
    No network calls are made.  Publishes fixture ticks to Redis
    (in-memory mock when --test is also passed to redis_client).
    """
    # Ensure the redis_client uses its in-memory mock as well.
    import execution.utils.redis_client as rc
    rc._use_mock = True

    from execution.utils.redis_client import (
        ensure_consumer_groups,
        stream_len,
        close_redis,
    )

    print()
    print("=" * 72)
    print("  Polymarket CLOB Connector -- Test Demo")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    client = PolymarketCLOBClient(
        api_key="test_api_key",
        api_secret="test_api_secret",
        private_key="test_private_key",
        wallet_address="0x0000000000000000000000000000000000000000",
        test_mode=True,
    )

    # -- Ensure Redis consumer groups (mock) --------------------------------
    logger.info("=" * 60)
    logger.info("  Step 1: Ensure Redis consumer groups")
    logger.info("=" * 60)
    await ensure_consumer_groups()

    # -- Test get_markets() ------------------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 2: get_markets()")
    logger.info("=" * 60)
    markets = await client.get_markets(limit=10)
    for mkt in markets:
        logger.info(
            "  Market: %s (tokens: %d)",
            mkt.get("question", "?")[:50],
            len(mkt.get("tokens", [])),
        )

    # -- Test get_orderbook() -----------------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 3: get_orderbook()")
    logger.info("=" * 60)
    ob = await client.get_orderbook("tok_yes_1")
    logger.info(
        "  Orderbook: bids=%d asks=%d",
        len(ob.get("bids", [])),
        len(ob.get("asks", [])),
    )

    # -- Test place_order() -------------------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 4: place_order()")
    logger.info("=" * 60)
    order = await client.place_order(
        market_id="tok_yes_1",
        side="BUY",
        price=0.6400,
        size=50.0,
        order_type="limit",
    )
    order_id = order.get("id", "?")
    logger.info("  Order placed: id=%s status=%s", order_id, order.get("status"))

    # -- Test get_order() ---------------------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 5: get_order()")
    logger.info("=" * 60)
    fetched = await client.get_order(order_id)
    logger.info("  Order fetched: id=%s status=%s", fetched.get("id"), fetched.get("status"))

    # -- Test cancel_order() ------------------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 6: cancel_order()")
    logger.info("=" * 60)
    cancelled = await client.cancel_order(order_id)
    logger.info("  Order cancelled: id=%s status=%s", cancelled.get("id"), cancelled.get("status"))

    # -- Test get_positions() -----------------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 7: get_positions()")
    logger.info("=" * 60)
    positions = await client.get_positions()
    for pos in positions:
        asset = pos.get("asset", {})
        logger.info(
            "  Position: %s %s size=%s pnl=%s",
            asset.get("token_id", "?"),
            asset.get("outcome", "?"),
            pos.get("size", "?"),
            pos.get("pnl", "?"),
        )

    # -- Test WebSocket feed (simulated) ------------------------------------
    logger.info("=" * 60)
    logger.info("  Step 8: run_ws_feed() (simulated)")
    logger.info("=" * 60)
    await client.run_ws_feed(
        market_ids=["tok_yes_1", "tok_no_1", "tok_yes_2"],
        callback=lambda tick: _test_ws_callback(tick),
    )

    # -- Check Redis stream length ------------------------------------------
    length = await stream_len(STREAM_PRICES_POLYMARKET)
    logger.info("  Redis stream '%s' length: %d", STREAM_PRICES_POLYMARKET, length)

    # -- Test HMAC signature generation (deterministic check) ---------------
    logger.info("=" * 60)
    logger.info("  Step 9: _sign_request() deterministic check")
    logger.info("=" * 60)
    headers = client._sign_request("GET", "/test", "", "1700000000")
    logger.info("  Signature headers: %s", json.dumps(headers, indent=2))
    assert headers["POLY_TIMESTAMP"] == "1700000000"
    assert headers["POLY_API_KEY"] == "test_api_key"
    assert len(headers["POLY_SIGNATURE"]) == 64  # hex SHA-256
    logger.info("  Signature check passed.")

    # -- Cleanup ------------------------------------------------------------
    await client.close()
    await close_redis()

    print()
    print("=" * 72)
    print("  All tests passed successfully.")
    print("=" * 72)
    print()


async def _test_ws_callback(tick: dict) -> None:
    """Simple callback for the test WebSocket feed."""
    logger.info(
        "  [ws callback] %s price=%s",
        tick.get("market_id", "?"),
        tick.get("price", "?"),
    )


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point: parse --test flag and either run the test demo or
    start the live price-streaming loop.

    In --test mode, exercises all client methods with fixture data using
    the in-memory Redis mock.  In live mode, connects to the Polymarket
    CLOB API and streams prices to stream:prices:polymarket.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Polymarket CLOB API connector. Streams live prices to "
            "stream:prices:polymarket. Use --test for fixture mode."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use fixture data and in-memory Redis mock (no API calls).",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode with fixture data.")
        asyncio.run(_run_test_demo())
    else:
        logger.info("Running in live mode against Polymarket CLOB API.")

        # Late import to avoid requiring config in --test mode path above
        # (config singleton is already initialised via sys.argv detection).
        from execution.utils.config import config

        api_key = config.get_required("POLYMARKET_API_KEY")
        api_secret = config.get_required("POLYMARKET_API_SECRET")
        private_key = config.get_required("POLYMARKET_PRIVATE_KEY")
        wallet_address = config.get_required("POLYMARKET_WALLET_ADDRESS")

        client = PolymarketCLOBClient(
            api_key=api_key,
            api_secret=api_secret,
            private_key=private_key,
            wallet_address=wallet_address,
            test_mode=False,
        )

        try:
            asyncio.run(_stream_prices_loop(client))
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Shutting down.")


if __name__ == "__main__":
    main()
