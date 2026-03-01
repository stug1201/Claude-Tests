#!/usr/bin/env python3
"""
kalshi_ws.py -- Kalshi WebSocket streaming connector.

Subscribes to live orderbook and trade updates for Kalshi markets via
WebSocket and publishes raw price ticks to stream:prices:kalshi.

Base URL: wss://api.elections.kalshi.com/trade-api/ws
Auth: Same Bearer token as REST API (obtained from KalshiRESTClient)

Message types handled:
    orderbook_snapshot  -- Full orderbook state on initial subscribe
    orderbook_delta     -- Incremental orderbook changes
    trade               -- Individual trade executions
    ticker              -- Periodic ticker summary (best bid/ask, last price)

Reconnection:
    Exponential backoff: 1s, 2s, 4s, 8s, 16s (max).  On auth expiry (401
    or token rejected), obtains a fresh token from the REST client before
    reconnecting.

Usage:
    python execution/connectors/kalshi_ws.py          # Live mode
    python execution/connectors/kalshi_ws.py --test   # Fixture mode

Environment variables (via execution.utils.config):
    KALSHI_EMAIL     -- Kalshi account email
    KALSHI_PASSWORD  -- Kalshi account password
    KALSHI_API_KEY   -- Kalshi API key (optional)

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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"

# ---------------------------------------------------------------------------
# Kalshi WebSocket constants
# ---------------------------------------------------------------------------
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws"

# Stream name for Redis publishing (matches redis_client.py constant)
STREAM_PRICES_KALSHI = "stream:prices:kalshi"

# ---------------------------------------------------------------------------
# Reconnection configuration
# ---------------------------------------------------------------------------
BACKOFF_BASE = 1        # seconds
BACKOFF_MAX = 16        # seconds
BACKOFF_MULTIPLIER = 2

# ---------------------------------------------------------------------------
# Fixture data for --test mode
# ---------------------------------------------------------------------------
_FIXTURE_MESSAGES: list[dict[str, Any]] = [
    {
        "type": "orderbook_snapshot",
        "market_ticker": "KXBTC-26FEB28-100K",
        "yes_bids": [
            {"price": 65, "quantity": 120},
            {"price": 64, "quantity": 250},
            {"price": 63, "quantity": 80},
        ],
        "yes_asks": [
            {"price": 67, "quantity": 100},
            {"price": 68, "quantity": 200},
            {"price": 70, "quantity": 50},
        ],
        "no_bids": [
            {"price": 33, "quantity": 100},
            {"price": 32, "quantity": 200},
        ],
        "no_asks": [
            {"price": 35, "quantity": 150},
            {"price": 36, "quantity": 80},
        ],
    },
    {
        "type": "orderbook_delta",
        "market_ticker": "KXBTC-26FEB28-100K",
        "side": "yes",
        "price": 66,
        "delta": 75,
    },
    {
        "type": "trade",
        "market_ticker": "KXBTC-26FEB28-100K",
        "side": "yes",
        "price": 66,
        "count": 50,
        "trade_id": "trade-fixture-001",
        "ts": 1740700800000,
    },
    {
        "type": "ticker",
        "market_ticker": "KXBTC-26FEB28-100K",
        "yes_bid": 65,
        "yes_ask": 67,
        "last_price": 66,
        "volume": 4200,
        "open_interest": 15800,
    },
    {
        "type": "orderbook_delta",
        "market_ticker": "KXBTC-26FEB28-100K",
        "side": "no",
        "price": 34,
        "delta": -30,
    },
    {
        "type": "trade",
        "market_ticker": "KXETH-26MAR15-5K",
        "side": "no",
        "price": 58,
        "count": 25,
        "trade_id": "trade-fixture-002",
        "ts": 1740700830000,
    },
    {
        "type": "ticker",
        "market_ticker": "KXETH-26MAR15-5K",
        "yes_bid": 41,
        "yes_ask": 43,
        "last_price": 42,
        "volume": 1850,
        "open_interest": 7200,
    },
    {
        "type": "orderbook_snapshot",
        "market_ticker": "KXETH-26MAR15-5K",
        "yes_bids": [
            {"price": 41, "quantity": 90},
            {"price": 40, "quantity": 180},
        ],
        "yes_asks": [
            {"price": 43, "quantity": 110},
            {"price": 45, "quantity": 60},
        ],
        "no_bids": [
            {"price": 57, "quantity": 110},
        ],
        "no_asks": [
            {"price": 59, "quantity": 90},
            {"price": 60, "quantity": 70},
        ],
    },
]


# ---------------------------------------------------------------------------
# KalshiWSClient
# ---------------------------------------------------------------------------

class KalshiWSClient:
    """
    Asynchronous WebSocket client for Kalshi live market data.

    Uses the same authentication token managed by KalshiRESTClient.  On
    connection or auth failure, reconnects with exponential backoff and
    obtains a fresh token from the REST client.

    Parsed price updates are published to the ``stream:prices:kalshi``
    Redis Stream via ``redis_client.publish()``.
    """

    def __init__(self, rest_client: Any) -> None:
        """
        Initialise the WebSocket client.

        Args:
            rest_client: A KalshiRESTClient instance used to obtain and
                         refresh the Bearer token.  Must expose:
                         - ``get_token() -> str`` (current valid token)
                         - ``refresh_token() -> str`` (force-refresh and return)
        """
        self._rest_client = rest_client
        self._ws: Optional[Any] = None
        self._subscribed_tickers: set[str] = set()
        self._running: bool = False
        self._backoff: float = BACKOFF_BASE
        logger.info("KalshiWSClient initialised.")

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Establish a WebSocket connection to Kalshi with the current
        auth token.

        Raises:
            ConnectionError: If the WebSocket handshake fails after the
                             underlying library raises.
        """
        # Late import so --test mode never requires websockets installed.
        try:
            import websockets  # type: ignore[import-untyped]
        except ImportError:
            logger.error(
                "websockets package is not installed. "
                "Install with: pip install websockets"
            )
            raise ImportError("websockets package required")

        token = self._rest_client.get_token()
        if not token:
            logger.warning("No token available; requesting fresh token.")
            token = await self._ensure_fresh_token()

        ws_url = KALSHI_WS_URL
        headers = {"Authorization": f"Bearer {token}"}

        logger.info("Connecting to Kalshi WebSocket: %s", ws_url)

        try:
            self._ws = await websockets.connect(
                ws_url,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            )
            # Reset backoff on successful connection.
            self._backoff = BACKOFF_BASE
            logger.info("WebSocket connection established.")
        except Exception as exc:
            logger.error("WebSocket connection failed: %s", exc)
            raise ConnectionError(f"WS handshake failed: {exc}") from exc

    async def _ensure_fresh_token(self) -> str:
        """
        Force-refresh the Kalshi auth token via the REST client.

        Returns:
            The new Bearer token string.

        Raises:
            RuntimeError: If the REST client cannot produce a valid token.
        """
        logger.info("Refreshing Kalshi auth token...")
        try:
            token = await asyncio.to_thread(self._rest_client.refresh_token)
            if not token:
                raise RuntimeError("REST client returned empty token")
            logger.info("Token refreshed successfully.")
            return token
        except Exception as exc:
            logger.error("Token refresh failed: %s", exc)
            raise RuntimeError(f"Cannot refresh Kalshi token: {exc}") from exc

    async def _close_ws(self) -> None:
        """Close the WebSocket connection if it is open."""
        if self._ws is not None:
            try:
                await self._ws.close()
                logger.info("WebSocket connection closed.")
            except Exception as exc:
                logger.warning("Error closing WebSocket: %s", exc)
            finally:
                self._ws = None

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    async def subscribe(self, tickers: list[str]) -> None:
        """
        Subscribe to orderbook/trade updates for the given market tickers.

        Args:
            tickers: List of Kalshi market ticker strings to subscribe to.

        Raises:
            ConnectionError: If no WebSocket connection is active.
        """
        if self._ws is None:
            raise ConnectionError("WebSocket is not connected. Call connect() first.")

        if not tickers:
            logger.warning("subscribe() called with empty ticker list.")
            return

        subscribe_msg = {
            "id": int(time.time() * 1000),
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "ticker", "trade"],
                "market_tickers": tickers,
            },
        }

        try:
            await self._ws.send(json.dumps(subscribe_msg))
            self._subscribed_tickers.update(tickers)
            logger.info(
                "Subscribed to %d ticker(s): %s",
                len(tickers),
                ", ".join(tickers[:5]) + ("..." if len(tickers) > 5 else ""),
            )
        except Exception as exc:
            logger.error("Failed to send subscribe message: %s", exc)
            raise

    async def unsubscribe(self, tickers: list[str]) -> None:
        """
        Unsubscribe from updates for the given market tickers.

        Args:
            tickers: List of Kalshi market ticker strings to unsubscribe from.

        Raises:
            ConnectionError: If no WebSocket connection is active.
        """
        if self._ws is None:
            raise ConnectionError("WebSocket is not connected. Call connect() first.")

        if not tickers:
            logger.warning("unsubscribe() called with empty ticker list.")
            return

        unsubscribe_msg = {
            "id": int(time.time() * 1000),
            "cmd": "unsubscribe",
            "params": {
                "channels": ["orderbook_delta", "ticker", "trade"],
                "market_tickers": tickers,
            },
        }

        try:
            await self._ws.send(json.dumps(unsubscribe_msg))
            self._subscribed_tickers.difference_update(tickers)
            logger.info(
                "Unsubscribed from %d ticker(s): %s",
                len(tickers),
                ", ".join(tickers[:5]) + ("..." if len(tickers) > 5 else ""),
            )
        except Exception as exc:
            logger.error("Failed to send unsubscribe message: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def _on_message(self, raw_msg: str) -> Optional[dict[str, Any]]:
        """
        Parse an incoming WebSocket message and publish price updates
        to Redis.

        Handles four message types:
            - orderbook_snapshot: Full orderbook state
            - orderbook_delta:   Incremental change
            - trade:             Individual trade execution
            - ticker:            Periodic summary

        Malformed messages are logged and dropped (never published).

        Args:
            raw_msg: Raw JSON string received from the WebSocket.

        Returns:
            The parsed message dict if valid, or None if malformed/ignored.
        """
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError as exc:
            logger.warning("Malformed JSON received, dropping: %s", exc)
            return None

        if not isinstance(msg, dict):
            logger.warning("Unexpected message format (not a dict), dropping.")
            return None

        msg_type = msg.get("type")

        # Ignore internal control messages (subscription confirmations, etc.)
        if msg_type not in ("orderbook_snapshot", "orderbook_delta", "trade", "ticker"):
            logger.debug("Ignoring message type: %s", msg_type)
            return None

        market_ticker = msg.get("market_ticker")
        if not market_ticker:
            logger.warning("Message missing market_ticker, dropping: %s", msg_type)
            return None

        # Build the price update payload for Redis.
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        price_update: dict[str, Any] = {
            "timestamp": now_iso,
            "venue": "kalshi",
            "market_ticker": market_ticker,
            "msg_type": msg_type,
        }

        if msg_type == "orderbook_snapshot":
            price_update["yes_bids"] = msg.get("yes_bids", [])
            price_update["yes_asks"] = msg.get("yes_asks", [])
            price_update["no_bids"] = msg.get("no_bids", [])
            price_update["no_asks"] = msg.get("no_asks", [])
            # Derive best bid/ask from snapshot for convenience.
            yes_bids = msg.get("yes_bids", [])
            yes_asks = msg.get("yes_asks", [])
            if yes_bids:
                price_update["yes_bid"] = max(b["price"] for b in yes_bids)
            if yes_asks:
                price_update["yes_ask"] = min(a["price"] for a in yes_asks)

        elif msg_type == "orderbook_delta":
            price_update["side"] = msg.get("side", "unknown")
            price_update["price"] = msg.get("price")
            price_update["delta"] = msg.get("delta")

        elif msg_type == "trade":
            price_update["side"] = msg.get("side", "unknown")
            price_update["price"] = msg.get("price")
            price_update["count"] = msg.get("count")
            price_update["trade_id"] = msg.get("trade_id", "")
            price_update["trade_ts"] = msg.get("ts")

        elif msg_type == "ticker":
            price_update["yes_bid"] = msg.get("yes_bid")
            price_update["yes_ask"] = msg.get("yes_ask")
            price_update["last_price"] = msg.get("last_price")
            price_update["volume"] = msg.get("volume")
            price_update["open_interest"] = msg.get("open_interest")

        # Publish to Redis.
        try:
            from execution.utils.redis_client import publish
            await publish(STREAM_PRICES_KALSHI, price_update)
            logger.debug(
                "Published %s for %s to Redis.", msg_type, market_ticker
            )
        except Exception as exc:
            logger.error(
                "Failed to publish %s update to Redis: %s", msg_type, exc
            )

        return price_update

    # ------------------------------------------------------------------
    # Main event loop
    # ------------------------------------------------------------------

    async def run_forever(
        self,
        callback: Optional[
            Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
        ] = None,
    ) -> None:
        """
        Main event loop: receive messages, parse, publish to Redis, and
        invoke the optional *callback* with each parsed update.

        Reconnects automatically on disconnect with exponential backoff.
        If the disconnection was caused by an auth error (401), a fresh
        token is obtained before reconnecting.

        Args:
            callback: Optional async callable invoked with each parsed
                      price update dict.  Useful for downstream consumers
                      that want real-time access without polling Redis.
        """
        self._running = True
        logger.info("Starting Kalshi WS event loop (run_forever).")

        while self._running:
            try:
                # Ensure we have an active connection.
                if self._ws is None:
                    await self.connect()

                    # Re-subscribe to any tickers that were active before
                    # the disconnect.
                    if self._subscribed_tickers:
                        tickers_list = list(self._subscribed_tickers)
                        logger.info(
                            "Re-subscribing to %d ticker(s) after reconnect.",
                            len(tickers_list),
                        )
                        await self.subscribe(tickers_list)

                # Read messages from the WebSocket.
                async for raw_msg in self._ws:  # type: ignore[union-attr]
                    parsed = await self._on_message(raw_msg)
                    if parsed and callback:
                        try:
                            await callback(parsed)
                        except Exception as cb_exc:
                            logger.error(
                                "Callback error (non-fatal): %s", cb_exc
                            )

            except Exception as exc:
                exc_str = str(exc).lower()
                is_auth_error = (
                    "401" in exc_str
                    or "unauthorized" in exc_str
                    or "token" in exc_str and "expired" in exc_str
                    or "auth" in exc_str and "fail" in exc_str
                )

                if is_auth_error:
                    logger.warning(
                        "Auth error detected (%s). Refreshing token before "
                        "reconnect...",
                        exc,
                    )
                    try:
                        await self._ensure_fresh_token()
                    except Exception as refresh_exc:
                        logger.error(
                            "Token refresh failed during reconnect: %s. "
                            "Halting connector.",
                            refresh_exc,
                        )
                        self._running = False
                        break
                else:
                    logger.warning("WebSocket disconnected: %s", exc)

                # Clean up the dead connection.
                await self._close_ws()

                if not self._running:
                    break

                # Exponential backoff before reconnect.
                logger.info(
                    "Reconnecting in %.1f s (backoff)...", self._backoff
                )
                await asyncio.sleep(self._backoff)
                self._backoff = min(
                    self._backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX
                )

        logger.info("Kalshi WS event loop stopped.")
        await self._close_ws()

    async def stop(self) -> None:
        """Signal the event loop to stop gracefully."""
        logger.info("Stop requested for Kalshi WS client.")
        self._running = False
        await self._close_ws()


# ---------------------------------------------------------------------------
# Mock REST client for --test mode
# ---------------------------------------------------------------------------

class _MockKalshiRESTClient:
    """
    Minimal mock of KalshiRESTClient for --test mode.

    Returns a static fixture token so no real authentication occurs.
    """

    def __init__(self) -> None:
        self._token = "test-fixture-token-kalshi-ws"
        logger.info("MockKalshiRESTClient initialised (--test mode).")

    def get_token(self) -> str:
        """Return the fixture token."""
        return self._token

    def refresh_token(self) -> str:
        """Simulate a token refresh."""
        self._token = f"test-fixture-token-refreshed-{int(time.time())}"
        logger.info("MockKalshiRESTClient: token refreshed -> %s", self._token)
        return self._token


# ---------------------------------------------------------------------------
# Test / fixture mode
# ---------------------------------------------------------------------------

async def _run_test_mode() -> None:
    """
    Simulate a stream of fixture price updates without making any
    network calls.

    Creates fixture data, feeds it through KalshiWSClient._on_message()
    to exercise the full parsing and Redis publishing pipeline, and
    prints each parsed update to stdout.
    """
    # Enable in-memory Redis mock.
    import execution.utils.redis_client as rc
    rc._use_mock = True

    print()
    print("=" * 72)
    print("  Kalshi WebSocket Connector -- Test Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # Ensure fixture directory exists.
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Write fixture messages to disk for reference.
    fixture_file = FIXTURES_DIR / "kalshi_ws_fixture.json"
    fixture_file.write_text(
        json.dumps(_FIXTURE_MESSAGES, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Fixture data written to %s", fixture_file)

    # Initialise client with mock REST client.
    mock_rest = _MockKalshiRESTClient()
    client = KalshiWSClient(rest_client=mock_rest)

    # Simulate processing each fixture message.
    print()
    print("  Processing %d fixture messages..." % len(_FIXTURE_MESSAGES))
    print("-" * 72)

    processed = 0
    for i, msg in enumerate(_FIXTURE_MESSAGES):
        raw_json = json.dumps(msg)
        parsed = await client._on_message(raw_json)

        if parsed:
            processed += 1
            ticker = parsed.get("market_ticker", "?")
            msg_type = parsed.get("msg_type", "?")

            # Format a summary line based on message type.
            if msg_type == "orderbook_snapshot":
                yes_bid = parsed.get("yes_bid", "?")
                yes_ask = parsed.get("yes_ask", "?")
                detail = f"yes_bid={yes_bid} yes_ask={yes_ask}"
            elif msg_type == "orderbook_delta":
                detail = (
                    f"side={parsed.get('side')} "
                    f"price={parsed.get('price')} "
                    f"delta={parsed.get('delta')}"
                )
            elif msg_type == "trade":
                detail = (
                    f"side={parsed.get('side')} "
                    f"price={parsed.get('price')} "
                    f"count={parsed.get('count')}"
                )
            elif msg_type == "ticker":
                detail = (
                    f"bid={parsed.get('yes_bid')} "
                    f"ask={parsed.get('yes_ask')} "
                    f"last={parsed.get('last_price')} "
                    f"vol={parsed.get('volume')}"
                )
            else:
                detail = ""

            print(f"  [{i + 1:>2}] {msg_type:<22} {ticker:<28} {detail}")
        else:
            print(f"  [{i + 1:>2}] <dropped/ignored>")

        # Simulate a small delay between messages.
        await asyncio.sleep(0.15)

    # Verify Redis stream length.
    from execution.utils.redis_client import stream_len, close_redis
    length = await stream_len(STREAM_PRICES_KALSHI)

    print("-" * 72)
    print(f"  Processed: {processed}/{len(_FIXTURE_MESSAGES)} messages")
    print(f"  Redis stream '{STREAM_PRICES_KALSHI}' length: {length}")
    print()

    # Test malformed message handling.
    print("  Testing malformed message handling...")
    malformed_cases = [
        "not valid json at all {{{{",
        json.dumps(["unexpected", "array"]),
        json.dumps({"type": "orderbook_delta"}),  # missing market_ticker
        json.dumps({"type": "unknown_type", "market_ticker": "X"}),
    ]
    for case in malformed_cases:
        result = await client._on_message(case)
        status = "dropped" if result is None else "ERROR: should have been dropped"
        print(f"    {case[:50]:<52} -> {status}")

    await close_redis()

    print()
    print("=" * 72)
    print("  Test mode complete.  All fixture messages processed.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Live mode
# ---------------------------------------------------------------------------

async def _run_live() -> None:
    """
    Start the Kalshi WebSocket connector in live mode.

    Creates a KalshiRESTClient for token management, connects the WS
    client, and runs the event loop indefinitely.
    """
    # Late import so --test mode never requires kalshi_rest dependencies.
    try:
        from execution.connectors.kalshi_rest import KalshiRESTClient
    except ImportError as exc:
        logger.error(
            "Cannot import KalshiRESTClient. Ensure kalshi_rest.py is "
            "implemented: %s",
            exc,
        )
        sys.exit(1)

    from execution.utils.config import config

    # Build REST client for token management.
    rest_client = KalshiRESTClient()
    ws_client = KalshiWSClient(rest_client=rest_client)

    # Define an optional callback that logs each update.
    async def _log_callback(update: dict[str, Any]) -> None:
        logger.info(
            "Price update: %s %s %s",
            update.get("msg_type"),
            update.get("market_ticker"),
            {k: v for k, v in update.items()
             if k not in ("timestamp", "venue", "market_ticker", "msg_type")},
        )

    try:
        await ws_client.run_forever(callback=_log_callback)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down.")
        await ws_client.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse --test flag and run the appropriate mode.

    --test mode simulates a stream of fixture price updates through
    the full parsing and Redis publishing pipeline without any network
    calls.

    Live mode connects to Kalshi via WebSocket, subscribes to
    configured markets, and publishes price ticks to Redis indefinitely.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Kalshi WebSocket streaming connector. "
            "Subscribes to live market data and publishes to Redis."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Simulate fixture price updates without network calls. "
            "Uses in-memory Redis mock."
        ),
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode with fixture data.")
        asyncio.run(_run_test_mode())
    else:
        logger.info("Running in live mode against Kalshi WebSocket.")
        asyncio.run(_run_live())


if __name__ == "__main__":
    main()
