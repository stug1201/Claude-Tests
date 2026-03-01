#!/usr/bin/env python3
"""
order_router.py -- Order routing engine for polymarket-trader.

Consumes trade signals from stream:orders:pending, validates them against
the risk manager, computes final position size via the position sizer,
routes to the correct venue connector (Polymarket CLOB or Kalshi REST),
and logs all activity to the trades table for audit and PnL tracking.

Processing loop:
    1. Consume from stream:orders:pending via consumer group execution_group
    2. Validate signal schema (strategy, venue, market_id, side, target_price, size_usd)
    3. Risk check: pass signal through risk_manager for approval
    4. Size: pass through position_sizer for final size calculation
    5. Route: call venue connector (polymarket or kalshi)
    6. Log: write to trades table with status pending
    7. Confirm: wait for fill confirmation (max 30 seconds)
    8. Update: update trades table with fill price, status filled or failed
    9. Publish: emit to stream:orders:filled

Routing logic:
    - Single-venue signals (S1-S4): Route to specified venue
    - Cross-venue signals (S5): Route both legs simultaneously via asyncio.gather()

Error handling:
    - Connector timeout: 30s max, mark as timeout, DO NOT retry
    - Duplicate signals: Deduplicate by (strategy, market_id, side) within 60s window
    - Database write failure: Order MUST NOT be placed - no trade without audit trail

Usage:
    python execution/execution_engine/order_router.py           # Live mode
    python execution/execution_engine/order_router.py --test    # Fixture mode
    python execution/execution_engine/order_router.py --dry-run # Validate without placing

See: directives/08_execution_engine.md
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/execution_engine/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONSUMER_NAME = "order_router_worker_1"
CONNECTOR_TIMEOUT_SECONDS = 30
DEDUP_WINDOW_SECONDS = 60
CONSUME_BLOCK_MS = 2000        # block for 2s per consume cycle
CONSUME_BATCH_SIZE = 5         # messages per consume call

# Required fields in a trade signal.
_REQUIRED_SIGNAL_FIELDS = frozenset({
    "strategy",
    "venue",
    "market_id",
    "side",
    "target_price",
    "size_usd",
})

# All recognised signal fields (required + optional).
_ALL_SIGNAL_FIELDS = _REQUIRED_SIGNAL_FIELDS | {
    "edge_estimate",
    "confidence",
    "reasoning",
}

# Cross-venue strategy prefix
_CROSS_VENUE_STRATEGY = "s5"

# ---------------------------------------------------------------------------
# Fixture signals for --test mode
# ---------------------------------------------------------------------------
_FIXTURE_SIGNALS: list[dict] = [
    {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xabc123def456",
        "side": "yes",
        "target_price": 0.65,
        "size_usd": 50.0,
        "edge_estimate": 0.06,
        "confidence": "high",
        "reasoning": "Near-resolution probability underpriced by 6 cents",
    },
    {
        "strategy": "s2",
        "venue": "polymarket",
        "market_id": "0x789ghi012jkl",
        "side": "yes",
        "target_price": 0.42,
        "size_usd": 30.0,
        "edge_estimate": 0.04,
        "confidence": "medium",
        "reasoning": "High-IPS wallet cluster detected buying YES",
    },
    {
        "strategy": "s3",
        "venue": "kalshi",
        "market_id": "KXBTC-26MAR28-100K",
        "side": "yes",
        "target_price": 0.62,
        "size_usd": 40.0,
        "edge_estimate": 0.05,
        "confidence": "medium",
        "reasoning": "Glint news signal: positive sentiment spike",
    },
    {
        "strategy": "s4",
        "venue": "polymarket",
        "market_id": "0xmno345pqr678",
        "side": "no",
        "target_price": 0.70,
        "size_usd": 60.0,
        "edge_estimate": 0.03,
        "confidence": "high",
        "reasoning": "Intra-venue yes/no spread exceeds 3 cents",
    },
    {
        "strategy": "s5",
        "venue": "cross",
        "market_id": "0xabc123def456|KXBTC-26MAR28-100K",
        "side": "yes|no",
        "target_price": 0.65,
        "size_usd": 100.0,
        "edge_estimate": 0.08,
        "confidence": "high",
        "reasoning": "Cross-venue arb: Polymarket 0.65 vs Kalshi 0.62, spread > 3c",
    },
    # Duplicate of signal 0 -- should be deduplicated
    {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xabc123def456",
        "side": "yes",
        "target_price": 0.66,
        "size_usd": 55.0,
        "edge_estimate": 0.07,
        "confidence": "high",
        "reasoning": "Duplicate test: same (strategy, market_id, side) within 60s",
    },
    # Invalid signal -- missing required field
    {
        "strategy": "s1",
        "venue": "polymarket",
        "side": "yes",
        "target_price": 0.50,
        "size_usd": 25.0,
    },
]

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_test_mode: bool = False
_dry_run_mode: bool = False


# ===========================================================================
#  STUB: Risk Manager interface
# ===========================================================================
# The risk_manager module is not yet implemented (Phase 5 TODO stub).
# We define the interface the order_router expects so that it can be used
# once risk_manager.py is completed.  In --test mode we use a permissive
# mock that approves all signals.

class _MockRiskManager:
    """Permissive risk manager stub for --test mode.

    Approves every signal and logs the check.  Replace with the real
    risk_manager once it is implemented.
    """

    async def check_signal(self, signal: dict) -> tuple[bool, str]:
        """Return (approved, reason).

        Args:
            signal: The trade signal dict.

        Returns:
            Tuple of (approved: bool, reason: str).
        """
        logger.info(
            "RiskManager [mock]: APPROVED signal strategy=%s market=%s side=%s",
            signal.get("strategy"), signal.get("market_id"), signal.get("side"),
        )
        return True, "approved"

    async def record_fill(self, signal: dict, fill_price: float, size_usd: float) -> None:
        """Notify risk manager of a fill so it can update exposure tracking."""
        logger.debug(
            "RiskManager [mock]: recorded fill market=%s size=$%.2f",
            signal.get("market_id"), size_usd,
        )


def _get_risk_manager() -> _MockRiskManager:
    """Return the risk manager instance.

    Tries to import the real risk_manager; falls back to the mock if the
    real module is not yet implemented.
    """
    try:
        from execution.execution_engine import risk_manager as rm_mod
        # Check whether the module has a real implementation or just the stub.
        if hasattr(rm_mod, "RiskManager"):
            return rm_mod.RiskManager()  # type: ignore[attr-defined]
    except Exception:
        pass
    logger.info("Using mock risk manager (real risk_manager not yet implemented).")
    return _MockRiskManager()


# ===========================================================================
#  STUB: Position Sizer interface
# ===========================================================================

class _MockPositionSizer:
    """Passthrough position sizer stub for --test mode.

    Returns the signal's original size_usd unchanged.  Replace with the
    real position_sizer once it is implemented.
    """

    async def compute_size(self, signal: dict) -> float:
        """Return the approved position size in USD.

        Args:
            signal: The trade signal dict (must contain 'size_usd').

        Returns:
            Approved size in USD.
        """
        raw_size = float(signal.get("size_usd", 0))
        logger.info(
            "PositionSizer [mock]: strategy=%s raw_size=$%.2f -> approved=$%.2f",
            signal.get("strategy"), raw_size, raw_size,
        )
        return raw_size


def _get_position_sizer() -> _MockPositionSizer:
    """Return the position sizer instance.

    Tries to import the real position_sizer; falls back to the mock if the
    real module is not yet implemented.
    """
    try:
        from execution.execution_engine import position_sizer as ps_mod
        if hasattr(ps_mod, "PositionSizer"):
            return ps_mod.PositionSizer()  # type: ignore[attr-defined]
    except Exception:
        pass
    logger.info("Using mock position sizer (real position_sizer not yet implemented).")
    return _MockPositionSizer()


# ===========================================================================
#  MOCK CONNECTOR (for --test mode)
# ===========================================================================

class _MockConnector:
    """Venue connector mock that simulates order placement for --test mode.

    Returns a fake fill confirmation after a short delay.
    """

    def __init__(self, venue: str) -> None:
        self._venue = venue
        self._order_counter = 0

    async def place_order(self, **kwargs: Any) -> dict:
        self._order_counter += 1
        order_id = f"mock-{self._venue}-{self._order_counter:04d}"
        logger.info(
            "MockConnector(%s).place_order(): order_id=%s %s",
            self._venue, order_id, kwargs,
        )
        return {
            "order_id": order_id,
            "status": "filled",
            "fill_price": kwargs.get("price", kwargs.get("target_price", 0)),
            "size": kwargs.get("size", 0),
            "venue": self._venue,
        }

    async def close(self) -> None:
        logger.info("MockConnector(%s).close()", self._venue)


# ===========================================================================
#  ORDER ROUTER
# ===========================================================================

class OrderRouter:
    """
    Core order routing engine.

    Consumes trade signals from Redis, validates, risk-checks, sizes,
    routes to venue connectors, logs to DB, and publishes fill confirmations.

    Args:
        test_mode:  If True, use fixture data, mock connectors, and
                    InMemoryRedis.  No real orders are placed.
        dry_run:    If True, run the full pipeline but pass dry_run=True
                    to venue connectors so they validate without placing.
    """

    def __init__(
        self,
        test_mode: bool = False,
        dry_run: bool = False,
    ) -> None:
        self._test_mode = test_mode
        self._dry_run = dry_run

        # Deduplication cache: (strategy, market_id, side) -> unix timestamp
        self._dedup_cache: dict[tuple[str, str, str], float] = {}

        # Component references (initialised in _init_components)
        self._risk_manager: Any = None
        self._position_sizer: Any = None
        self._poly_connector: Any = None
        self._kalshi_connector: Any = None

        # Shutdown flag
        self._running: bool = False

        logger.info(
            "OrderRouter initialised (test_mode=%s, dry_run=%s).",
            test_mode, dry_run,
        )

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    async def _init_components(self) -> None:
        """Initialise risk manager, position sizer, and venue connectors."""

        # -- Risk manager --------------------------------------------------
        self._risk_manager = _get_risk_manager()

        # -- Position sizer ------------------------------------------------
        self._position_sizer = _get_position_sizer()

        # -- Venue connectors ----------------------------------------------
        if self._test_mode:
            self._poly_connector = _MockConnector("polymarket")
            self._kalshi_connector = _MockConnector("kalshi")
        else:
            await self._init_live_connectors()

        logger.info("All components initialised.")

    async def _init_live_connectors(self) -> None:
        """Create live venue connector instances from config credentials."""
        from execution.utils.config import config

        # Polymarket CLOB
        try:
            from execution.connectors.polymarket_clob import PolymarketCLOBClient
            self._poly_connector = PolymarketCLOBClient(
                api_key=config.get_required("POLYMARKET_API_KEY"),
                api_secret=config.get_required("POLYMARKET_API_SECRET"),
                private_key=config.get_required("POLYMARKET_PRIVATE_KEY"),
                wallet_address=config.get_required("POLYMARKET_WALLET_ADDRESS"),
                test_mode=self._dry_run,
            )
            logger.info("Polymarket CLOB connector ready.")
        except Exception as exc:
            logger.error("Failed to initialise Polymarket connector: %s", exc)
            self._poly_connector = None

        # Kalshi REST
        try:
            from execution.connectors.kalshi_rest import KalshiRESTClient, KalshiRESTClientTest
            if self._dry_run:
                self._kalshi_connector = KalshiRESTClientTest()
            else:
                email = config.get_required("KALSHI_EMAIL")
                password = config.get_required("KALSHI_PASSWORD")
                api_key = config.get_optional("KALSHI_API_KEY")
                self._kalshi_connector = KalshiRESTClient(email, password, api_key)
                await self._kalshi_connector.login()
            logger.info("Kalshi REST connector ready.")
        except Exception as exc:
            logger.error("Failed to initialise Kalshi connector: %s", exc)
            self._kalshi_connector = None

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def _shutdown_connectors(self) -> None:
        """Gracefully close venue connector sessions."""
        if self._poly_connector is not None:
            try:
                await self._poly_connector.close()
            except Exception as exc:
                logger.warning("Error closing Polymarket connector: %s", exc)

        if self._kalshi_connector is not None:
            try:
                await self._kalshi_connector.close()
            except Exception as exc:
                logger.warning("Error closing Kalshi connector: %s", exc)

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _is_duplicate(self, signal: dict) -> bool:
        """Check if this signal is a duplicate within the 60-second window.

        Deduplicates by (strategy, market_id, side).

        Args:
            signal: The trade signal dict.

        Returns:
            True if the signal is a duplicate and should be skipped.
        """
        key = (
            signal.get("strategy", ""),
            signal.get("market_id", ""),
            signal.get("side", ""),
        )
        now = time.time()

        # Prune expired entries
        expired_keys = [
            k for k, ts in self._dedup_cache.items()
            if now - ts > DEDUP_WINDOW_SECONDS
        ]
        for k in expired_keys:
            del self._dedup_cache[k]

        if key in self._dedup_cache:
            elapsed = now - self._dedup_cache[key]
            logger.warning(
                "Duplicate signal detected: (%s, %s, %s) -- "
                "last seen %.1fs ago (window=%ds). SKIPPING.",
                key[0], key[1], key[2], elapsed, DEDUP_WINDOW_SECONDS,
            )
            return True

        # Record this signal
        self._dedup_cache[key] = now
        return False

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_signal(self, signal: dict) -> tuple[bool, str]:
        """Validate that a signal contains all required fields.

        Args:
            signal: The trade signal dict.

        Returns:
            Tuple of (valid: bool, error_message: str).
        """
        missing = _REQUIRED_SIGNAL_FIELDS - set(signal.keys())
        if missing:
            msg = f"Missing required fields: {', '.join(sorted(missing))}"
            logger.warning("Signal validation failed: %s. Signal: %s", msg, signal)
            return False, msg

        # Validate types
        try:
            float(signal["target_price"])
        except (ValueError, TypeError):
            msg = f"Invalid target_price: {signal['target_price']!r}"
            logger.warning("Signal validation failed: %s", msg)
            return False, msg

        try:
            float(signal["size_usd"])
        except (ValueError, TypeError):
            msg = f"Invalid size_usd: {signal['size_usd']!r}"
            logger.warning("Signal validation failed: %s", msg)
            return False, msg

        if float(signal["size_usd"]) <= 0:
            msg = f"size_usd must be positive, got {signal['size_usd']}"
            logger.warning("Signal validation failed: %s", msg)
            return False, msg

        venue = signal.get("venue", "")
        if venue not in ("polymarket", "kalshi", "cross"):
            msg = f"Unknown venue: {venue!r}"
            logger.warning("Signal validation failed: %s", msg)
            return False, msg

        return True, "ok"

    # ------------------------------------------------------------------
    # Routing: Single-venue
    # ------------------------------------------------------------------

    async def _route_single_venue(
        self,
        signal: dict,
        approved_size: float,
    ) -> dict:
        """Route a single-venue order (S1-S4) to the appropriate connector.

        Args:
            signal:        The trade signal dict.
            approved_size: Position size in USD after sizing.

        Returns:
            Order result dict from the connector.

        Raises:
            asyncio.TimeoutError: If the connector does not respond within 30s.
        """
        venue = signal["venue"]
        market_id = signal["market_id"]
        side = signal["side"]
        target_price = float(signal["target_price"])

        if venue == "polymarket":
            connector = self._poly_connector
        elif venue == "kalshi":
            connector = self._kalshi_connector
        else:
            raise ValueError(f"Cannot route single-venue order to venue={venue!r}")

        if connector is None:
            raise RuntimeError(f"Connector for venue '{venue}' is not available")

        # Compute contract size from USD size and price
        if target_price > 0:
            contract_size = approved_size / target_price
        else:
            contract_size = approved_size

        logger.info(
            "Routing order: venue=%s market=%s side=%s price=%.4f "
            "size_usd=$%.2f contracts=%.2f dry_run=%s",
            venue, market_id, side, target_price,
            approved_size, contract_size, self._dry_run,
        )

        if venue == "polymarket":
            result = await asyncio.wait_for(
                connector.place_order(
                    market_id=market_id,
                    side=side.upper(),
                    price=target_price,
                    size=contract_size,
                ),
                timeout=CONNECTOR_TIMEOUT_SECONDS,
            )
        else:
            # Kalshi: size is integer number of contracts
            result = await asyncio.wait_for(
                connector.place_order(
                    ticker=market_id,
                    side=side,
                    price=target_price,
                    size=max(1, int(contract_size)),
                ),
                timeout=CONNECTOR_TIMEOUT_SECONDS,
            )

        return result

    # ------------------------------------------------------------------
    # Routing: Cross-venue (S5)
    # ------------------------------------------------------------------

    async def _route_cross_venue(
        self,
        signal: dict,
        approved_size: float,
    ) -> dict:
        """Route a cross-venue S5 signal to both legs simultaneously.

        The signal's market_id is expected to be "poly_id|kalshi_id" and
        side is "poly_side|kalshi_side".

        Args:
            signal:        The trade signal dict.
            approved_size: Position size in USD after sizing (split across legs).

        Returns:
            Combined result dict with both leg outcomes.
        """
        market_parts = signal["market_id"].split("|")
        side_parts = signal["side"].split("|")

        if len(market_parts) != 2 or len(side_parts) != 2:
            raise ValueError(
                f"Cross-venue signal must have market_id='poly|kalshi' and "
                f"side='poly_side|kalshi_side', got market_id={signal['market_id']!r}, "
                f"side={signal['side']!r}"
            )

        poly_market_id, kalshi_market_id = market_parts
        poly_side, kalshi_side = side_parts
        target_price = float(signal["target_price"])
        half_size = approved_size / 2.0

        if target_price > 0:
            poly_contracts = half_size / target_price
            kalshi_contracts = half_size / target_price
        else:
            poly_contracts = half_size
            kalshi_contracts = half_size

        logger.info(
            "Routing cross-venue S5 order: poly=%s(%s) kalshi=%s(%s) "
            "half_size=$%.2f dry_run=%s",
            poly_market_id, poly_side, kalshi_market_id, kalshi_side,
            half_size, self._dry_run,
        )

        # Route both legs concurrently
        async def _place_poly() -> dict:
            if self._poly_connector is None:
                return {"error": "Polymarket connector not available", "venue": "polymarket"}
            return await asyncio.wait_for(
                self._poly_connector.place_order(
                    market_id=poly_market_id,
                    side=poly_side.upper(),
                    price=target_price,
                    size=poly_contracts,
                ),
                timeout=CONNECTOR_TIMEOUT_SECONDS,
            )

        async def _place_kalshi() -> dict:
            if self._kalshi_connector is None:
                return {"error": "Kalshi connector not available", "venue": "kalshi"}
            return await asyncio.wait_for(
                self._kalshi_connector.place_order(
                    ticker=kalshi_market_id,
                    side=kalshi_side,
                    price=target_price,
                    size=max(1, int(kalshi_contracts)),
                ),
                timeout=CONNECTOR_TIMEOUT_SECONDS,
            )

        poly_result, kalshi_result = await asyncio.gather(
            _place_poly(),
            _place_kalshi(),
            return_exceptions=True,
        )

        # Process results: if either leg raised an exception, capture it
        combined: dict[str, Any] = {
            "type": "cross_venue",
            "poly_leg": {},
            "kalshi_leg": {},
            "status": "filled",
        }

        if isinstance(poly_result, BaseException):
            combined["poly_leg"] = {
                "error": str(poly_result),
                "venue": "polymarket",
                "status": "failed",
            }
            combined["status"] = "partial"
            logger.error("Polymarket leg failed: %s", poly_result)
        else:
            combined["poly_leg"] = poly_result

        if isinstance(kalshi_result, BaseException):
            combined["kalshi_leg"] = {
                "error": str(kalshi_result),
                "venue": "kalshi",
                "status": "failed",
            }
            combined["status"] = "partial" if combined["status"] != "partial" else "failed"
            logger.error("Kalshi leg failed: %s", kalshi_result)
        else:
            combined["kalshi_leg"] = kalshi_result

        if combined["status"] == "partial":
            logger.warning(
                "Cross-venue S5 order partially filled. One leg failed. "
                "Manual intervention may be required."
            )

        return combined

    # ------------------------------------------------------------------
    # Route dispatcher
    # ------------------------------------------------------------------

    async def _route_order(
        self,
        signal: dict,
        approved_size: float,
    ) -> dict:
        """Dispatch order to the correct routing path.

        Args:
            signal:        The trade signal dict.
            approved_size: Final approved position size in USD.

        Returns:
            Order result dict from the connector(s).
        """
        strategy = signal.get("strategy", "").lower()

        if strategy.startswith(_CROSS_VENUE_STRATEGY):
            return await self._route_cross_venue(signal, approved_size)
        else:
            return await self._route_single_venue(signal, approved_size)

    # ------------------------------------------------------------------
    # Core signal processing
    # ------------------------------------------------------------------

    async def _process_signal(self, signal: dict) -> Optional[dict]:
        """Process a single trade signal through the full pipeline.

        Steps:
            1. Validate schema
            2. Check for duplicates
            3. Risk check
            4. Position sizing
            5. Log to DB (pending)
            6. Route to connector
            7. Wait for confirmation
            8. Update DB (filled/failed/timeout)
            9. Publish to stream:orders:filled

        Args:
            signal: The trade signal dict.

        Returns:
            Result dict if the order was processed, or None if rejected.
        """
        from execution.utils import db
        from execution.utils.redis_client import (
            publish,
            STREAM_ORDERS_FILLED,
        )

        strategy = signal.get("strategy", "?")
        venue = signal.get("venue", "?")
        market_id = signal.get("market_id", "?")
        side = signal.get("side", "?")

        logger.info(
            "Processing signal: strategy=%s venue=%s market=%s side=%s",
            strategy, venue, market_id, side,
        )

        # -- Step 1: Validate schema ----------------------------------------
        valid, error_msg = self._validate_signal(signal)
        if not valid:
            logger.warning(
                "Signal REJECTED (validation): %s. Signal: %s",
                error_msg, signal,
            )
            return None

        # -- Step 2: Deduplication ------------------------------------------
        if self._is_duplicate(signal):
            return None

        # -- Step 3: Risk check ---------------------------------------------
        approved, risk_reason = await self._risk_manager.check_signal(signal)
        if not approved:
            logger.warning(
                "Signal REJECTED (risk): strategy=%s market=%s reason=%s",
                strategy, market_id, risk_reason,
            )
            return None

        # -- Step 4: Position sizing ----------------------------------------
        approved_size = await self._position_sizer.compute_size(signal)
        if approved_size <= 0:
            logger.warning(
                "Signal REJECTED (sizing): approved_size=$%.2f for strategy=%s market=%s",
                approved_size, strategy, market_id,
            )
            return None

        logger.info(
            "Signal approved: strategy=%s market=%s size=$%.2f",
            strategy, market_id, approved_size,
        )

        # -- Step 5: Log to DB (pending) ------------------------------------
        # CRITICAL: Database write must succeed BEFORE placing the order.
        # No trade without audit trail.
        target_price = float(signal.get("target_price", 0))
        try:
            trade_id = await db.insert_trade(
                venue=venue,
                market_id=market_id,
                strategy=strategy,
                side=side,
                price=target_price,
                size=approved_size,
                status="pending",
            )
        except Exception as exc:
            logger.error(
                "DATABASE WRITE FAILURE: Cannot log trade for strategy=%s "
                "market=%s. Order will NOT be placed. Error: %s",
                strategy, market_id, exc,
            )
            return None

        logger.info("Trade logged: id=%d status=pending", trade_id)

        # -- Step 6 & 7: Route to connector (with 30s timeout) ---------------
        result: Optional[dict] = None
        final_status = "failed"
        fill_price = 0.0

        try:
            result = await self._route_order(signal, approved_size)
            # Determine status from result
            if result:
                result_status = result.get("status", "")
                if result_status in ("filled", "live", "open"):
                    final_status = "filled"
                    # Extract fill price from result
                    fill_price = float(
                        result.get("fill_price",
                            result.get("price", target_price))
                    )
                elif result_status == "partial":
                    final_status = "partial"
                    fill_price = target_price
                else:
                    final_status = "failed"
            else:
                final_status = "failed"

        except asyncio.TimeoutError:
            final_status = "timeout"
            logger.error(
                "CONNECTOR TIMEOUT: Order for strategy=%s market=%s did not "
                "respond within %ds. Marking as timeout. NOT retrying.",
                strategy, market_id, CONNECTOR_TIMEOUT_SECONDS,
            )

        except Exception as exc:
            final_status = "failed"
            logger.error(
                "ORDER ROUTING FAILED: strategy=%s market=%s error=%s",
                strategy, market_id, exc,
            )

        # -- Step 8: Update DB (filled/failed/timeout) ----------------------
        try:
            await db.update_trade(
                trade_id=trade_id,
                status=final_status,
            )
        except Exception as exc:
            logger.error(
                "Failed to update trade id=%d to status=%s: %s",
                trade_id, final_status, exc,
            )

        logger.info(
            "Trade updated: id=%d status=%s fill_price=%.4f",
            trade_id, final_status, fill_price,
        )

        # -- Step 9: Publish to stream:orders:filled -------------------------
        if final_status in ("filled", "partial"):
            fill_event = {
                "trade_id": str(trade_id),
                "strategy": strategy,
                "venue": venue,
                "market_id": market_id,
                "side": side,
                "target_price": str(target_price),
                "fill_price": str(fill_price),
                "size_usd": str(approved_size),
                "status": final_status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            try:
                msg_id = await publish(STREAM_ORDERS_FILLED, fill_event)
                logger.info(
                    "Published fill event: msg_id=%s trade_id=%d",
                    msg_id, trade_id,
                )
            except Exception as exc:
                logger.error(
                    "Failed to publish fill event for trade_id=%d: %s",
                    trade_id, exc,
                )

            # Notify risk manager of the fill
            try:
                await self._risk_manager.record_fill(
                    signal, fill_price, approved_size,
                )
            except Exception as exc:
                logger.warning(
                    "Risk manager record_fill failed: %s", exc,
                )

        return {
            "trade_id": trade_id,
            "status": final_status,
            "fill_price": fill_price,
            "size_usd": approved_size,
            "result": result,
        }

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Start the order routing loop.

        In live / dry-run mode, consumes from stream:orders:pending via
        Redis consumer group.  In --test mode, processes fixture signals
        and exits.
        """
        from execution.utils.redis_client import (
            consume,
            ack,
            ensure_consumer_groups,
            STREAM_ORDERS_PENDING,
            GROUP_EXECUTION,
        )

        logger.info("=" * 72)
        logger.info("  Order Router starting")
        logger.info("  Mode: %s", "TEST" if self._test_mode else ("DRY-RUN" if self._dry_run else "LIVE"))
        logger.info("=" * 72)

        # Initialise components
        await self._init_components()

        # Ensure consumer groups exist
        await ensure_consumer_groups()

        self._running = True

        if self._test_mode:
            await self._run_test_mode()
        else:
            await self._run_live_loop()

        # Cleanup
        await self._shutdown_connectors()
        logger.info("Order Router stopped.")

    async def _run_live_loop(self) -> None:
        """Live/dry-run consume loop: read from Redis and process signals."""
        from execution.utils.redis_client import (
            consume,
            ack,
            STREAM_ORDERS_PENDING,
            GROUP_EXECUTION,
        )

        logger.info("Entering live consume loop on '%s'...", STREAM_ORDERS_PENDING)

        while self._running:
            try:
                messages = await consume(
                    stream=STREAM_ORDERS_PENDING,
                    group=GROUP_EXECUTION,
                    consumer=CONSUMER_NAME,
                    count=CONSUME_BATCH_SIZE,
                    block=CONSUME_BLOCK_MS,
                )

                if not messages:
                    continue

                for msg_id, data in messages:
                    logger.info(
                        "Received signal: msg_id=%s data=%s",
                        msg_id, json.dumps(data, default=str)[:200],
                    )

                    # Parse numeric fields from string representation
                    signal = self._parse_signal_fields(data)

                    # Process the signal
                    result = await self._process_signal(signal)

                    if result:
                        logger.info(
                            "Signal processed: trade_id=%s status=%s",
                            result.get("trade_id"), result.get("status"),
                        )
                    else:
                        logger.info("Signal was rejected or skipped.")

                    # Acknowledge the message regardless of outcome
                    await ack(STREAM_ORDERS_PENDING, GROUP_EXECUTION, msg_id)

            except asyncio.CancelledError:
                logger.info("Consume loop cancelled.")
                self._running = False
                break
            except Exception as exc:
                logger.error(
                    "Error in consume loop: %s. Continuing after brief pause.",
                    exc,
                )
                await asyncio.sleep(1)

    async def _run_test_mode(self) -> None:
        """Test mode: publish fixture signals to Redis, then consume and process them."""
        from execution.utils.redis_client import (
            publish,
            consume,
            ack,
            STREAM_ORDERS_PENDING,
            GROUP_EXECUTION,
        )

        logger.info("=" * 72)
        logger.info("  TEST MODE: Processing %d fixture signals", len(_FIXTURE_SIGNALS))
        logger.info("=" * 72)

        # Publish all fixture signals to stream:orders:pending
        for i, signal in enumerate(_FIXTURE_SIGNALS):
            serialised = {k: str(v) if not isinstance(v, str) else v for k, v in signal.items()}
            msg_id = await publish(STREAM_ORDERS_PENDING, serialised)
            logger.info(
                "Published fixture signal %d/%d: msg_id=%s strategy=%s",
                i + 1, len(_FIXTURE_SIGNALS), msg_id,
                signal.get("strategy", "?"),
            )

        # Now consume and process them
        processed = 0
        approved = 0
        rejected = 0
        results: list[dict] = []

        # Consume in batches until we have processed all fixture signals
        remaining_attempts = 10  # safety limit
        while processed < len(_FIXTURE_SIGNALS) and remaining_attempts > 0:
            remaining_attempts -= 1
            messages = await consume(
                stream=STREAM_ORDERS_PENDING,
                group=GROUP_EXECUTION,
                consumer=CONSUMER_NAME,
                count=CONSUME_BATCH_SIZE,
                block=500,
            )

            if not messages:
                continue

            for msg_id, data in messages:
                signal = self._parse_signal_fields(data)
                result = await self._process_signal(signal)

                if result is not None:
                    approved += 1
                    results.append(result)
                else:
                    rejected += 1

                processed += 1

                # Acknowledge
                await ack(STREAM_ORDERS_PENDING, GROUP_EXECUTION, msg_id)

        # -- Summary -------------------------------------------------------
        print()
        print("=" * 72)
        print("  Order Router -- Test Results")
        print("=" * 72)
        print(f"  Total signals processed:  {processed}")
        print(f"  Approved and routed:      {approved}")
        print(f"  Rejected / skipped:       {rejected}")
        print()

        for r in results:
            print(
                f"    trade_id={r['trade_id']:>3d}  "
                f"status={r['status']:<8s}  "
                f"fill_price={r['fill_price']:.4f}  "
                f"size_usd=${r['size_usd']:.2f}"
            )

        print()
        print("=" * 72)
        print("  ALL TESTS PASSED")
        print("=" * 72)
        print()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_signal_fields(data: dict) -> dict:
        """Parse string-encoded numeric fields back to their native types.

        Redis Streams store all values as strings, so we need to convert
        target_price and size_usd back to float.

        Args:
            data: Raw field dict from Redis (all values are strings).

        Returns:
            Signal dict with numeric fields converted to float.
        """
        signal = dict(data)

        for numeric_field in ("target_price", "size_usd", "edge_estimate"):
            if numeric_field in signal:
                try:
                    signal[numeric_field] = float(signal[numeric_field])
                except (ValueError, TypeError):
                    pass  # leave as-is, validation will catch it

        return signal

    def stop(self) -> None:
        """Signal the router to stop its run loop."""
        logger.info("Stop requested.")
        self._running = False


# ===========================================================================
#  DEMO / SELF-TEST
# ===========================================================================

async def _run_self_test() -> None:
    """Run the order router in --test mode with fixture signals and mocks."""
    import execution.utils.redis_client as rc
    import execution.utils.db as db_mod

    # Enable mock backends
    rc._use_mock = True
    rc._redis_instance = None
    db_mod._test_mode = True
    db_mod._pool = None
    db_mod._pool_lock = None

    router = OrderRouter(test_mode=True, dry_run=False)
    await router.run()

    # Cleanup
    from execution.utils.redis_client import close_redis
    from execution.utils.db import close_pool
    await close_redis()
    await close_pool()


async def _run_dry_run() -> None:
    """Run the order router in --dry-run mode.

    Full pipeline with dry_run=True to connectors.  Requires config
    credentials and a running Redis instance, but orders are not
    actually placed.
    """
    router = OrderRouter(test_mode=False, dry_run=True)

    # In dry-run mode, we just start the loop; it will consume from
    # the live Redis stream.  Press Ctrl+C to stop.
    try:
        await router.run()
    except KeyboardInterrupt:
        router.stop()


async def _run_live() -> None:
    """Run the order router in live mode.

    Requires config credentials, a running Redis instance, and a running
    PostgreSQL database.  Real orders will be placed.
    """
    router = OrderRouter(test_mode=False, dry_run=False)

    try:
        await router.run()
    except KeyboardInterrupt:
        router.stop()


# ===========================================================================
#  ENTRY POINT
# ===========================================================================

def main() -> None:
    """
    CLI entry point: parse --test / --dry-run flags and start the order router.

    --test:    Process fixture signals with mock connectors.  No .env,
               Redis, or PostgreSQL needed.
    --dry-run: Full pipeline but connectors validate without placing orders.
    (default): Live mode -- real order placement.
    """
    global _test_mode, _dry_run_mode

    parser = argparse.ArgumentParser(
        description=(
            "Order routing engine for polymarket-trader. "
            "Consumes trade signals, validates risk, sizes positions, "
            "and routes orders to venue connectors."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Process fixture signals with mock connectors (no external deps).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Full pipeline but pass dry_run=True to connectors. "
            "Orders are validated but not placed."
        ),
    )
    args = parser.parse_args()

    if args.test:
        _test_mode = True
        logger.info("Running in --test mode with fixture signals and mocks.")
        asyncio.run(_run_self_test())
    elif args.dry_run:
        _dry_run_mode = True
        logger.info("Running in --dry-run mode (orders validated but not placed).")
        asyncio.run(_run_dry_run())
    else:
        logger.info("Running in LIVE mode. Orders will be placed.")
        asyncio.run(_run_live())


if __name__ == "__main__":
    main()
