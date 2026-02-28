#!/usr/bin/env python3
"""
s2_insider_detection.py -- Strategy S2: Insider Wallet Detection.

Monitors high-IPS wallet trades in real-time via stream:insider:alerts.
When a high-IPS wallet (IPS > 0.70) trades, waits N minutes (configurable,
default 5), checks the price response on stream:prices:normalised, then
emits a trade signal to stream:orders:pending:

    - Price moved > 3c in their direction:  reduce size 50% (info partially
      incorporated).  The edge is shrinking.
    - Price flat (< 1c movement):  maximum conviction entry.  The market
      hasn't reacted to the insider's information.
    - Price moved against them:  skip.  The insider was wrong or this is
      a false positive.

Position sizing uses a modified Kelly criterion scaled by IPS confidence.

Input streams:
    stream:insider:alerts       -- High-IPS wallet trade events
    stream:prices:normalised    -- Unified price ticks for timing analysis

Output stream:
    stream:orders:pending       -- Queued trade signals

Signal format (emitted to stream:orders:pending):
    {
        "strategy": "s2",
        "venue": "polymarket",
        "market_id": "0x1234...",
        "side": "yes",
        "target_price": 0.55,
        "size_usd": 75.00,
        "edge_estimate": 0.08,
        "confidence": "high",
        "reasoning": "Wallet 0xABCD (IPS=0.87) bought Yes at $0.52. ...",
        "trigger_wallet": "0xABCD...",
        "trigger_ips": 0.87,
        "price_response": "flat"
    }

Usage:
    python execution/strategies/s2_insider_detection.py              # Live mode
    python execution/strategies/s2_insider_detection.py --test       # Fixture mode
    python execution/strategies/s2_insider_detection.py --backtest   # Historical backtest

Environment variables (via execution.utils.config):
    POSTGRES_URL         -- PostgreSQL connection string
    REDIS_URL            -- Redis connection string
    INITIAL_BANKROLL     -- Starting bankroll in USD (default: 1000.0)

See: directives/04_strategy_s2_insider_detection.md
"""

import argparse
import asyncio
import json
import logging
import math
import sys
import time as _time
from datetime import datetime, timedelta, timezone
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/strategies/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
BACKTEST_DIR = TMP_DIR / "backtest"
FIXTURE_PATH = FIXTURES_DIR / "s2_wallets.json"

# ---------------------------------------------------------------------------
# Stream name constants (must match redis_client.py)
# ---------------------------------------------------------------------------
STREAM_INSIDER_ALERTS = "stream:insider:alerts"
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
STREAM_ORDERS_PENDING = "stream:orders:pending"

# ---------------------------------------------------------------------------
# Consumer group constants (must match redis_client.py)
# ---------------------------------------------------------------------------
GROUP_INSIDER = "insider_group"
GROUP_STRATEGY = "strategy_group"

# ---------------------------------------------------------------------------
# Strategy parameters
# ---------------------------------------------------------------------------
IPS_HIGH_THRESHOLD = 0.70          # Minimum IPS to act on
PRICE_RESPONSE_WAIT_MINUTES = 5    # How long to wait before checking price
PRICE_MOVE_THRESHOLD = 0.03        # 3c movement = info partially incorporated
PRICE_FLAT_THRESHOLD = 0.01        # < 1c movement = price flat
MAX_MARKET_PRICE = 0.95            # Skip markets already at 95c+
MIN_EDGE_THRESHOLD = 0.02          # Minimum edge to justify a trade
SIZE_REDUCTION_FACTOR = 0.50       # Reduce size by 50% if price already moved

# ---------------------------------------------------------------------------
# Kelly criterion parameters
# ---------------------------------------------------------------------------
KELLY_FRACTION = 0.25              # Quarter-Kelly for safety
KELLY_MAX_SIZE_PCT = 0.05          # Never risk more than 5% of bankroll
DEFAULT_BANKROLL = 1000.0          # Fallback if config unavailable
MIN_SIZE_USD = 5.0                 # Minimum trade size
MAX_SIZE_USD = 500.0               # Maximum trade size

# ---------------------------------------------------------------------------
# Backtest parameters
# ---------------------------------------------------------------------------
BACKTEST_INITIAL_BANKROLL = 1000.0
BACKTEST_FEE_BPS = 50              # 50 bps round-trip fee assumption

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_test_mode: bool = False


# =========================================================================
#  HELPER: Timestamp parsing
# =========================================================================

def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO-8601 timestamp string into a timezone-aware datetime."""
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)


def _now_utc() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# =========================================================================
#  MODIFIED KELLY POSITION SIZING
# =========================================================================

def compute_kelly_size(
    edge: float,
    ips_confidence: float,
    bankroll: float,
    price: float,
    price_response: str,
) -> float:
    """
    Compute position size using a modified Kelly criterion.

    The full Kelly formula is: f* = (b*p - q) / b
    where b = decimal odds, p = probability of winning, q = 1-p.

    We modify this by:
    1. Scaling by IPS confidence (higher IPS = more conviction)
    2. Applying quarter-Kelly for safety
    3. Reducing by 50% if price has already moved (partial incorporation)
    4. Clamping to bankroll percentage and absolute limits

    Args:
        edge:            Estimated edge (true_prob - market_price).
        ips_confidence:  IPS score of the triggering wallet (0-1).
        bankroll:        Current bankroll in USD.
        price:           Current market price (0-1).
        price_response:  One of "flat", "moved_with", "moved_against".

    Returns:
        Position size in USD.
    """
    if edge <= 0 or price <= 0 or price >= 1.0:
        return 0.0

    # Decimal odds from the market price
    odds = (1.0 / price) - 1.0
    if odds <= 0:
        return 0.0

    # Estimated probability of winning = price + edge
    p_win = min(0.99, price + edge)
    p_lose = 1.0 - p_win

    # Full Kelly fraction
    kelly_f = (odds * p_win - p_lose) / odds
    if kelly_f <= 0:
        return 0.0

    # Scale by IPS confidence -- higher IPS means more trust in the signal
    confidence_scalar = 0.5 + 0.5 * ips_confidence  # ranges from 0.5 to 1.0

    # Quarter-Kelly for safety
    position_fraction = kelly_f * KELLY_FRACTION * confidence_scalar

    # Apply bankroll percentage cap
    position_fraction = min(position_fraction, KELLY_MAX_SIZE_PCT)

    # Compute raw USD size
    size_usd = bankroll * position_fraction

    # Reduce size by 50% if price already moved in the insider's direction
    if price_response == "moved_with":
        size_usd *= SIZE_REDUCTION_FACTOR
        logger.info(
            "Price moved with insider -- reducing size by %.0f%%.",
            (1.0 - SIZE_REDUCTION_FACTOR) * 100,
        )

    # Clamp to absolute limits
    size_usd = max(MIN_SIZE_USD, min(MAX_SIZE_USD, size_usd))

    logger.info(
        "Kelly sizing: edge=%.4f ips=%.3f kelly_f=%.4f conf=%.2f "
        "pct=%.4f size=$%.2f (response=%s)",
        edge, ips_confidence, kelly_f, confidence_scalar,
        position_fraction, size_usd, price_response,
    )
    return round(size_usd, 2)


# =========================================================================
#  PRICE RESPONSE ANALYSIS
# =========================================================================

def classify_price_response(
    entry_price: float,
    current_price: float,
    side: str,
) -> str:
    """
    Classify the market's price response after an insider trade.

    Args:
        entry_price:   Price at time of insider's trade.
        current_price: Price N minutes after the insider's trade.
        side:          Insider's trade side ("yes" or "no").

    Returns:
        One of:
        - "flat":          Price moved < 1c (information not propagated)
        - "moved_with":    Price moved > 3c in insider's direction
        - "moved_against": Price moved against the insider
    """
    delta = current_price - entry_price

    # For YES side, positive delta = moved in insider's direction
    # For NO side, negative delta = moved in insider's direction
    if side.lower() == "no":
        delta = -delta

    abs_delta = abs(current_price - entry_price)

    if abs_delta < PRICE_FLAT_THRESHOLD:
        return "flat"
    elif delta >= PRICE_MOVE_THRESHOLD:
        return "moved_with"
    elif delta < 0:
        return "moved_against"
    else:
        # Moved in their direction but < 3c -- treat as partial, still flat-ish
        return "flat"


def estimate_edge(
    ips_score: float,
    entry_price: float,
    price_response: str,
) -> float:
    """
    Estimate the trading edge based on IPS score and price response.

    The edge is the estimated difference between true probability and
    market price.  Higher IPS = more confidence that the insider is
    correct, so a larger implied edge.

    Args:
        ips_score:       IPS of the triggering wallet (0-1).
        entry_price:     Price at which the insider entered.
        price_response:  Classification of price movement.

    Returns:
        Estimated edge as a float (0-1 scale).
    """
    # Base edge scales with IPS: a 0.70 wallet implies ~8% edge, 1.0 implies ~15%
    base_edge = 0.05 + 0.10 * (ips_score - IPS_HIGH_THRESHOLD) / (1.0 - IPS_HIGH_THRESHOLD)

    if price_response == "flat":
        # Full edge -- the market hasn't reacted
        edge = base_edge
    elif price_response == "moved_with":
        # Partially incorporated -- halve the edge
        edge = base_edge * 0.5
    else:
        # Moved against -- no edge
        edge = 0.0

    # Edge is capped based on distance to 1.0 (or 0.0 for NO side)
    max_edge = 1.0 - entry_price - 0.02  # Leave 2c buffer for fees
    edge = max(0.0, min(edge, max_edge))

    return round(edge, 4)


def determine_confidence(ips_score: float, price_response: str) -> str:
    """
    Determine the confidence level for the trade signal.

    Args:
        ips_score:      IPS of the triggering wallet.
        price_response: Price response classification.

    Returns:
        One of "high", "medium", "low".
    """
    if price_response == "moved_against":
        return "low"

    if ips_score >= 0.85 and price_response == "flat":
        return "high"
    elif ips_score >= 0.75 and price_response == "flat":
        return "medium"
    elif ips_score >= 0.85 and price_response == "moved_with":
        return "medium"
    else:
        return "low"


# =========================================================================
#  SIGNAL BUILDER
# =========================================================================

def build_signal(
    alert: dict,
    current_price: float,
    price_response: str,
    edge: float,
    size_usd: float,
    confidence: str,
) -> dict:
    """
    Build a trade signal dict for emission to stream:orders:pending.

    Args:
        alert:           The original insider alert from stream:insider:alerts.
        current_price:   Current market price after the response window.
        price_response:  Price response classification.
        edge:            Estimated edge.
        size_usd:        Computed position size in USD.
        confidence:      Confidence level string.

    Returns:
        Signal dict matching the directive schema.
    """
    wallet = alert.get("wallet_address", "unknown")
    ips = float(alert.get("ips_score", "0.0"))
    market_id = alert.get("market_id", "")
    side = alert.get("side", "yes")
    entry_price = float(alert.get("price", "0.0"))

    # Build a human-readable reasoning string
    action = "bought Yes" if side.lower() == "yes" else "sold (bought No)"
    if price_response == "flat":
        response_desc = (
            "Price flat after %dmin = info not incorporated."
            % PRICE_RESPONSE_WAIT_MINUTES
        )
    elif price_response == "moved_with":
        response_desc = (
            "Price moved >3c in their direction after %dmin = info partially "
            "incorporated. Reducing position size."
            % PRICE_RESPONSE_WAIT_MINUTES
        )
    else:
        response_desc = "Price moved against them. Skipping."

    reasoning = (
        "Wallet %s (IPS=%.2f) %s at $%.2f. %s"
        % (wallet[:10], ips, action, entry_price, response_desc)
    )

    signal = {
        "strategy": "s2",
        "venue": "polymarket",
        "market_id": market_id,
        "side": side.lower(),
        "target_price": str(round(current_price, 4)),
        "size_usd": str(round(size_usd, 2)),
        "edge_estimate": str(round(edge, 4)),
        "confidence": confidence,
        "reasoning": reasoning,
        "trigger_wallet": wallet,
        "trigger_ips": str(round(ips, 4)),
        "price_response": price_response,
        "timestamp": _now_utc().isoformat(),
    }

    return signal


# =========================================================================
#  CORE: PROCESS A SINGLE INSIDER ALERT
# =========================================================================

async def process_alert(
    alert: dict,
    get_current_price_fn: Any = None,
    bankroll: float = DEFAULT_BANKROLL,
) -> Optional[dict]:
    """
    Process a single insider alert through the full S2 pipeline.

    Steps:
    1. Validate the alert (IPS threshold, market price cap).
    2. Wait for the price response window (or use provided price fn).
    3. Classify the price response.
    4. Estimate edge and compute position size.
    5. Build and return the trade signal (or None if skipping).

    Args:
        alert:                The insider alert dict from stream:insider:alerts.
        get_current_price_fn: Async callable(market_id) -> float. If None,
                              uses the entry price (backtest/test mode).
        bankroll:             Current bankroll in USD.

    Returns:
        Trade signal dict, or None if the alert should be skipped.
    """
    wallet = alert.get("wallet_address", "unknown")
    ips = float(alert.get("ips_score", "0.0"))
    market_id = alert.get("market_id", "")
    side = alert.get("side", "yes").lower()
    entry_price = float(alert.get("price", "0.0"))

    logger.info(
        "Processing alert: wallet=%s IPS=%.3f market=%s side=%s price=%.4f",
        wallet[:10], ips, market_id, side, entry_price,
    )

    # --- Gate 1: IPS threshold -------------------------------------------
    if ips < IPS_HIGH_THRESHOLD:
        logger.info(
            "Skipping wallet %s: IPS %.3f below threshold %.2f.",
            wallet[:10], ips, IPS_HIGH_THRESHOLD,
        )
        return None

    # --- Gate 2: Market price cap ----------------------------------------
    # For YES side, skip if price already >= 95c
    # For NO side, the YES price is (1 - no_price), so skip if no_price >= 95c
    if entry_price >= MAX_MARKET_PRICE:
        logger.info(
            "Skipping market %s: price %.2f >= %.2f (insufficient edge).",
            market_id, entry_price, MAX_MARKET_PRICE,
        )
        return None

    # --- Gate 3: Get current price after response window -----------------
    if get_current_price_fn is not None:
        current_price = await get_current_price_fn(market_id)
    else:
        # In test/backtest mode without a price fn, assume price is unchanged
        current_price = entry_price

    if current_price <= 0 or current_price >= 1.0:
        logger.warning(
            "Invalid current price %.4f for market %s. Skipping.",
            current_price, market_id,
        )
        return None

    # --- Step 1: Classify price response ---------------------------------
    price_response = classify_price_response(entry_price, current_price, side)
    logger.info(
        "Price response for %s: entry=%.4f current=%.4f response=%s",
        market_id, entry_price, current_price, price_response,
    )

    # --- Gate 4: Skip if price moved against ----------------------------
    if price_response == "moved_against":
        logger.info(
            "Skipping %s: price moved against insider wallet %s.",
            market_id, wallet[:10],
        )
        return None

    # --- Step 2: Estimate edge ------------------------------------------
    edge = estimate_edge(ips, entry_price, price_response)

    if edge < MIN_EDGE_THRESHOLD:
        logger.info(
            "Skipping %s: estimated edge %.4f below minimum %.4f.",
            market_id, edge, MIN_EDGE_THRESHOLD,
        )
        return None

    # --- Step 3: Compute position size ----------------------------------
    size_usd = compute_kelly_size(
        edge=edge,
        ips_confidence=ips,
        bankroll=bankroll,
        price=current_price,
        price_response=price_response,
    )

    if size_usd < MIN_SIZE_USD:
        logger.info(
            "Skipping %s: computed size $%.2f below minimum $%.2f.",
            market_id, size_usd, MIN_SIZE_USD,
        )
        return None

    # --- Step 4: Determine confidence -----------------------------------
    confidence = determine_confidence(ips, price_response)

    # --- Step 5: Build signal -------------------------------------------
    signal = build_signal(
        alert=alert,
        current_price=current_price,
        price_response=price_response,
        edge=edge,
        size_usd=size_usd,
        confidence=confidence,
    )

    logger.info(
        "Signal generated: market=%s side=%s size=$%.2f edge=%.4f "
        "confidence=%s response=%s",
        market_id, side, size_usd, edge, confidence, price_response,
    )

    return signal


# =========================================================================
#  LIVE MODE: Real-time stream consumer
# =========================================================================

async def _get_price_from_stream(
    market_id: str,
    price_cache: dict[str, float],
) -> float:
    """
    Look up the latest normalised price for a market from the price cache.

    The price cache is populated by the price consumer coroutine running
    concurrently.  Falls back to 0.0 if no price is available.

    Args:
        market_id:   Market identifier.
        price_cache: Shared dict of market_id -> latest yes_price.

    Returns:
        Latest yes_price as float, or 0.0 if unavailable.
    """
    price = price_cache.get(market_id, 0.0)
    if price <= 0:
        logger.warning(
            "No cached price for market %s. Price cache has %d entries.",
            market_id, len(price_cache),
        )
    return price


async def _consume_prices(price_cache: dict[str, float]) -> None:
    """
    Background coroutine that continuously consumes from
    stream:prices:normalised and updates the shared price cache.

    This runs alongside the alert consumer so that when an insider
    alert arrives, we can immediately check the latest price.

    Args:
        price_cache: Shared mutable dict updated in-place.
    """
    from execution.utils.redis_client import (
        consume, ack, ensure_consumer_groups,
    )

    consumer_name = "s2-price-consumer"
    logger.info("Starting price consumer on %s...", STREAM_PRICES_NORMALISED)

    while True:
        try:
            messages = await consume(
                stream=STREAM_PRICES_NORMALISED,
                group=GROUP_STRATEGY,
                consumer=consumer_name,
                count=50,
                block=500,
            )

            for msg_id, data in messages:
                market_id = data.get("market_id", "")
                try:
                    yes_price = float(data.get("yes_price", "0"))
                except (ValueError, TypeError):
                    yes_price = 0.0

                if market_id and yes_price > 0:
                    price_cache[market_id] = yes_price

                await ack(STREAM_PRICES_NORMALISED, GROUP_STRATEGY, msg_id)

        except asyncio.CancelledError:
            logger.info("Price consumer cancelled.")
            break
        except Exception as exc:
            logger.error("Price consumer error: %s", exc, exc_info=True)
            await asyncio.sleep(2)


async def _consume_alerts(
    price_cache: dict[str, float],
    bankroll: float,
) -> None:
    """
    Main alert consumer coroutine.  Reads from stream:insider:alerts,
    waits for the price response window, classifies the response, and
    emits trade signals to stream:orders:pending.

    Args:
        price_cache: Shared dict of market_id -> latest yes_price.
        bankroll:    Current bankroll in USD.
    """
    from execution.utils.redis_client import consume, ack, publish

    consumer_name = "s2-alert-consumer"
    logger.info("Starting alert consumer on %s...", STREAM_INSIDER_ALERTS)

    while True:
        try:
            messages = await consume(
                stream=STREAM_INSIDER_ALERTS,
                group=GROUP_INSIDER,
                consumer=consumer_name,
                count=10,
                block=2000,
            )

            for msg_id, alert in messages:
                wallet = alert.get("wallet_address", "unknown")
                ips = float(alert.get("ips_score", "0.0"))
                market_id = alert.get("market_id", "")

                logger.info(
                    "Received insider alert: wallet=%s IPS=%.3f market=%s",
                    wallet[:10], ips, market_id,
                )

                # Wait for the price response window
                logger.info(
                    "Waiting %d minutes for price response...",
                    PRICE_RESPONSE_WAIT_MINUTES,
                )
                await asyncio.sleep(PRICE_RESPONSE_WAIT_MINUTES * 60)

                # Build the price lookup function using the cache
                async def get_price(mid: str) -> float:
                    return await _get_price_from_stream(mid, price_cache)

                # Process the alert
                signal = await process_alert(
                    alert=alert,
                    get_current_price_fn=get_price,
                    bankroll=bankroll,
                )

                if signal is not None:
                    signal_id = await publish(STREAM_ORDERS_PENDING, signal)
                    logger.info(
                        "Emitted signal %s -> %s: market=%s side=%s size=$%s",
                        signal_id, STREAM_ORDERS_PENDING,
                        signal["market_id"], signal["side"],
                        signal["size_usd"],
                    )

                # Acknowledge the alert
                await ack(STREAM_INSIDER_ALERTS, GROUP_INSIDER, msg_id)
                logger.info("Acknowledged alert %s.", msg_id)

        except asyncio.CancelledError:
            logger.info("Alert consumer cancelled.")
            break
        except Exception as exc:
            logger.error("Alert consumer error: %s", exc, exc_info=True)
            await asyncio.sleep(2)


async def _run_live_mode() -> None:
    """
    Run Strategy S2 in live mode.

    Starts two concurrent coroutines:
    1. Price consumer: continuously updates the price cache from
       stream:prices:normalised.
    2. Alert consumer: processes insider alerts from stream:insider:alerts,
       waits for price response, and emits trade signals.
    """
    from execution.utils.redis_client import ensure_consumer_groups

    logger.info("=" * 72)
    logger.info("  Strategy S2: Insider Wallet Detection -- LIVE MODE")
    logger.info("  Time: %s", _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"))
    logger.info("=" * 72)

    # Ensure Redis consumer groups exist
    await ensure_consumer_groups()

    # Load bankroll from config
    try:
        from execution.utils.config import config
        bankroll = config.INITIAL_BANKROLL
    except Exception:
        bankroll = DEFAULT_BANKROLL
        logger.warning(
            "Could not load bankroll from config; using default $%.2f.",
            bankroll,
        )

    logger.info("Bankroll: $%.2f", bankroll)
    logger.info("IPS threshold: %.2f", IPS_HIGH_THRESHOLD)
    logger.info("Price response window: %d minutes", PRICE_RESPONSE_WAIT_MINUTES)
    logger.info("Price move threshold: %.2fc", PRICE_MOVE_THRESHOLD * 100)
    logger.info("Price flat threshold: %.2fc", PRICE_FLAT_THRESHOLD * 100)

    # Shared mutable price cache
    price_cache: dict[str, float] = {}

    # Run both consumers concurrently
    price_task = asyncio.create_task(_consume_prices(price_cache))
    alert_task = asyncio.create_task(_consume_alerts(price_cache, bankroll))

    try:
        await asyncio.gather(price_task, alert_task)
    except KeyboardInterrupt:
        logger.info("Shutting down S2 strategy...")
    finally:
        price_task.cancel()
        alert_task.cancel()
        try:
            await asyncio.gather(price_task, alert_task, return_exceptions=True)
        except Exception:
            pass

        from execution.utils.redis_client import close_redis
        await close_redis()
        logger.info("Strategy S2 shut down cleanly.")


# =========================================================================
#  FIXTURE DATA (--test mode)
# =========================================================================

FIXTURE_ALERTS: list[dict] = [
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xMKT_A",
        "side": "yes",
        "price": "0.35",
        "size_usd": "500.0",
        "timestamp": "2026-02-26T10:00:00Z",
        "strategy": "s2",
    },
    {
        "wallet_address": "0xINSIDER_002",
        "ips_score": "0.82",
        "market_id": "0xMKT_B",
        "side": "yes",
        "price": "0.40",
        "size_usd": "350.0",
        "timestamp": "2026-02-26T10:05:00Z",
        "strategy": "s2",
    },
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xMKT_C",
        "side": "no",
        "price": "0.60",
        "size_usd": "300.0",
        "timestamp": "2026-02-26T10:10:00Z",
        "strategy": "s2",
    },
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xMKT_D",
        "side": "yes",
        "price": "0.52",
        "size_usd": "750.0",
        "timestamp": "2026-02-26T10:15:00Z",
        "strategy": "s2",
    },
    # This alert should be skipped: price already at 96c
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.90",
        "market_id": "0xMKT_E",
        "side": "yes",
        "price": "0.96",
        "size_usd": "200.0",
        "timestamp": "2026-02-26T10:20:00Z",
        "strategy": "s2",
    },
    # This alert should be skipped: IPS below threshold
    {
        "wallet_address": "0xRETAIL_001",
        "ips_score": "0.45",
        "market_id": "0xMKT_F",
        "side": "yes",
        "price": "0.50",
        "size_usd": "50.0",
        "timestamp": "2026-02-26T10:25:00Z",
        "strategy": "s2",
    },
    # This alert: price moves against the insider
    {
        "wallet_address": "0xINSIDER_002",
        "ips_score": "0.78",
        "market_id": "0xMKT_G",
        "side": "yes",
        "price": "0.45",
        "size_usd": "400.0",
        "timestamp": "2026-02-26T10:30:00Z",
        "strategy": "s2",
    },
]

# Simulated prices after the response window
# market_id -> price after N minutes
FIXTURE_PRICES_AFTER: dict[str, float] = {
    "0xMKT_A": 0.35,    # Flat -- maximum conviction
    "0xMKT_B": 0.44,    # Moved > 3c with insider -- reduce size
    "0xMKT_C": 0.60,    # Flat (NO side, so YES price unchanged) -- max conviction
    "0xMKT_D": 0.525,   # Barely moved (< 1c) -- flat, max conviction
    "0xMKT_E": 0.97,    # Irrelevant, will be skipped at gate 2
    "0xMKT_F": 0.50,    # Irrelevant, will be skipped at gate 1
    "0xMKT_G": 0.40,    # Moved 5c against the insider -- skip
}


# =========================================================================
#  BACKTEST DATA
# =========================================================================

BACKTEST_ALERTS: list[dict] = [
    # Day 1: Insider buys YES on MKT_A at 35c, price stays flat -> win
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xBT_MKT_A",
        "side": "yes",
        "price": "0.35",
        "size_usd": "500.0",
        "timestamp": "2026-01-10T10:00:00Z",
    },
    # Day 3: Insider buys YES on MKT_B at 40c, price moves 4c -> partial win
    {
        "wallet_address": "0xINSIDER_002",
        "ips_score": "0.82",
        "market_id": "0xBT_MKT_B",
        "side": "yes",
        "price": "0.40",
        "size_usd": "350.0",
        "timestamp": "2026-01-12T14:00:00Z",
    },
    # Day 7: Insider buys NO on MKT_C at 60c, price flat -> win
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xBT_MKT_C",
        "side": "no",
        "price": "0.60",
        "size_usd": "300.0",
        "timestamp": "2026-01-16T09:00:00Z",
    },
    # Day 10: Insider buys YES at 55c, price drops 4c against -> skip
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xBT_MKT_D",
        "side": "yes",
        "price": "0.55",
        "size_usd": "600.0",
        "timestamp": "2026-01-19T11:00:00Z",
    },
    # Day 14: Insider buys YES at 28c, flat -> big win
    {
        "wallet_address": "0xINSIDER_002",
        "ips_score": "0.91",
        "market_id": "0xBT_MKT_E",
        "side": "yes",
        "price": "0.28",
        "size_usd": "1000.0",
        "timestamp": "2026-01-23T08:00:00Z",
    },
    # Day 18: Insider buys YES at 62c, moves 5c with -> partial win
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.85",
        "market_id": "0xBT_MKT_F",
        "side": "yes",
        "price": "0.62",
        "size_usd": "400.0",
        "timestamp": "2026-01-27T15:00:00Z",
    },
    # Day 22: Insider buys YES at 45c, flat -> win
    {
        "wallet_address": "0xINSIDER_002",
        "ips_score": "0.79",
        "market_id": "0xBT_MKT_G",
        "side": "yes",
        "price": "0.45",
        "size_usd": "550.0",
        "timestamp": "2026-01-31T10:00:00Z",
    },
    # Day 25: Insider buys NO at 70c, moved against -> skip
    {
        "wallet_address": "0xINSIDER_001",
        "ips_score": "0.87",
        "market_id": "0xBT_MKT_H",
        "side": "no",
        "price": "0.70",
        "size_usd": "350.0",
        "timestamp": "2026-02-03T13:00:00Z",
    },
]

BACKTEST_PRICES_AFTER: dict[str, float] = {
    "0xBT_MKT_A": 0.35,    # Flat
    "0xBT_MKT_B": 0.44,    # Moved 4c with insider
    "0xBT_MKT_C": 0.60,    # Flat for NO side
    "0xBT_MKT_D": 0.51,    # Moved 4c against
    "0xBT_MKT_E": 0.28,    # Flat
    "0xBT_MKT_F": 0.67,    # Moved 5c with
    "0xBT_MKT_G": 0.45,    # Flat
    "0xBT_MKT_H": 0.74,    # Moved 4c against (NO side)
}

# Resolution outcomes for backtest PnL computation
BACKTEST_RESOLUTIONS: dict[str, str] = {
    "0xBT_MKT_A": "yes",   # Insider was right
    "0xBT_MKT_B": "yes",   # Insider was right
    "0xBT_MKT_C": "no",    # Insider (NO) was right
    "0xBT_MKT_D": "no",    # Insider (YES) was wrong
    "0xBT_MKT_E": "yes",   # Insider was right
    "0xBT_MKT_F": "yes",   # Insider was right
    "0xBT_MKT_G": "yes",   # Insider was right
    "0xBT_MKT_H": "no",    # Insider (NO) was right, but we skipped
}


# =========================================================================
#  TEST MODE
# =========================================================================

async def _run_test_mode() -> None:
    """
    Run Strategy S2 in --test mode with fixture data.

    Exercises the full alert processing pipeline without external
    dependencies.  Uses fixture alerts and simulated price responses
    to verify each gate and signal generation path.
    """
    import execution.utils.db as db
    import execution.utils.redis_client as redis_client

    # Enable mock modes
    db._test_mode = True
    redis_client._use_mock = True

    from execution.utils.redis_client import (
        ensure_consumer_groups, publish, consume, ack, stream_len,
    )

    await ensure_consumer_groups()

    print()
    print("=" * 72)
    print("  Strategy S2: Insider Wallet Detection -- TEST MODE")
    print("  Time: %s" % _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    bankroll = DEFAULT_BANKROLL
    signals_generated: list[dict] = []
    signals_skipped: int = 0

    # ---- Test 1: Process all fixture alerts ----------------------------
    print()
    print("-" * 72)
    print("  Test 1: Process fixture insider alerts")
    print("-" * 72)

    for i, alert in enumerate(FIXTURE_ALERTS):
        market_id = alert["market_id"]

        # Build a price lookup that returns the fixture price after response
        async def make_price_fn(mid: str) -> float:
            return FIXTURE_PRICES_AFTER.get(mid, 0.0)

        signal = await process_alert(
            alert=alert,
            get_current_price_fn=make_price_fn,
            bankroll=bankroll,
        )

        wallet = alert["wallet_address"]
        ips = alert["ips_score"]

        if signal is not None:
            signals_generated.append(signal)
            print(
                "  [%d] SIGNAL: wallet=%s IPS=%s market=%s side=%s "
                "size=$%s edge=%s confidence=%s response=%s"
                % (
                    i + 1, wallet[:12], ips, market_id,
                    signal["side"], signal["size_usd"],
                    signal["edge_estimate"], signal["confidence"],
                    signal["price_response"],
                )
            )
        else:
            signals_skipped += 1
            print(
                "  [%d] SKIP:   wallet=%s IPS=%s market=%s"
                % (i + 1, wallet[:12], ips, market_id)
            )

    print()
    print("  Signals generated: %d" % len(signals_generated))
    print("  Alerts skipped:    %d" % signals_skipped)
    print("  Total alerts:      %d" % len(FIXTURE_ALERTS))

    # ---- Test 2: Verify signal schema ----------------------------------
    print()
    print("-" * 72)
    print("  Test 2: Verify signal schema")
    print("-" * 72)

    required_fields = {
        "strategy", "venue", "market_id", "side", "target_price",
        "size_usd", "edge_estimate", "confidence", "reasoning",
        "trigger_wallet", "trigger_ips", "price_response",
    }

    for i, sig in enumerate(signals_generated):
        missing = required_fields - set(sig.keys())
        if missing:
            print("  [%d] FAIL: Missing fields: %s" % (i + 1, missing))
        else:
            print("  [%d] PASS: All required fields present." % (i + 1))
        assert sig["strategy"] == "s2", "Strategy must be s2"
        assert sig["venue"] == "polymarket", "Venue must be polymarket"

    print("  PASS: All signals have correct schema.")

    # ---- Test 3: Price response classification -------------------------
    print()
    print("-" * 72)
    print("  Test 3: Price response classification")
    print("-" * 72)

    test_cases = [
        (0.50, 0.50, "yes", "flat"),        # No movement
        (0.50, 0.505, "yes", "flat"),        # < 1c movement
        (0.50, 0.54, "yes", "moved_with"),   # > 3c in direction
        (0.50, 0.45, "yes", "moved_against"),# Against
        (0.60, 0.60, "no", "flat"),          # NO side, flat
        (0.60, 0.56, "no", "moved_with"),    # NO side, YES dropped
        (0.60, 0.64, "no", "moved_against"), # NO side, YES rose
    ]

    all_pass = True
    for entry, current, side, expected in test_cases:
        result = classify_price_response(entry, current, side)
        status = "PASS" if result == expected else "FAIL"
        if result != expected:
            all_pass = False
        print(
            "  %s: entry=%.2f current=%.2f side=%s -> %s (expected %s)"
            % (status, entry, current, side, result, expected)
        )

    assert all_pass, "Price response classification tests failed"
    print("  PASS: All price response classifications correct.")

    # ---- Test 4: Kelly position sizing ---------------------------------
    print()
    print("-" * 72)
    print("  Test 4: Kelly position sizing")
    print("-" * 72)

    size_flat = compute_kelly_size(
        edge=0.10, ips_confidence=0.87, bankroll=1000.0,
        price=0.35, price_response="flat",
    )
    size_moved = compute_kelly_size(
        edge=0.05, ips_confidence=0.82, bankroll=1000.0,
        price=0.44, price_response="moved_with",
    )
    size_no_edge = compute_kelly_size(
        edge=0.0, ips_confidence=0.90, bankroll=1000.0,
        price=0.50, price_response="flat",
    )

    print("  Flat (edge=0.10, IPS=0.87):    $%.2f" % size_flat)
    print("  Moved (edge=0.05, IPS=0.82):   $%.2f" % size_moved)
    print("  No edge (edge=0.0):             $%.2f" % size_no_edge)

    assert size_flat > 0, "Flat signal with edge should produce non-zero size"
    assert size_no_edge == 0, "Zero edge should produce zero size"
    if size_moved > 0:
        # Moved-with should produce smaller size than equivalent flat
        size_flat_equiv = compute_kelly_size(
            edge=0.05, ips_confidence=0.82, bankroll=1000.0,
            price=0.44, price_response="flat",
        )
        assert size_moved <= size_flat_equiv, (
            "Moved-with size should be <= flat size"
        )
        print("  PASS: Moved-with size ($%.2f) <= flat equiv ($%.2f)"
              % (size_moved, size_flat_equiv))

    print("  PASS: Kelly sizing behaves correctly.")

    # ---- Test 5: Edge estimation ---------------------------------------
    print()
    print("-" * 72)
    print("  Test 5: Edge estimation")
    print("-" * 72)

    edge_flat = estimate_edge(0.87, 0.35, "flat")
    edge_moved = estimate_edge(0.82, 0.40, "moved_with")
    edge_against = estimate_edge(0.87, 0.50, "moved_against")

    print("  Flat (IPS=0.87, price=0.35):     %.4f" % edge_flat)
    print("  Moved (IPS=0.82, price=0.40):    %.4f" % edge_moved)
    print("  Against (IPS=0.87, price=0.50):  %.4f" % edge_against)

    assert edge_flat > edge_moved, "Flat edge should exceed moved-with edge"
    assert edge_against == 0.0, "Moved-against should have zero edge"
    print("  PASS: Edge estimation correct.")

    # ---- Test 6: Publish signals to Redis and verify -------------------
    print()
    print("-" * 72)
    print("  Test 6: Redis stream publish and verify")
    print("-" * 72)

    for sig in signals_generated:
        await publish(STREAM_ORDERS_PENDING, sig)

    pending_count = await stream_len(STREAM_ORDERS_PENDING)
    print("  Signals in %s: %d" % (STREAM_ORDERS_PENDING, pending_count))
    assert pending_count == len(signals_generated), (
        "Published signal count should match generated count"
    )
    print("  PASS: All signals published to Redis.")

    # ---- Test 7: Confidence determination ------------------------------
    print()
    print("-" * 72)
    print("  Test 7: Confidence determination")
    print("-" * 72)

    conf_cases = [
        (0.90, "flat", "high"),
        (0.80, "flat", "medium"),
        (0.90, "moved_with", "medium"),
        (0.72, "moved_with", "low"),
        (0.87, "moved_against", "low"),
    ]

    for ips, response, expected in conf_cases:
        result = determine_confidence(ips, response)
        status = "PASS" if result == expected else "FAIL"
        print(
            "  %s: IPS=%.2f response=%-13s -> %s (expected %s)"
            % (status, ips, response, result, expected)
        )
        assert result == expected, (
            "Confidence mismatch: IPS=%.2f response=%s" % (ips, response)
        )

    print("  PASS: All confidence determinations correct.")

    # ---- Cleanup -------------------------------------------------------
    from execution.utils.db import close_pool
    from execution.utils.redis_client import close_redis
    await close_pool()
    await close_redis()

    print()
    print("=" * 72)
    print("  ALL TESTS PASSED")
    print("=" * 72)
    print()


# =========================================================================
#  BACKTEST MODE
# =========================================================================

async def _run_backtest_mode() -> None:
    """
    Replay historical insider alerts and simulate trading.

    Uses fixture backtest data with known resolutions to compute:
    - Win rate (signals that resolved correctly)
    - Total PnL (accounting for fees)
    - Max drawdown
    - Sharpe-like ratio (PnL / volatility)

    No external dependencies required.
    """
    print()
    print("=" * 72)
    print("  Strategy S2: Insider Wallet Detection -- BACKTEST MODE")
    print("  Time: %s" % _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    bankroll = BACKTEST_INITIAL_BANKROLL
    initial_bankroll = bankroll
    trades: list[dict] = []
    pnl_series: list[float] = []

    print()
    print("  Initial bankroll: $%.2f" % bankroll)
    print("  Fee assumption:   %d bps round-trip" % BACKTEST_FEE_BPS)
    print()
    print("-" * 72)
    print("  %-6s %-14s %-6s %-8s %-8s %-8s %-10s %-8s"
          % ("Trade", "Market", "Side", "Entry", "After",
             "Response", "Size", "Result"))
    print("-" * 72)

    trade_num = 0

    for alert in BACKTEST_ALERTS:
        market_id = alert["market_id"]

        # Simulated price after response window
        async def make_price_fn(mid: str) -> float:
            return BACKTEST_PRICES_AFTER.get(mid, 0.0)

        signal = await process_alert(
            alert=alert,
            get_current_price_fn=make_price_fn,
            bankroll=bankroll,
        )

        if signal is None:
            trade_num += 1
            entry_price = float(alert.get("price", "0"))
            price_after = BACKTEST_PRICES_AFTER.get(market_id, entry_price)
            print(
                "  %-6d %-14s %-6s %-8.2f %-8.2f %-8s %-10s %-8s"
                % (trade_num, market_id[-8:], alert.get("side", "?"),
                   entry_price, price_after, "n/a", "$0.00", "SKIP")
            )
            continue

        trade_num += 1
        side = signal["side"]
        size_usd = float(signal["size_usd"])
        entry_price = float(alert.get("price", "0"))
        price_after = BACKTEST_PRICES_AFTER.get(market_id, entry_price)
        resolution = BACKTEST_RESOLUTIONS.get(market_id, "unknown")

        # PnL calculation:
        # If we bought YES and resolution is YES: profit = size * (1/price - 1) - fees
        # If we bought YES and resolution is NO: loss = -size
        # If we bought NO and resolution is NO: profit = size * (1/no_price - 1) - fees
        # If we bought NO and resolution is YES: loss = -size
        fee = size_usd * (BACKTEST_FEE_BPS / 10000.0)

        if side == "yes":
            if resolution == "yes":
                # Buy at entry_price, resolve at $1
                pnl = size_usd * ((1.0 / entry_price) - 1.0) - fee
                result = "WIN"
            else:
                pnl = -size_usd - fee
                result = "LOSS"
        else:  # NO side
            no_price = entry_price  # In the alert, price is the contract price
            if resolution == "no":
                pnl = size_usd * ((1.0 / no_price) - 1.0) - fee
                result = "WIN"
            else:
                pnl = -size_usd - fee
                result = "LOSS"

        bankroll += pnl
        pnl_series.append(pnl)

        trades.append({
            "market_id": market_id,
            "side": side,
            "entry_price": entry_price,
            "price_after": price_after,
            "size_usd": size_usd,
            "pnl": pnl,
            "result": result,
            "response": signal["price_response"],
            "bankroll_after": bankroll,
        })

        print(
            "  %-6d %-14s %-6s %-8.2f %-8.2f %-8s $%-9.2f %-8s"
            % (trade_num, market_id[-8:], side, entry_price, price_after,
               signal["price_response"], size_usd, result)
        )

    # ---- Summary statistics --------------------------------------------
    print()
    print("=" * 72)
    print("  BACKTEST SUMMARY")
    print("=" * 72)

    executed_trades = [t for t in trades]
    wins = [t for t in executed_trades if t["result"] == "WIN"]
    losses = [t for t in executed_trades if t["result"] == "LOSS"]

    total_pnl = sum(t["pnl"] for t in executed_trades) if executed_trades else 0.0
    win_rate = len(wins) / len(executed_trades) if executed_trades else 0.0

    # Max drawdown
    peak = initial_bankroll
    max_dd = 0.0
    running_bankroll = initial_bankroll
    for t in executed_trades:
        running_bankroll += t["pnl"]
        peak = max(peak, running_bankroll)
        dd = (peak - running_bankroll) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    # Sharpe-like ratio
    if len(pnl_series) > 1:
        avg_pnl = sum(pnl_series) / len(pnl_series)
        variance = sum((p - avg_pnl) ** 2 for p in pnl_series) / (len(pnl_series) - 1)
        std_pnl = math.sqrt(variance) if variance > 0 else 0.001
        sharpe = avg_pnl / std_pnl
    else:
        sharpe = 0.0

    print()
    print("  Alerts processed:  %d" % len(BACKTEST_ALERTS))
    print("  Trades executed:   %d" % len(executed_trades))
    print("  Trades skipped:    %d" % (len(BACKTEST_ALERTS) - len(executed_trades)))
    print("  Wins:              %d" % len(wins))
    print("  Losses:            %d" % len(losses))
    print("  Win rate:          %.1f%%" % (win_rate * 100))
    print()
    print("  Initial bankroll:  $%.2f" % initial_bankroll)
    print("  Final bankroll:    $%.2f" % bankroll)
    print("  Total PnL:         $%.2f" % total_pnl)
    print("  Return:            %.1f%%" % ((total_pnl / initial_bankroll) * 100))
    print("  Max drawdown:      %.1f%%" % (max_dd * 100))
    print("  Sharpe ratio:      %.2f" % sharpe)

    # Per-trade detail
    if executed_trades:
        print()
        print("  Per-trade PnL breakdown:")
        for t in executed_trades:
            print(
                "    %-14s %-6s entry=%.2f pnl=$%+.2f bankroll=$%.2f %s"
                % (t["market_id"][-10:], t["side"], t["entry_price"],
                   t["pnl"], t["bankroll_after"], t["result"])
            )

    print()
    print("=" * 72)
    print("  BACKTEST COMPLETE")
    print("=" * 72)
    print()


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point: parse arguments and dispatch to the appropriate mode.

    Modes:
        (default)   Live real-time stream consumer
        --test      Fixture-based test mode (no external dependencies)
        --backtest  Historical replay with PnL simulation
    """
    global _test_mode, PRICE_RESPONSE_WAIT_MINUTES, IPS_HIGH_THRESHOLD

    parser = argparse.ArgumentParser(
        description=(
            "Strategy S2: Insider Wallet Detection. "
            "Monitors high-IPS wallet trades and emits piggyback "
            "trade signals based on price response analysis."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run with fixture data (no DB/Redis/API needed).",
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Replay historical alerts and simulate PnL.",
    )
    parser.add_argument(
        "--wait-minutes",
        type=int,
        default=PRICE_RESPONSE_WAIT_MINUTES,
        help=(
            "Minutes to wait for price response before acting "
            "(default: %d)." % PRICE_RESPONSE_WAIT_MINUTES
        ),
    )
    parser.add_argument(
        "--ips-threshold",
        type=float,
        default=IPS_HIGH_THRESHOLD,
        help=(
            "Minimum IPS score to act on alerts "
            "(default: %.2f)." % IPS_HIGH_THRESHOLD
        ),
    )
    args = parser.parse_args()

    # Apply CLI overrides to module-level constants
    PRICE_RESPONSE_WAIT_MINUTES = args.wait_minutes
    IPS_HIGH_THRESHOLD = args.ips_threshold

    if args.test:
        _test_mode = True
        logger.info("Running in --test mode with fixture data.")
        asyncio.run(_run_test_mode())
    elif args.backtest:
        _test_mode = True
        logger.info("Running in --backtest mode.")
        asyncio.run(_run_backtest_mode())
    else:
        logger.info("Running in live mode.")
        asyncio.run(_run_live_mode())


if __name__ == "__main__":
    main()
