#!/usr/bin/env python3
"""
s3_glint_news.py -- Strategy S3: Glint News Pipeline.

Consumes parsed Glint signals from stream:news:signals, computes Bayesian
posteriors by combining Glint impact scores with current market prices
(from stream:prices:normalised), applies Satopaa extremizing calibration
(alpha=1.73), and emits trade signals to stream:orders:pending when the
extremized probability diverges from the market price by more than 5 cents
after fees.

Pipeline stages:
    1. Consume signal from stream:news:signals
    2. Validate signal schema and check staleness (< 10 min)
    3. Deduplicate: discard if same market signalled within 5 min
    4. Look up current market price from stream:prices:normalised
    5. Compute likelihood ratio from impact level + relevance score
    6. Bayesian update: posterior = (prior * LR) / (prior * LR + (1-prior))
    7. Apply Satopaa extremizing: extremized_p = logistic(alpha * logit(posterior))
    8. Compute edge after fees; check decay from price movement since signal
    9. Gate: net_edge > 0.05 (0.07 on weekends)
    10. Modified Kelly position sizing
    11. Emit trade signal to stream:orders:pending

Usage:
    python execution/strategies/s3_glint_news.py              # Live mode
    python execution/strategies/s3_glint_news.py --test       # Fixture mode
    python execution/strategies/s3_glint_news.py --backtest   # Historical backtest

See: directives/05_strategy_s3_glint_news.md
"""

import argparse
import asyncio
import json
import logging
import math
import sys
from datetime import datetime, timezone, timedelta
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/strategies/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "s3_glint_signals.json"

# Ensure project root is on sys.path so `execution.utils.*` imports work
# when this script is invoked directly (python execution/strategies/...).
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Redis stream constants
# ---------------------------------------------------------------------------
STREAM_NEWS_SIGNALS = "stream:news:signals"
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
STREAM_ORDERS_PENDING = "stream:orders:pending"

# Consumer group and consumer name for this strategy.
GROUP_NEWS = "news_group"
CONSUMER_NAME = "s3_glint_news"

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
# Extremizing parameter (Satopaa et al. 2014, prediction markets).
ALPHA = 1.73

# Base likelihood ratios by Glint impact level.
BASE_LIKELIHOOD_RATIOS: dict[str, float] = {
    "HIGH":   3.0,
    "MEDIUM": 1.5,
    "LOW":    1.1,
}

# Polymarket taker fee (~2%).
FEE = 0.02

# Minimum net edge thresholds (after fees).
MIN_EDGE_WEEKDAY = 0.05   # 5 cents
MIN_EDGE_WEEKEND = 0.07   # 7 cents (lower liquidity on weekends)

# Signal staleness: discard signals older than 10 minutes.
MAX_SIGNAL_AGE = timedelta(minutes=10)

# Deduplication window: ignore same-market signals within 5 minutes.
DEDUP_WINDOW = timedelta(minutes=5)

# Edge decay: if market price has already moved > 2c toward the posterior
# since the signal timestamp, reduce edge estimate by the movement amount.
DECAY_THRESHOLD = 0.02

# Probability clamp bounds (avoid extreme values from calibration errors).
P_CLAMP_LO = 0.01
P_CLAMP_HI = 0.99

# Modified Kelly fraction (fractional Kelly for risk management).
KELLY_FRACTION = 0.25

# Maximum position size in USD.
MAX_POSITION_USD = 100.0

# Minimum position size in USD (below this, not worth the gas/fees).
MIN_POSITION_USD = 5.0

# Consumer poll settings.
CONSUME_COUNT = 10
CONSUME_BLOCK_MS = 2000

# Confidence mapping based on impact level.
CONFIDENCE_MAP: dict[str, str] = {
    "HIGH":   "high",
    "MEDIUM": "medium",
    "LOW":    "low",
}

# Valid signal types (from directive).
VALID_SIGNAL_TYPES = {
    "news_break", "data_release", "political",
    "legal", "market_move", "rumor",
}

# ---------------------------------------------------------------------------
# Deduplication state
# ---------------------------------------------------------------------------
# Maps market_id -> timestamp of last processed signal for that market.
_recent_signals: dict[str, datetime] = {}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _clamp_probability(p: float) -> float:
    """Clamp a probability to [P_CLAMP_LO, P_CLAMP_HI]."""
    return max(P_CLAMP_LO, min(P_CLAMP_HI, p))


def _is_weekend(dt: datetime) -> bool:
    """Return True if the given datetime falls on a Saturday or Sunday."""
    return dt.weekday() in (5, 6)


def _get_min_edge() -> float:
    """Return the minimum edge threshold, accounting for weekends."""
    now = datetime.now(timezone.utc)
    if _is_weekend(now):
        return MIN_EDGE_WEEKEND
    return MIN_EDGE_WEEKDAY


def _parse_timestamp(ts_str: str) -> Optional[datetime]:
    """
    Parse an ISO-8601 timestamp string into a timezone-aware datetime.

    Handles both 'Z' suffix and explicit '+00:00' offsets.

    Returns:
        datetime with tzinfo, or None if parsing fails.
    """
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logger.warning("Could not parse timestamp '%s'.", ts_str)
        return None


def _is_stale(signal_ts: datetime) -> bool:
    """
    Check whether a signal is older than MAX_SIGNAL_AGE.

    Args:
        signal_ts: Signal timestamp (timezone-aware).

    Returns:
        True if the signal is too old to process.
    """
    age = datetime.now(timezone.utc) - signal_ts
    if age > MAX_SIGNAL_AGE:
        logger.info(
            "Signal is stale (age=%s, max=%s). Discarding.",
            age, MAX_SIGNAL_AGE,
        )
        return True
    return False


def _is_duplicate(market_id: str, signal_ts: datetime) -> bool:
    """
    Check whether we have already processed a signal for this market
    within the DEDUP_WINDOW.

    Args:
        market_id: The contract/market identifier.
        signal_ts: Timestamp of the current signal.

    Returns:
        True if a signal for this market was processed within the window.
    """
    last_ts = _recent_signals.get(market_id)
    if last_ts is not None:
        elapsed = signal_ts - last_ts
        if elapsed < DEDUP_WINDOW:
            logger.info(
                "Duplicate signal for market %s (%.1fs ago, window=%ss). "
                "Using most recent only.",
                market_id,
                elapsed.total_seconds(),
                DEDUP_WINDOW.total_seconds(),
            )
            return True
    return False


def _record_signal(market_id: str, signal_ts: datetime) -> None:
    """Record that we processed a signal for this market at signal_ts."""
    _recent_signals[market_id] = signal_ts

    # Evict old entries to prevent unbounded growth.
    cutoff = datetime.now(timezone.utc) - DEDUP_WINDOW * 2
    stale_keys = [
        k for k, v in _recent_signals.items() if v < cutoff
    ]
    for k in stale_keys:
        del _recent_signals[k]


def compute_likelihood_ratio(impact_level: str, relevance_score: float) -> float:
    """
    Compute the likelihood ratio from Glint impact level and relevance score.

    LR = base_lr[impact_level] * relevance_score

    Args:
        impact_level: "HIGH", "MEDIUM", or "LOW".
        relevance_score: Float in [0.0, 1.0].

    Returns:
        Likelihood ratio (>= 0).
    """
    base_lr = BASE_LIKELIHOOD_RATIOS.get(impact_level.upper(), 1.1)
    lr = base_lr * max(0.0, min(1.0, relevance_score))
    logger.debug(
        "Likelihood ratio: base_lr=%.2f * relevance=%.2f = %.4f",
        base_lr, relevance_score, lr,
    )
    return lr


def compute_edge_with_decay(
    extremized_p: float,
    market_price: float,
    prior_price: float,
    fee: float = FEE,
) -> float:
    """
    Compute net edge after fees, accounting for price decay.

    If the market price has already moved > 2c toward the posterior since the
    signal, reduce the edge estimate by the movement amount.

    Args:
        extremized_p: Our extremized probability estimate.
        market_price: Current market price (may have moved since signal).
        prior_price: Market price at time of signal (the prior).
        fee: Round-trip fee as a fraction.

    Returns:
        Net edge after fees and decay adjustment.
    """
    raw_edge = abs(extremized_p - market_price)

    # Check for price decay: if market already moved toward our estimate.
    price_movement = abs(market_price - prior_price)
    if price_movement > DECAY_THRESHOLD:
        # Market already moved; check if it moved toward our posterior.
        direction_match = (
            (extremized_p > prior_price and market_price > prior_price)
            or (extremized_p < prior_price and market_price < prior_price)
        )
        if direction_match:
            # Reduce edge by the movement already absorbed.
            raw_edge = max(0.0, raw_edge - price_movement)
            logger.info(
                "Edge decay applied: market moved %.4f toward posterior. "
                "Adjusted raw_edge=%.4f",
                price_movement, raw_edge,
            )

    net_edge = raw_edge - fee
    return net_edge


def compute_side(extremized_p: float, market_price: float) -> str:
    """
    Determine trade side (yes or no) based on directional edge.

    Args:
        extremized_p: Our extremized probability estimate.
        market_price: Current market price.

    Returns:
        "yes" if we believe the event is more likely than priced,
        "no" if we believe it is less likely.
    """
    return "yes" if extremized_p > market_price else "no"


def modified_kelly_size(
    edge: float,
    probability: float,
    bankroll: float,
    kelly_fraction: float = KELLY_FRACTION,
) -> float:
    """
    Compute position size using modified (fractional) Kelly criterion.

    Full Kelly: f* = (p * b - q) / b
    where b = odds (net payout per $1 wagered), p = probability of win,
    q = 1 - p.

    For binary prediction markets: b = (1/market_price) - 1 for yes side.
    We use a fractional Kelly (default 25%) for risk management.

    Args:
        edge: Net edge after fees (positive).
        probability: Our estimated probability (extremized_p).
        bankroll: Current bankroll in USD.
        kelly_fraction: Fraction of full Kelly to use (default 0.25).

    Returns:
        Position size in USD, clamped to [MIN_POSITION_USD, MAX_POSITION_USD].
    """
    if edge <= 0 or probability <= 0 or probability >= 1:
        return 0.0

    # Kelly formula for binary outcome.
    # f* = edge / odds, simplified for prediction market payoff structure.
    # Effective odds: b = 1/p_market - 1 (but we use our estimate as the true p).
    q = 1.0 - probability
    if q <= 0:
        return 0.0

    # Kelly fraction of bankroll.
    # f* = (p * (1 + b) - 1) / b where b = odds
    # Simplified: f* = edge / (1 - market_implied_p)
    # But safer to use: f* = p - q/b ~ edge for small edges
    kelly_f = edge / (1.0 - probability) if probability < 0.99 else edge
    kelly_f = max(0.0, kelly_f)

    # Apply fractional Kelly.
    size = bankroll * kelly_f * kelly_fraction
    size = max(MIN_POSITION_USD, min(MAX_POSITION_USD, size))

    logger.debug(
        "Kelly sizing: edge=%.4f, p=%.4f, kelly_f=%.4f, fraction=%.2f, "
        "raw_size=%.2f, clamped_size=%.2f",
        edge, probability, kelly_f, kelly_fraction,
        bankroll * kelly_f * kelly_fraction, size,
    )
    return round(size, 2)


def build_trade_signal(
    market_id: str,
    side: str,
    extremized_p: float,
    prior_price: float,
    posterior: float,
    edge: float,
    size_usd: float,
    signal: dict,
) -> dict:
    """
    Build the trade signal dict for emission to stream:orders:pending.

    Args:
        market_id: Contract/market identifier.
        side: "yes" or "no".
        extremized_p: Our extremized probability.
        prior_price: Market price used as prior.
        posterior: Raw Bayesian posterior (before extremizing).
        edge: Net edge after fees.
        size_usd: Position size in USD.
        signal: Original Glint signal dict.

    Returns:
        Trade signal dict matching the schema in the directive.
    """
    impact_level = signal.get("impact_level", "MEDIUM")
    confidence = CONFIDENCE_MAP.get(impact_level, "medium")
    signal_type = signal.get("signal_type", "news_break")

    target_price = round(extremized_p, 4) if side == "yes" else round(
        1.0 - extremized_p, 4
    )

    reasoning = (
        f"Glint {impact_level} impact signal. "
        f"Prior={prior_price:.2f}, Posterior={posterior:.2f}, "
        f"Extremized={extremized_p:.2f}. "
        f"Edge={edge:.2f} after fees."
    )

    return {
        "strategy": "s3",
        "venue": "polymarket",
        "market_id": market_id,
        "side": side,
        "target_price": target_price,
        "size_usd": size_usd,
        "edge_estimate": round(edge, 4),
        "confidence": confidence,
        "reasoning": reasoning,
        "glint_signal_type": signal_type,
        "glint_impact": impact_level,
        "prior_price": round(prior_price, 4),
        "posterior": round(posterior, 4),
        "extremized_p": round(extremized_p, 4),
    }


# ---------------------------------------------------------------------------
# Price lookup
# ---------------------------------------------------------------------------

async def _lookup_market_price(market_id: str) -> Optional[float]:
    """
    Look up the current market price for a given contract from the
    normalised price stream.

    Scans recent entries in stream:prices:normalised for a matching
    market_id and returns the yes_price.

    In --test mode, returns a synthetic price derived from the market_id
    hash (deterministic but varied).

    Args:
        market_id: Contract/market identifier (0x... or title string).

    Returns:
        Current yes_price as float, or None if not found.
    """
    from execution.utils.redis_client import get_redis

    r = await get_redis()

    # For the mock, we need to check if the stream exists and has data.
    # In a real system, we would XREVRANGE to find the most recent price.
    # The InMemoryRedis mock does not support XREVRANGE, so we scan the
    # stream entries directly for testing.
    if hasattr(r, '_streams'):
        # Mock Redis: scan stream entries.
        entries = r._streams.get(STREAM_PRICES_NORMALISED, [])
        for _msg_id, fields in reversed(entries):
            if fields.get("market_id") == market_id:
                try:
                    return float(fields["yes_price"])
                except (KeyError, ValueError):
                    continue
        return None

    # Live Redis: use XREVRANGE to find the most recent entry.
    try:
        results = await r.xrevrange(
            STREAM_PRICES_NORMALISED, count=500,
        )
        for _msg_id, fields in results:
            if fields.get("market_id") == market_id:
                try:
                    return float(fields["yes_price"])
                except (KeyError, ValueError):
                    continue
    except Exception as exc:
        logger.error("Failed to lookup price for %s: %s", market_id, exc)

    return None


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

async def process_signal(
    signal: dict,
    bankroll: float,
    extremizer: "Extremizer",
    test_prices: Optional[dict[str, float]] = None,
) -> list[dict]:
    """
    Process a single Glint signal through the full S3 pipeline.

    Stages:
        1. Validate signal schema
        2. Check staleness (< 10 min, skipped in --test/--backtest)
        3. For each matched contract:
           a. Check deduplication window
           b. Look up market price (or use test_prices)
           c. Compute likelihood ratio
           d. Bayesian update
           e. Extremize posterior
           f. Compute edge with decay
           g. Gate on minimum edge
           h. Kelly position sizing
           i. Build and collect trade signal

    Args:
        signal: Parsed Glint signal dict.
        bankroll: Current bankroll in USD.
        extremizer: Extremizer instance (from execution.pipeline.extremizer).
        test_prices: Optional dict of market_id -> price for test mode.

    Returns:
        List of trade signal dicts to emit (may be empty if no edge).
    """
    trade_signals: list[dict] = []

    # -- Validate required fields ------------------------------------------
    signal_type = signal.get("signal_type", "")
    impact_level = signal.get("impact_level", "")
    relevance_score_raw = signal.get("relevance_score", 0.5)
    matched_contracts_raw = signal.get("matched_contracts", [])
    headline = signal.get("headline", "")
    timestamp_str = signal.get("timestamp", "")

    if not signal_type or not impact_level:
        logger.warning(
            "Signal missing required fields (signal_type=%s, impact_level=%s). "
            "Skipping.",
            signal_type, impact_level,
        )
        return trade_signals

    # Parse relevance score (may come from Redis as string).
    try:
        relevance_score = float(relevance_score_raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid relevance_score '%s'. Using default 0.5.",
            relevance_score_raw,
        )
        relevance_score = 0.5

    # Parse matched_contracts (may be JSON-encoded string from Redis).
    if isinstance(matched_contracts_raw, str):
        try:
            matched_contracts = json.loads(matched_contracts_raw)
        except (json.JSONDecodeError, TypeError):
            matched_contracts = [
                c.strip() for c in matched_contracts_raw.split(",")
                if c.strip()
            ]
    elif isinstance(matched_contracts_raw, list):
        matched_contracts = matched_contracts_raw
    else:
        matched_contracts = []

    if not matched_contracts:
        logger.warning(
            "Signal has no matched contracts. Skipping. headline=%.60s",
            headline,
        )
        return trade_signals

    # -- Parse and check staleness (skip in test mode) ---------------------
    signal_ts = _parse_timestamp(timestamp_str)
    if signal_ts is None:
        signal_ts = datetime.now(timezone.utc)

    if test_prices is None:
        # Live mode: enforce staleness.
        if _is_stale(signal_ts):
            return trade_signals

    # -- Compute likelihood ratio ------------------------------------------
    lr = compute_likelihood_ratio(impact_level, relevance_score)

    if lr <= 0:
        logger.warning(
            "Computed LR <= 0 (lr=%.4f). Skipping signal.", lr,
        )
        return trade_signals

    # -- Process each matched contract independently -----------------------
    min_edge = _get_min_edge()

    for market_id in matched_contracts:
        market_id = str(market_id).strip()
        if not market_id:
            continue

        # Deduplication check (skip in backtest mode).
        if test_prices is None and _is_duplicate(market_id, signal_ts):
            continue

        # -- Look up market price ------------------------------------------
        if test_prices is not None:
            market_price = test_prices.get(market_id)
        else:
            market_price = await _lookup_market_price(market_id)

        if market_price is None:
            logger.warning(
                "No market price found for '%s'. Skipping contract.",
                market_id[:20] + "..." if len(market_id) > 20 else market_id,
            )
            continue

        # Validate price range.
        if market_price <= 0.0 or market_price >= 1.0:
            logger.warning(
                "Market price %.4f out of valid range for '%s'. Skipping.",
                market_price, market_id,
            )
            continue

        prior = market_price

        # -- Bayesian update -----------------------------------------------
        posterior = extremizer.bayesian_update(prior, lr)

        # -- Extremize -----------------------------------------------------
        extremized_p = extremizer.extremize(posterior)

        # Enforce probability clamp [0.01, 0.99].
        extremized_p = _clamp_probability(extremized_p)

        logger.info(
            "Market %s: prior=%.4f, LR=%.4f, posterior=%.4f, "
            "extremized=%.4f",
            market_id[:16] + "..." if len(market_id) > 16 else market_id,
            prior, lr, posterior, extremized_p,
        )

        # -- Edge computation with decay -----------------------------------
        # In test mode with test_prices, prior_price == market_price (no decay).
        net_edge = compute_edge_with_decay(
            extremized_p=extremized_p,
            market_price=market_price,
            prior_price=prior,
            fee=FEE,
        )

        # -- Gate on minimum edge ------------------------------------------
        if net_edge < min_edge:
            logger.info(
                "Edge %.4f below threshold %.4f for %s. No trade.",
                net_edge, min_edge,
                market_id[:16] + "..." if len(market_id) > 16 else market_id,
            )
            continue

        # -- Determine side ------------------------------------------------
        side = compute_side(extremized_p, market_price)

        # -- Position sizing (modified Kelly) ------------------------------
        size_usd = modified_kelly_size(
            edge=net_edge,
            probability=extremized_p if side == "yes" else (1.0 - extremized_p),
            bankroll=bankroll,
        )

        if size_usd < MIN_POSITION_USD:
            logger.info(
                "Position size $%.2f below minimum $%.2f. Skipping.",
                size_usd, MIN_POSITION_USD,
            )
            continue

        # -- Build trade signal --------------------------------------------
        trade_signal = build_trade_signal(
            market_id=market_id,
            side=side,
            extremized_p=extremized_p,
            prior_price=prior,
            posterior=posterior,
            edge=net_edge,
            size_usd=size_usd,
            signal=signal,
        )
        trade_signals.append(trade_signal)

        # Record for deduplication.
        _record_signal(market_id, signal_ts)

        logger.info(
            "Trade signal generated: market=%s side=%s edge=%.4f size=$%.2f",
            market_id[:16] + "..." if len(market_id) > 16 else market_id,
            side, net_edge, size_usd,
        )

    return trade_signals


# ---------------------------------------------------------------------------
# Live consumer loop
# ---------------------------------------------------------------------------

async def run_live() -> None:
    """
    Main live consumer loop for Strategy S3.

    Continuously consumes Glint signals from stream:news:signals, processes
    each through the S3 pipeline, and emits trade signals to
    stream:orders:pending.

    Runs indefinitely until interrupted (Ctrl-C / SIGINT).
    """
    from execution.utils.config import config
    from execution.utils.redis_client import (
        consume,
        ack,
        publish,
        ensure_consumer_groups,
    )
    from execution.pipeline.extremizer import Extremizer

    logger.info("Strategy S3 (Glint News Pipeline) starting in LIVE mode.")

    # Ensure consumer groups exist.
    await ensure_consumer_groups()

    # Initialise the extremizer.
    extremizer = Extremizer(alpha=ALPHA)

    bankroll = config.INITIAL_BANKROLL
    signals_processed = 0
    trades_emitted = 0

    logger.info(
        "S3 live loop starting. bankroll=$%.2f, alpha=%.2f, "
        "min_edge_weekday=%.2f, min_edge_weekend=%.2f",
        bankroll, ALPHA, MIN_EDGE_WEEKDAY, MIN_EDGE_WEEKEND,
    )

    try:
        while True:
            # Consume signals from stream:news:signals.
            messages = await consume(
                stream=STREAM_NEWS_SIGNALS,
                group=GROUP_NEWS,
                consumer=CONSUMER_NAME,
                count=CONSUME_COUNT,
                block=CONSUME_BLOCK_MS,
            )

            if not messages:
                continue

            for msg_id, signal_data in messages:
                signals_processed += 1

                try:
                    trade_signals = await process_signal(
                        signal=signal_data,
                        bankroll=bankroll,
                        extremizer=extremizer,
                    )

                    # Emit each trade signal to stream:orders:pending.
                    for ts in trade_signals:
                        await publish(STREAM_ORDERS_PENDING, ts)
                        trades_emitted += 1
                        logger.info(
                            "Emitted trade #%d to %s: market=%s side=%s "
                            "edge=%.4f size=$%.2f",
                            trades_emitted,
                            STREAM_ORDERS_PENDING,
                            ts.get("market_id", "?")[:16],
                            ts.get("side", "?"),
                            ts.get("edge_estimate", 0),
                            ts.get("size_usd", 0),
                        )

                except Exception as exc:
                    logger.error(
                        "Error processing signal %s: %s", msg_id, exc,
                        exc_info=True,
                    )

                # Acknowledge the message regardless of processing outcome
                # to prevent re-delivery loops.
                await ack(STREAM_NEWS_SIGNALS, GROUP_NEWS, msg_id)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("S3 live loop interrupted. Shutting down.")

    finally:
        from execution.utils.redis_client import close_redis
        await close_redis()
        logger.info(
            "S3 shutdown. Signals processed: %d, Trades emitted: %d.",
            signals_processed, trades_emitted,
        )


# ---------------------------------------------------------------------------
# Test mode (--test)
# ---------------------------------------------------------------------------

async def run_test() -> None:
    """
    --test mode: load fixture Glint signals, compute posteriors against
    fixture market prices, and report results.

    Uses InMemoryRedis mock -- no network calls are made.

    If the fixture file (.tmp/fixtures/s3_glint_signals.json) does not exist,
    falls back to using the glint_alerts.json fixture processed through the
    Glint parser with synthetic market prices.
    """
    from execution.pipeline.extremizer import Extremizer

    # Enable mock Redis.
    import execution.utils.redis_client as redis_mod
    redis_mod._use_mock = True

    from execution.utils.redis_client import (
        publish,
        ensure_consumer_groups,
        stream_len,
        close_redis,
    )
    from execution.utils.config import config

    logger.info("Strategy S3 (Glint News Pipeline) starting in TEST mode.")

    # Ensure consumer groups exist on mock.
    await ensure_consumer_groups()

    # Initialise extremizer.
    extremizer = Extremizer(alpha=ALPHA)

    bankroll = config.INITIAL_BANKROLL

    # -- Load fixture data -------------------------------------------------
    signals: list[dict] = []
    test_prices: dict[str, float] = {}

    if FIXTURE_FILE.exists():
        logger.info("Loading S3 fixtures from %s.", FIXTURE_FILE)
        raw = FIXTURE_FILE.read_text(encoding="utf-8")
        try:
            fixture_data = json.loads(raw)
            if isinstance(fixture_data, dict):
                signals = fixture_data.get("signals", [])
                test_prices = fixture_data.get("prices", {})
            elif isinstance(fixture_data, list):
                signals = fixture_data
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in fixture file: %s", exc)
            sys.exit(1)
    else:
        logger.info(
            "S3 fixture file not found (%s). "
            "Falling back to glint_alerts.json with synthetic prices.",
            FIXTURE_FILE,
        )
        # Fall back to glint_alerts.json.
        glint_fixture = FIXTURES_DIR / "glint_alerts.json"
        if not glint_fixture.exists():
            logger.error(
                "No fixture files found. Expected: %s or %s",
                FIXTURE_FILE, glint_fixture,
            )
            sys.exit(1)

        raw = glint_fixture.read_text(encoding="utf-8")
        try:
            raw_alerts: list[dict] = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in glint_alerts.json: %s", exc)
            sys.exit(1)

        # Parse each alert through the GlintTelegramParser.
        from execution.connectors.glint_telegram import GlintTelegramParser

        parser = GlintTelegramParser(
            bot_token=config.TELEGRAM_BOT_TOKEN or "test_token",
            channel_id=config.TELEGRAM_CHANNEL_ID or "-1001234567890",
        )

        for alert in raw_alerts:
            msg_text = alert.get("text", "")
            msg_date_str = alert.get("date")
            msg_ts = None
            if msg_date_str:
                try:
                    msg_ts = datetime.fromisoformat(
                        msg_date_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass

            parsed = parser.parse_alert(msg_text, timestamp=msg_ts)
            if parsed is not None:
                signals.append(parsed)

        # Generate synthetic market prices for matched contracts.
        # Use a deterministic hash to produce varied but consistent prices.
        for sig in signals:
            contracts = sig.get("matched_contracts", [])
            if isinstance(contracts, str):
                try:
                    contracts = json.loads(contracts)
                except (json.JSONDecodeError, TypeError):
                    contracts = [c.strip() for c in contracts.split(",") if c.strip()]

            for contract_id in contracts:
                if contract_id not in test_prices:
                    # Deterministic synthetic price from hash of market_id.
                    h = hash(contract_id) % 80 + 10  # range [10, 89]
                    test_prices[contract_id] = h / 100.0

    if not signals:
        logger.error("No signals to process. Check fixture data.")
        sys.exit(1)

    logger.info(
        "Loaded %d signals and %d market prices for test mode.",
        len(signals), len(test_prices),
    )

    # -- Process each signal -----------------------------------------------
    print()
    print("=" * 72)
    print("  Strategy S3: Glint News Pipeline -- Test Mode")
    print("=" * 72)

    all_trade_signals: list[dict] = []
    total_signals = len(signals)
    total_skipped = 0

    for idx, signal in enumerate(signals):
        headline = signal.get("headline", "<no headline>")
        impact = signal.get("impact_level", "?")
        sig_type = signal.get("signal_type", "?")

        print(f"\n  --- Signal #{idx + 1}/{total_signals} ---")
        print(f"  Type:    {sig_type}")
        print(f"  Impact:  {impact}")
        print(f"  Headline: {headline[:65]}")

        trade_signals = await process_signal(
            signal=signal,
            bankroll=bankroll,
            extremizer=extremizer,
            test_prices=test_prices,
        )

        if not trade_signals:
            print("  Result:  NO TRADE (edge below threshold or no price data)")
            total_skipped += 1
        else:
            for ts in trade_signals:
                all_trade_signals.append(ts)

                # Publish to mock Redis.
                await publish(STREAM_ORDERS_PENDING, ts)

                print(f"  Result:  TRADE SIGNAL")
                print(f"    Market:     {ts['market_id'][:40]}...")
                print(f"    Side:       {ts['side']}")
                print(f"    Prior:      {ts['prior_price']:.4f}")
                print(f"    Posterior:   {ts['posterior']:.4f}")
                print(f"    Extremized: {ts['extremized_p']:.4f}")
                print(f"    Edge:       {ts['edge_estimate']:.4f}")
                print(f"    Size:       ${ts['size_usd']:.2f}")
                print(f"    Confidence: {ts['confidence']}")

    # -- Summary -----------------------------------------------------------
    orders_len = await stream_len(STREAM_ORDERS_PENDING)

    print()
    print("-" * 72)
    print("  Summary")
    print("-" * 72)
    print(f"  Total signals processed:  {total_signals}")
    print(f"  Signals with no trade:    {total_skipped}")
    print(f"  Trade signals emitted:    {len(all_trade_signals)}")
    print(f"  Orders stream length:     {orders_len}")
    print(f"  Bankroll:                 ${bankroll:.2f}")
    print(f"  Alpha:                    {ALPHA}")
    print(f"  Min edge (today):         {_get_min_edge():.2f}")

    if all_trade_signals:
        avg_edge = sum(
            ts["edge_estimate"] for ts in all_trade_signals
        ) / len(all_trade_signals)
        avg_size = sum(
            ts["size_usd"] for ts in all_trade_signals
        ) / len(all_trade_signals)
        total_exposure = sum(ts["size_usd"] for ts in all_trade_signals)

        print(f"  Average edge:             {avg_edge:.4f}")
        print(f"  Average position size:    ${avg_size:.2f}")
        print(f"  Total exposure:           ${total_exposure:.2f}")

    # -- Extremizer verification -------------------------------------------
    print()
    print("-" * 72)
    print("  Extremizer Verification")
    print("-" * 72)

    # Verify fixed point: extremize(0.5, 1.73) == 0.5
    result_05 = extremizer.extremize(0.5)
    status = "PASS" if abs(result_05 - 0.5) < 1e-10 else "FAIL"
    print(f"  extremize(0.5, alpha={ALPHA}) = {result_05:.6f}  [{status}]")

    # Verify known value: extremize(0.6, 1.73) ~ 0.667
    result_06 = extremizer.extremize(0.6)
    print(f"  extremize(0.6, alpha={ALPHA}) = {result_06:.6f}  (expected ~0.667)")

    # Symmetry check.
    sym = extremizer.extremize(0.7) + extremizer.extremize(0.3)
    status = "PASS" if abs(sym - 1.0) < 1e-10 else "FAIL"
    print(f"  extremize(0.7) + extremize(0.3) = {sym:.6f}  [{status}]")

    print()
    print("=" * 72)
    print("  Test mode complete.")
    print("=" * 72)
    print()

    await close_redis()


# ---------------------------------------------------------------------------
# Backtest mode (--backtest)
# ---------------------------------------------------------------------------

async def run_backtest() -> None:
    """
    --backtest mode: replay historical Glint signals against historical prices.

    Reports:
        - Brier score of raw posteriors vs extremized posteriors
        - Calibration summary
        - Simulated PnL assuming signals are resolved at their outcome

    Uses fixture data with synthetic outcomes for demonstration. In production
    this would load from PostgreSQL (historical signals + resolved markets).
    """
    from execution.pipeline.extremizer import Extremizer, brier_score

    logger.info("Strategy S3 (Glint News Pipeline) starting in BACKTEST mode.")

    # Initialise extremizer.
    extremizer = Extremizer(alpha=ALPHA)

    # -- Load fixture data -------------------------------------------------
    # For backtest, we use the extremizer fixture data (predictions + outcomes)
    # augmented with synthetic Glint signal metadata.
    from execution.pipeline.extremizer import (
        FIXTURE_PREDICTIONS,
        FIXTURE_OUTCOMES,
        FIXTURE_MARKET_PRICES,
    )

    n = len(FIXTURE_PREDICTIONS)

    print()
    print("=" * 72)
    print("  Strategy S3: Glint News Pipeline -- Backtest Mode")
    print("  Samples: %d" % n)
    print("=" * 72)

    # -- Stage 1: Compute posteriors and extremize -------------------------
    print()
    print("-" * 72)
    print("  1. Bayesian Update + Extremizing")
    print("-" * 72)

    posteriors: list[float] = []
    extremized: list[float] = []
    edges: list[float] = []
    trades_taken: list[bool] = []
    pnl: list[float] = []

    # Simulate with varied impact levels.
    impact_cycle = ["HIGH", "MEDIUM", "LOW"]
    relevance_cycle = [0.92, 0.78, 0.55, 0.85, 0.60]

    print(f"  {'#':>3s}  {'Prior':>6s}  {'LR':>6s}  {'Post':>6s}  "
          f"{'Extrem':>6s}  {'Mkt':>6s}  {'Edge':>7s}  {'Trade':>5s}  "
          f"{'Outcome':>7s}  {'PnL':>7s}")
    print(f"  {'---':>3s}  {'------':>6s}  {'------':>6s}  {'------':>6s}  "
          f"{'------':>6s}  {'------':>6s}  {'-------':>7s}  {'-----':>5s}  "
          f"{'-------':>7s}  {'-------':>7s}")

    for i in range(n):
        raw_p = FIXTURE_PREDICTIONS[i]
        mkt_p = FIXTURE_MARKET_PRICES[i]
        outcome = FIXTURE_OUTCOMES[i]
        impact = impact_cycle[i % len(impact_cycle)]
        relevance = relevance_cycle[i % len(relevance_cycle)]

        # Compute LR.
        lr = compute_likelihood_ratio(impact, relevance)

        # Bayesian update using market price as prior.
        post = extremizer.bayesian_update(mkt_p, lr)
        posteriors.append(post)

        # Extremize.
        ext_p = extremizer.extremize(post)
        ext_p = _clamp_probability(ext_p)
        extremized.append(ext_p)

        # Edge.
        edge = compute_edge_with_decay(ext_p, mkt_p, mkt_p, FEE)
        edges.append(edge)

        # Trade gate.
        trade = edge >= MIN_EDGE_WEEKDAY
        trades_taken.append(trade)

        # PnL simulation.
        if trade:
            side = compute_side(ext_p, mkt_p)
            if side == "yes":
                # Bought yes at mkt_p, outcome determines resolution.
                trade_pnl = (1.0 - mkt_p) if outcome else (-mkt_p)
            else:
                # Bought no at (1 - mkt_p).
                trade_pnl = (1.0 - (1.0 - mkt_p)) if not outcome else (-(1.0 - mkt_p))
            # Scale by position size (simplified to $10 per trade).
            trade_pnl *= 10.0
        else:
            trade_pnl = 0.0
        pnl.append(trade_pnl)

        if i < 15:  # Print first 15 rows.
            print(
                f"  {i + 1:3d}  {mkt_p:6.3f}  {lr:6.3f}  {post:6.3f}  "
                f"{ext_p:6.3f}  {mkt_p:6.3f}  {edge:+7.4f}  "
                f"{'YES' if trade else 'no':>5s}  "
                f"{'TRUE' if outcome else 'FALSE':>7s}  "
                f"{'$' + f'{trade_pnl:+.2f}' if trade else '---':>7s}"
            )

    if n > 15:
        print(f"  ... ({n - 15} more rows)")

    # -- Stage 2: Brier scores --------------------------------------------
    print()
    print("-" * 72)
    print("  2. Calibration (Brier Score)")
    print("-" * 72)

    bs_raw = brier_score(posteriors, FIXTURE_OUTCOMES)
    bs_ext = brier_score(extremized, FIXTURE_OUTCOMES)
    bs_market = brier_score(list(FIXTURE_MARKET_PRICES), FIXTURE_OUTCOMES)

    improvement = bs_raw - bs_ext
    pct_improvement = (improvement / bs_raw) * 100.0 if bs_raw > 0 else 0.0

    print(f"  Market prices Brier:    {bs_market:.6f}")
    print(f"  Raw posteriors Brier:   {bs_raw:.6f}")
    print(f"  Extremized Brier:       {bs_ext:.6f}")
    print(f"  Improvement (ext):      {improvement:+.6f} ({pct_improvement:+.1f}%)")

    status = "PASS" if bs_ext < bs_raw else "WARN"
    print(f"  Extremizing improves:   [{status}]")

    # -- Stage 3: Alpha recalibration attempt ------------------------------
    print()
    print("-" * 72)
    print("  3. Alpha Recalibration")
    print("-" * 72)

    ext_cal = Extremizer(alpha=1.0)
    optimal_alpha = ext_cal.calibrate_alpha(posteriors, FIXTURE_OUTCOMES)
    calibrated_ext = ext_cal.extremize_batch(posteriors)
    bs_cal = brier_score(calibrated_ext, FIXTURE_OUTCOMES)

    print(f"  Optimal alpha:          {optimal_alpha:.4f}")
    print(f"  Calibrated Brier:       {bs_cal:.6f}")
    print(f"  vs default ({ALPHA}):      {bs_ext:.6f}")

    # -- Stage 4: PnL summary ---------------------------------------------
    print()
    print("-" * 72)
    print("  4. PnL Summary")
    print("-" * 72)

    num_trades = sum(1 for t in trades_taken if t)
    total_pnl = sum(pnl)
    winning_trades = sum(
        1 for i in range(n)
        if trades_taken[i] and pnl[i] > 0
    )
    losing_trades = sum(
        1 for i in range(n)
        if trades_taken[i] and pnl[i] < 0
    )
    win_rate = winning_trades / num_trades if num_trades > 0 else 0.0

    print(f"  Total signals:          {n}")
    print(f"  Trades taken:           {num_trades}")
    print(f"  Trades skipped:         {n - num_trades}")
    print(f"  Winning trades:         {winning_trades}")
    print(f"  Losing trades:          {losing_trades}")
    print(f"  Win rate:               {win_rate:.1%}")
    print(f"  Total PnL:              ${total_pnl:+.2f}")
    if num_trades > 0:
        avg_pnl = total_pnl / num_trades
        print(f"  Average PnL per trade:  ${avg_pnl:+.2f}")

    # -- Calibration curve (binned) ----------------------------------------
    print()
    print("-" * 72)
    print("  5. Calibration Curve (Extremized Posteriors)")
    print("-" * 72)

    # Bin predictions into deciles.
    bins: dict[str, list[tuple[float, bool]]] = {}
    for i in range(n):
        p = extremized[i]
        bin_key = f"{int(p * 10) * 10:02d}-{int(p * 10) * 10 + 10:02d}%"
        bins.setdefault(bin_key, []).append((p, FIXTURE_OUTCOMES[i]))

    print(f"  {'Bin':>10s}  {'N':>4s}  {'Mean Pred':>10s}  {'Actual':>7s}  {'Gap':>7s}")
    print(f"  {'----------':>10s}  {'----':>4s}  {'----------':>10s}  {'-------':>7s}  {'-------':>7s}")

    for bin_key in sorted(bins.keys()):
        items = bins[bin_key]
        mean_pred = sum(p for p, _ in items) / len(items)
        actual_rate = sum(1 for _, o in items if o) / len(items)
        gap = mean_pred - actual_rate
        print(
            f"  {bin_key:>10s}  {len(items):4d}  {mean_pred:10.4f}  "
            f"{actual_rate:7.3f}  {gap:+7.4f}"
        )

    print()
    print("=" * 72)
    print("  Backtest complete.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse flags and dispatch to the appropriate mode.

    Modes:
        --test      Load fixture signals, compute posteriors against fixture
                    market prices, report results. Uses mock Redis.

        --backtest  Replay historical signals against historical prices.
                    Reports Brier score, calibration, PnL.

        (default)   Start the live consumer loop that reads from
                    stream:news:signals and emits to stream:orders:pending.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Strategy S3: Glint News Pipeline. "
            "Consumes Glint signals, computes Bayesian posteriors with "
            "Satopaa extremizing calibration, and emits trade signals "
            "when edge exceeds threshold."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Process fixture Glint signals against fixture market prices. "
            "Uses mock Redis (no network calls)."
        ),
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help=(
            "Replay historical signals against historical prices. "
            "Reports Brier score, calibration curve, and PnL."
        ),
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running S3 in --test mode.")
        asyncio.run(run_test())
    elif args.backtest:
        logger.info("Running S3 in --backtest mode.")
        asyncio.run(run_backtest())
    else:
        logger.info("Running S3 in live mode.")
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
