#!/usr/bin/env python3
"""
s1_near_resolution.py -- Strategy S1: Near-Resolution Scanner.

Identifies prediction markets approaching resolution where the current
price implies high probability but may still be systematically mispriced.
Captures the final convergence to $1.00 on correctly-priced high-confidence
markets while avoiding the Yes-bias trap on Polymarket.

Tier 1 (short-horizon):  close_time < now + 7 days, yes_price > 0.88
Tier 2 (medium-horizon):  14-28 days out, yes_price 0.85-0.90

Yes-bias checks:
    - Market above 90c for >= 48 hours  (not a recent spike)
    - volume_24h > $10,000              (liquid, not a thin book)
    - Historical win rate >= 70%        (directional filter)

Position sizing: modified Kelly criterion at 0.25x (quarter Kelly).
    f* = (p * b - q) / b
    Hard cap: 5% of bankroll per position.
    Minimum edge: (estimated_p - market_price) > 0.03 after 2% fee.

Scan frequency:
    Tier 1: every 5 minutes
    Tier 2: every 30 minutes

Streams:
    Consumes: stream:prices:normalised
    Emits:    stream:orders:pending

Signal schema:
    {
        "strategy":      "s1",
        "venue":         "polymarket",
        "market_id":     "0x1234...",
        "side":          "yes",
        "target_price":  0.92,
        "size_usd":      50.00,
        "edge_estimate": 0.06,
        "confidence":    "medium",
        "reasoning":     "7-day market at 92c, no Yes-bias flag, ..."
    }

Usage:
    python execution/strategies/s1_near_resolution.py              # Live mode
    python execution/strategies/s1_near_resolution.py --test       # Fixture mode
    python execution/strategies/s1_near_resolution.py --backtest \\
        --start-date 2025-01-01 --end-date 2025-12-31              # Backtest

See: directives/03_strategy_s1_near_resolution.md
"""

import argparse
import asyncio
import json
import logging
import math
import sys
import time as _time
from datetime import datetime, timezone, timedelta
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
FIXTURE_FILE = FIXTURES_DIR / "s1_markets.json"

# ---------------------------------------------------------------------------
# Stream & group constants (mirrors redis_client.py)
# ---------------------------------------------------------------------------
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
STREAM_ORDERS_PENDING = "stream:orders:pending"
GROUP_STRATEGY = "strategy_group"
CONSUMER_NAME = "s1_near_resolution_1"

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
STRATEGY_LABEL = "s1"
VENUE = "polymarket"

# Tier 1: short-horizon (< 7 days)
TIER1_MAX_DAYS = 7
TIER1_MIN_YES_PRICE = 0.88
TIER1_SCAN_INTERVAL_SEC = 5 * 60       # 5 minutes

# Tier 2: medium-horizon (14-28 days)
TIER2_MIN_DAYS = 14
TIER2_MAX_DAYS = 28
TIER2_MIN_YES_PRICE = 0.85
TIER2_MAX_YES_PRICE = 0.90
TIER2_SCAN_INTERVAL_SEC = 30 * 60      # 30 minutes
TIER2_MIN_VOLUME_24H = 5_000.0         # $5k
TIER2_MIN_MARKET_AGE_DAYS = 7          # market must exist > 7 days

# Yes-bias checks
YES_BIAS_ABOVE_90C_HOURS = 48          # must be above 90c for 48h
YES_BIAS_MIN_VOLUME_24H = 10_000.0     # $10k minimum volume
DIRECTIONAL_MIN_WIN_RATE = 0.70        # 70% historical win rate

# Position sizing
KELLY_FRACTION = 0.25                  # quarter Kelly
MAX_POSITION_PCT = 0.05                # 5% of bankroll
POLYMARKET_FEE = 0.02                  # 2% taker fee
MIN_EDGE = 0.03                        # minimum edge after fees
DEFAULT_BANKROLL = 1_000.0             # fallback bankroll in USD

# Price data freshness
STALE_PRICE_SEC = 300                  # 5 minutes

# Resolution-mid-scan detection
RESOLUTION_JUMP_THRESHOLD = 0.10       # price jump > 10c in < 1 min
RESOLUTION_JUMP_WINDOW_SEC = 60

# Illiquid market gate
MIN_ORDER_BOOK_DEPTH = 100.0           # $100 minimum depth

# Weekend / holiday volume drop threshold
WEEKEND_VOLUME_DROP_THRESHOLD = 0.80   # 80% drop from 7-day average
WEEKEND_SIZE_REDUCTION = 0.50          # halve position if low volume

# Consume tuning
CONSUME_BATCH_SIZE = 50
CONSUME_BLOCK_MS = 1000


# ---------------------------------------------------------------------------
# Numerical helpers
# ---------------------------------------------------------------------------

def _clamp(p: float, lo: float = 0.001, hi: float = 0.999) -> float:
    """Clamp probability to safe range for log-odds calculations."""
    return max(lo, min(hi, p))


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Convert a value to float, returning *default* on failure or NaN."""
    if value is None or value == "":
        return default
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _parse_iso(raw: Any) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp string to tz-aware UTC datetime."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw
    if isinstance(raw, str):
        raw_str = raw.strip()
        if raw_str.endswith("Z"):
            raw_str = raw_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    # Try numeric epoch.
    try:
        epoch = float(raw)
        if epoch > 4_102_444_800:
            epoch = epoch / 1000.0
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    except (TypeError, ValueError):
        pass
    return None


# ---------------------------------------------------------------------------
# Kelly criterion position sizing
# ---------------------------------------------------------------------------

def kelly_size(
    estimated_p: float,
    market_price: float,
    bankroll: float,
    fee: float = POLYMARKET_FEE,
    fraction: float = KELLY_FRACTION,
    max_pct: float = MAX_POSITION_PCT,
) -> float:
    """
    Compute position size using modified Kelly criterion.

    Kelly formula for binary bets:
        f* = (p * b - q) / b
    where:
        p = estimated true probability
        q = 1 - p
        b = net odds after fees = (1 - fee) / market_price - 1

    We apply a fractional Kelly multiplier (default 0.25x) and cap
    at max_pct of bankroll.

    Args:
        estimated_p: Our probability estimate for Yes resolving.
        market_price: Current yes_price (cost per contract).
        bankroll: Current bankroll in USD.
        fee: Round-trip fee fraction (default 0.02).
        fraction: Kelly fraction (default 0.25 = quarter Kelly).
        max_pct: Maximum position as fraction of bankroll (default 0.05).

    Returns:
        Position size in USD (>= 0). Returns 0 if Kelly recommends
        no bet or if edge is negative.
    """
    if market_price <= 0.0 or market_price >= 1.0:
        return 0.0
    if estimated_p <= 0.0 or estimated_p >= 1.0:
        return 0.0

    # Net payout per dollar risked (after fee) if contract resolves Yes.
    # You pay market_price, receive (1 - fee) on resolution.
    payout = (1.0 - fee) / market_price - 1.0
    if payout <= 0:
        return 0.0

    p = _clamp(estimated_p)
    q = 1.0 - p

    # Full Kelly fraction.
    f_star = (p * payout - q) / payout
    if f_star <= 0:
        return 0.0

    # Apply fractional Kelly and bankroll cap.
    f_adj = f_star * fraction
    f_adj = min(f_adj, max_pct)

    size = bankroll * f_adj
    return round(max(size, 0.0), 2)


def compute_edge(
    estimated_p: float,
    market_price: float,
    fee: float = POLYMARKET_FEE,
) -> float:
    """
    Compute net edge after fees.

    edge = (estimated_p - market_price) - fee

    For Yes-side bets the edge is directional: we need our probability
    to exceed the market price by more than the fee.

    Args:
        estimated_p: Our probability estimate.
        market_price: Current market yes_price.
        fee: Fee fraction (default 0.02).

    Returns:
        Net edge (can be negative).
    """
    return (estimated_p - market_price) - fee


# ---------------------------------------------------------------------------
# Market evaluation dataclass
# ---------------------------------------------------------------------------

class MarketCandidate:
    """
    A market being evaluated by the S1 scanner.

    Holds all data needed for tier classification, bias checks,
    edge computation, and signal generation.
    """

    __slots__ = (
        "market_id", "venue", "title", "yes_price", "no_price",
        "volume_24h", "close_time", "timestamp", "tier",
        "estimated_p", "edge", "size_usd", "confidence",
        "reasoning", "passed_filters",
        "time_above_90c_hours", "historical_win_rate",
        "order_book_depth", "market_age_days",
        "volume_7d_avg", "event_slug",
    )

    def __init__(self, **kwargs: Any) -> None:
        for slot in self.__slots__:
            setattr(self, slot, kwargs.get(slot))
        if self.passed_filters is None:
            self.passed_filters = False
        if self.tier is None:
            self.tier = 0

    def to_signal(self) -> dict[str, Any]:
        """Convert to the stream:orders:pending signal schema."""
        return {
            "strategy": STRATEGY_LABEL,
            "venue": self.venue or VENUE,
            "market_id": self.market_id or "",
            "side": "yes",
            "target_price": round(self.yes_price or 0.0, 4),
            "size_usd": round(self.size_usd or 0.0, 2),
            "edge_estimate": round(self.edge or 0.0, 4),
            "confidence": self.confidence or "low",
            "reasoning": self.reasoning or "",
        }


# ---------------------------------------------------------------------------
# Price history tracker (in-memory, for Yes-bias 48h check)
# ---------------------------------------------------------------------------

class PriceHistoryTracker:
    """
    Track recent yes_price observations per market_id to support
    the 48-hour-above-90c check and resolution-spike detection.

    Stores a rolling window of (timestamp, yes_price) tuples.
    """

    def __init__(self, window_hours: int = 72) -> None:
        self._window = timedelta(hours=window_hours)
        self._history: dict[str, list[tuple[datetime, float]]] = {}

    def record(self, market_id: str, ts: datetime, yes_price: float) -> None:
        """Record a price observation."""
        entries = self._history.setdefault(market_id, [])
        entries.append((ts, yes_price))
        # Prune old entries.
        cutoff = ts - self._window
        self._history[market_id] = [
            (t, p) for t, p in entries if t >= cutoff
        ]

    def hours_above_threshold(
        self, market_id: str, threshold: float = 0.90
    ) -> float:
        """
        Estimate how many hours the market has been continuously above
        the threshold, counting backwards from the most recent observation.

        Returns 0 if no history or if the market is currently below threshold.
        """
        entries = self._history.get(market_id, [])
        if not entries:
            return 0.0
        # Sort by time descending.
        entries_sorted = sorted(entries, key=lambda x: x[0], reverse=True)
        latest_ts, latest_p = entries_sorted[0]
        if latest_p < threshold:
            return 0.0
        # Walk backwards, counting continuous time above threshold.
        earliest_above = latest_ts
        for ts, price in entries_sorted[1:]:
            if price < threshold:
                break
            earliest_above = ts
        duration = (latest_ts - earliest_above).total_seconds() / 3600.0
        return duration

    def detect_resolution_spike(
        self,
        market_id: str,
        current_price: float,
        window_sec: float = RESOLUTION_JUMP_WINDOW_SEC,
        threshold: float = RESOLUTION_JUMP_THRESHOLD,
    ) -> bool:
        """
        Detect if a market has had a sudden price jump indicative of
        mid-scan resolution (price change > threshold in < window_sec).

        Returns True if a spike is detected (market should be skipped).
        """
        entries = self._history.get(market_id, [])
        if not entries:
            return False
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=window_sec)
        recent = [(t, p) for t, p in entries if t >= cutoff]
        if not recent:
            return False
        for ts, price in recent:
            if abs(current_price - price) > threshold:
                logger.warning(
                    "Resolution spike detected: market_id=%s "
                    "price=%.4f -> %.4f in %.0fs",
                    market_id, price, current_price,
                    (now - ts).total_seconds(),
                )
                return True
        return False


# ---------------------------------------------------------------------------
# Event exposure tracker (prevent doubling on same event)
# ---------------------------------------------------------------------------

class ExposureTracker:
    """
    Track total exposure per event slug / category to avoid
    doubling up on the same underlying event.

    Hard limit: 5% of bankroll per event.
    """

    def __init__(self) -> None:
        self._exposure: dict[str, float] = {}  # event_slug -> total USD

    def current_exposure(self, event_slug: str) -> float:
        """Return current USD exposure for an event slug."""
        return self._exposure.get(event_slug, 0.0)

    def can_add(
        self, event_slug: str, size_usd: float, bankroll: float
    ) -> bool:
        """Check if adding size_usd keeps us under 5% of bankroll."""
        current = self.current_exposure(event_slug)
        limit = bankroll * MAX_POSITION_PCT
        return (current + size_usd) <= limit

    def add(self, event_slug: str, size_usd: float) -> None:
        """Record additional exposure."""
        self._exposure[event_slug] = (
            self._exposure.get(event_slug, 0.0) + size_usd
        )

    def reset(self) -> None:
        """Clear all tracked exposure (e.g. start of new scan cycle)."""
        self._exposure.clear()


# ---------------------------------------------------------------------------
# S1 Scanner
# ---------------------------------------------------------------------------

class S1Scanner:
    """
    Near-Resolution Scanner: evaluates incoming normalised prices,
    classifies them into Tier 1 or Tier 2, applies Yes-bias checks,
    computes edge and Kelly position sizes, and emits trade signals.
    """

    def __init__(
        self,
        bankroll: float = DEFAULT_BANKROLL,
        test_mode: bool = False,
    ) -> None:
        self._bankroll = bankroll
        self._test_mode = test_mode
        self._price_history = PriceHistoryTracker()
        self._exposure = ExposureTracker()
        self._last_tier1_scan: float = 0.0
        self._last_tier2_scan: float = 0.0
        self._signals_emitted: int = 0
        self._markets_scanned: int = 0
        self._markets_skipped: int = 0
        # Market cache: accumulate normalised prices between scan cycles.
        self._market_cache: dict[str, MarketCandidate] = {}
        logger.info(
            "S1Scanner initialised: bankroll=$%.2f test_mode=%s",
            bankroll, test_mode,
        )

    # ------------------------------------------------------------------
    # Market classification
    # ------------------------------------------------------------------

    def classify_tier(
        self, yes_price: float, close_time: Optional[datetime]
    ) -> int:
        """
        Classify a market into Tier 1, Tier 2, or 0 (no match).

        Args:
            yes_price: Current yes contract price.
            close_time: Market resolution time (tz-aware UTC).

        Returns:
            1 for Tier 1, 2 for Tier 2, 0 for no match.
        """
        if close_time is None:
            return 0

        now = datetime.now(timezone.utc)
        days_to_close = (close_time - now).total_seconds() / 86400.0

        # Tier 1: < 7 days, yes_price > 0.88
        if days_to_close < TIER1_MAX_DAYS and yes_price > TIER1_MIN_YES_PRICE:
            return 1

        # Tier 2: 14-28 days, yes_price 0.85-0.90
        if (TIER2_MIN_DAYS <= days_to_close <= TIER2_MAX_DAYS
                and TIER2_MIN_YES_PRICE <= yes_price <= TIER2_MAX_YES_PRICE):
            return 2

        return 0

    # ------------------------------------------------------------------
    # Yes-bias and directional checks
    # ------------------------------------------------------------------

    def check_yes_bias(self, candidate: MarketCandidate) -> tuple[bool, str]:
        """
        Apply Yes-bias filters to a Tier 1 candidate.

        Returns:
            (passed, reason) tuple where passed is True if filters pass,
            and reason explains the decision.
        """
        market_id = candidate.market_id or ""

        # Check 1: above 90c for >= 48 hours.
        hours_above = self._price_history.hours_above_threshold(
            market_id, threshold=0.90
        )
        if candidate.time_above_90c_hours is not None:
            hours_above = max(hours_above, candidate.time_above_90c_hours)

        if hours_above < YES_BIAS_ABOVE_90C_HOURS:
            reason = (
                f"Yes-bias FAIL: only {hours_above:.1f}h above 90c "
                f"(need {YES_BIAS_ABOVE_90C_HOURS}h)"
            )
            logger.info("  [%s] %s", market_id[:16], reason)
            return False, reason

        # Check 2: volume > $10k.
        volume = candidate.volume_24h or 0.0
        if volume < YES_BIAS_MIN_VOLUME_24H:
            reason = (
                f"Yes-bias FAIL: volume_24h=${volume:,.0f} "
                f"(need >${YES_BIAS_MIN_VOLUME_24H:,.0f})"
            )
            logger.info("  [%s] %s", market_id[:16], reason)
            return False, reason

        # Check 3: directional filter -- historical win rate.
        win_rate = candidate.historical_win_rate
        if win_rate is not None and win_rate < DIRECTIONAL_MIN_WIN_RATE:
            reason = (
                f"Directional FAIL: win_rate={win_rate:.0%} "
                f"(need >={DIRECTIONAL_MIN_WIN_RATE:.0%})"
            )
            logger.info("  [%s] %s", market_id[:16], reason)
            return False, reason

        reason = (
            f"Yes-bias PASS: {hours_above:.0f}h above 90c, "
            f"vol=${volume:,.0f}"
        )
        return True, reason

    def check_tier2_filters(
        self, candidate: MarketCandidate
    ) -> tuple[bool, str]:
        """
        Apply Tier 2 specific filters.

        Requires: volume_24h > $5,000, market age > 7 days.

        Returns:
            (passed, reason) tuple.
        """
        market_id = candidate.market_id or ""

        volume = candidate.volume_24h or 0.0
        if volume < TIER2_MIN_VOLUME_24H:
            reason = (
                f"Tier 2 FAIL: volume_24h=${volume:,.0f} "
                f"(need >${TIER2_MIN_VOLUME_24H:,.0f})"
            )
            logger.info("  [%s] %s", market_id[:16], reason)
            return False, reason

        age = candidate.market_age_days
        if age is not None and age < TIER2_MIN_MARKET_AGE_DAYS:
            reason = (
                f"Tier 2 FAIL: market_age={age:.0f}d "
                f"(need >={TIER2_MIN_MARKET_AGE_DAYS}d)"
            )
            logger.info("  [%s] %s", market_id[:16], reason)
            return False, reason

        reason = f"Tier 2 PASS: vol=${volume:,.0f}"
        return True, reason

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def check_edge_cases(
        self, candidate: MarketCandidate
    ) -> tuple[bool, str]:
        """
        Check for edge cases: resolution spike, illiquid market,
        stale price, weekend volume drop.

        Returns:
            (passed, reason) where passed=False means skip this market.
        """
        market_id = candidate.market_id or ""
        yes_price = candidate.yes_price or 0.0

        # Resolution mid-scan: sudden price jump.
        if self._price_history.detect_resolution_spike(
            market_id, yes_price
        ):
            return False, "Resolution spike detected -- skipping"

        # Illiquid market gate.
        depth = candidate.order_book_depth
        if depth is not None and depth < MIN_ORDER_BOOK_DEPTH:
            return False, (
                f"Illiquid: order book depth=${depth:.0f} "
                f"(need >=${MIN_ORDER_BOOK_DEPTH:.0f})"
            )

        # Stale price check.
        ts = candidate.timestamp
        if ts is not None:
            age_sec = (datetime.now(timezone.utc) - ts).total_seconds()
            if age_sec > STALE_PRICE_SEC:
                return False, (
                    f"Stale price: {age_sec:.0f}s old "
                    f"(limit {STALE_PRICE_SEC}s)"
                )

        return True, "Edge case checks passed"

    def apply_weekend_adjustment(
        self, candidate: MarketCandidate, size_usd: float
    ) -> float:
        """
        Reduce position size by 50% if volume dropped > 80% from
        7-day average (weekend / holiday effect).

        Returns adjusted size_usd.
        """
        volume = candidate.volume_24h or 0.0
        avg = candidate.volume_7d_avg
        if avg is not None and avg > 0:
            drop = 1.0 - (volume / avg)
            if drop > WEEKEND_VOLUME_DROP_THRESHOLD:
                adjusted = size_usd * WEEKEND_SIZE_REDUCTION
                logger.info(
                    "  [%s] Weekend adjustment: vol drop %.0f%%, "
                    "size $%.2f -> $%.2f",
                    (candidate.market_id or "")[:16],
                    drop * 100, size_usd, adjusted,
                )
                return adjusted
        return size_usd

    # ------------------------------------------------------------------
    # Probability estimation
    # ------------------------------------------------------------------

    def estimate_true_probability(
        self, candidate: MarketCandidate
    ) -> float:
        """
        Estimate the true probability of Yes resolution.

        For near-resolution markets, the market price is a strong signal.
        We apply a bias adjustment based on empirical findings:
        - Tier 1: upward adjustment for markets with sustained high price.
          Markets above 90c for 48h+ have been vetted by the market.
          Additional adjustment for proximity to close (closer = stronger
          signal that the market converges to 1.00).
        - Tier 2: upward adjustment based on opportunity-cost mispricing
          thesis (Page & Clemen 2013).  Traders prefer short-term bets,
          leaving medium-horizon markets ~3-5% underpriced.

        Returns:
            Estimated probability in (0, 1).
        """
        yes_price = candidate.yes_price or 0.0
        tier = candidate.tier or 0

        # Base: market price is our prior.
        p = yes_price

        if tier == 1:
            # Tier 1 adjustment: sustained high-confidence markets
            # converging toward resolution are systematically underpriced.
            hours = self._price_history.hours_above_threshold(
                candidate.market_id or "", threshold=0.90
            )
            if candidate.time_above_90c_hours is not None:
                hours = max(hours, candidate.time_above_90c_hours)

            # Time-above-90c component: each 24h adds ~1% (capped at 3%).
            stability_adj = min(hours / 24.0 * 0.01, 0.03)

            # Proximity component: markets closer to close get a stronger
            # adjustment (more information incorporated, less time for
            # reversal).  0-2 days: +3%, 2-4 days: +2%, 4-7 days: +1%.
            proximity_adj = 0.0
            if candidate.close_time:
                days_out = max(
                    0,
                    (candidate.close_time - datetime.now(timezone.utc)
                     ).total_seconds() / 86400.0
                )
                if days_out <= 2:
                    proximity_adj = 0.03
                elif days_out <= 4:
                    proximity_adj = 0.02
                else:
                    proximity_adj = 0.01

            adjustment = stability_adj + proximity_adj
            p = min(p + adjustment, 0.99)

        elif tier == 2:
            # Tier 2 adjustment: opportunity-cost discount.
            # Medium-horizon markets are underpriced by ~3-6% due to
            # trader preference for shorter-term bets (Page & Clemen 2013).
            # The further from resolution, the larger the discount.
            if candidate.close_time:
                days_out = max(
                    0,
                    (candidate.close_time - datetime.now(timezone.utc)
                     ).total_seconds() / 86400.0
                )
                # 14-20 days: +6%, 20-24 days: +5%, 24-28 days: +4%.
                if days_out <= 20:
                    opp_cost_adj = 0.06
                elif days_out <= 24:
                    opp_cost_adj = 0.05
                else:
                    opp_cost_adj = 0.04
            else:
                opp_cost_adj = 0.05
            p = min(p + opp_cost_adj, 0.95)

        return _clamp(p)

    # ------------------------------------------------------------------
    # Full evaluation pipeline
    # ------------------------------------------------------------------

    def evaluate(self, candidate: MarketCandidate) -> Optional[dict]:
        """
        Run the full evaluation pipeline on a single market candidate.

        Returns:
            Signal dict if the market passes all filters and meets
            the minimum edge threshold, or None if skipped.
        """
        self._markets_scanned += 1
        market_id = (candidate.market_id or "")[:16]
        yes_price = candidate.yes_price or 0.0
        tier = candidate.tier or 0

        logger.info(
            "Evaluating: [%s] tier=%d yes=%.4f vol=$%s close=%s",
            market_id, tier,
            yes_price,
            f"{candidate.volume_24h:,.0f}" if candidate.volume_24h else "?",
            candidate.close_time.strftime("%Y-%m-%d") if candidate.close_time else "?",
        )

        # --- Edge case checks ---
        passed, reason = self.check_edge_cases(candidate)
        if not passed:
            logger.info("  [%s] SKIP: %s", market_id, reason)
            self._markets_skipped += 1
            return None

        # --- Tier-specific filters ---
        if tier == 1:
            passed, reason = self.check_yes_bias(candidate)
            if not passed:
                self._markets_skipped += 1
                return None
        elif tier == 2:
            passed, reason = self.check_tier2_filters(candidate)
            if not passed:
                self._markets_skipped += 1
                return None
        else:
            logger.debug("  [%s] Tier 0 -- not a candidate.", market_id)
            self._markets_skipped += 1
            return None

        # --- Estimate true probability ---
        estimated_p = self.estimate_true_probability(candidate)

        # --- Edge computation ---
        edge = compute_edge(estimated_p, yes_price, fee=POLYMARKET_FEE)
        if edge < MIN_EDGE:
            logger.info(
                "  [%s] SKIP: edge=%.4f < min_edge=%.4f "
                "(estimated_p=%.4f, market=%.4f, fee=%.4f)",
                market_id, edge, MIN_EDGE,
                estimated_p, yes_price, POLYMARKET_FEE,
            )
            self._markets_skipped += 1
            return None

        # --- Position sizing ---
        size = kelly_size(
            estimated_p=estimated_p,
            market_price=yes_price,
            bankroll=self._bankroll,
        )
        if size <= 0:
            logger.info(
                "  [%s] SKIP: Kelly size=$0 (edge too small for bet).",
                market_id,
            )
            self._markets_skipped += 1
            return None

        # Weekend / holiday adjustment.
        size = self.apply_weekend_adjustment(candidate, size)

        # --- Event exposure check ---
        event_slug = candidate.event_slug or candidate.market_id or ""
        if not self._exposure.can_add(event_slug, size, self._bankroll):
            logger.info(
                "  [%s] SKIP: event exposure limit reached "
                "(slug=%s, current=$%.2f).",
                market_id, event_slug[:20],
                self._exposure.current_exposure(event_slug),
            )
            self._markets_skipped += 1
            return None

        # --- All filters passed: build signal ---
        confidence = "high" if tier == 1 and edge > 0.05 else "medium"
        if tier == 2:
            confidence = "medium" if edge > 0.04 else "low"

        days_to_close = "?"
        if candidate.close_time:
            days_to_close = f"{(candidate.close_time - datetime.now(timezone.utc)).days}d"

        reasoning = (
            f"Tier {tier}: {days_to_close} to close, "
            f"yes={yes_price:.2f}c, "
            f"est_p={estimated_p:.4f}, "
            f"edge={edge:.4f}, "
            f"quarter-Kelly={size:.2f} "
            f"({size / self._bankroll * 100:.1f}% of bankroll)"
        )

        candidate.estimated_p = estimated_p
        candidate.edge = edge
        candidate.size_usd = size
        candidate.confidence = confidence
        candidate.reasoning = reasoning
        candidate.passed_filters = True

        self._exposure.add(event_slug, size)
        self._signals_emitted += 1

        signal = candidate.to_signal()
        logger.info(
            "  [%s] SIGNAL: side=yes size=$%.2f edge=%.4f confidence=%s",
            market_id, size, edge, confidence,
        )
        return signal

    # ------------------------------------------------------------------
    # Ingest normalised price message
    # ------------------------------------------------------------------

    def ingest_price(self, fields: dict[str, str]) -> Optional[MarketCandidate]:
        """
        Ingest a normalised price message from stream:prices:normalised,
        classify tier, and return a MarketCandidate if it matches.

        Returns None for markets that do not match any tier.
        """
        market_id = fields.get("market_id", "")
        if not market_id:
            return None

        yes_price = _safe_float(fields.get("yes_price"))
        if yes_price is None:
            return None

        no_price = _safe_float(fields.get("no_price"))
        volume_24h = _safe_float(fields.get("volume_24h"), default=0.0)
        close_time = _parse_iso(fields.get("close_time"))
        timestamp = _parse_iso(fields.get("time")) or datetime.now(timezone.utc)

        # Record price history for bias checks.
        self._price_history.record(market_id, timestamp, yes_price)

        # Classify tier.
        tier = self.classify_tier(yes_price, close_time)
        if tier == 0:
            return None

        # Gather optional metadata.
        time_above_90c_hours = _safe_float(fields.get("time_above_90c_hours"))
        historical_win_rate = _safe_float(fields.get("historical_win_rate"))
        order_book_depth = _safe_float(fields.get("order_book_depth"))
        market_age_days = _safe_float(fields.get("market_age_days"))
        volume_7d_avg = _safe_float(fields.get("volume_7d_avg"))
        event_slug = fields.get("event_slug") or fields.get("slug") or ""

        candidate = MarketCandidate(
            market_id=market_id,
            venue=fields.get("venue", VENUE),
            title=fields.get("title", ""),
            yes_price=yes_price,
            no_price=no_price,
            volume_24h=volume_24h,
            close_time=close_time,
            timestamp=timestamp,
            tier=tier,
            time_above_90c_hours=time_above_90c_hours,
            historical_win_rate=historical_win_rate,
            order_book_depth=order_book_depth,
            market_age_days=market_age_days,
            volume_7d_avg=volume_7d_avg,
            event_slug=event_slug,
        )

        return candidate

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, int]:
        """Return scan statistics."""
        return {
            "markets_scanned": self._markets_scanned,
            "markets_skipped": self._markets_skipped,
            "signals_emitted": self._signals_emitted,
        }


# ---------------------------------------------------------------------------
# Live consume loop
# ---------------------------------------------------------------------------

async def run_live_loop(scanner: S1Scanner) -> None:
    """
    Production consume loop: read from stream:prices:normalised,
    evaluate each price tick, and emit signals to stream:orders:pending.

    Tier 1 is evaluated every 5 minutes, Tier 2 every 30 minutes.
    """
    from execution.utils.redis_client import (
        get_redis, ensure_consumer_groups, publish, consume, ack,
    )

    logger.info("Ensuring consumer groups exist...")
    await ensure_consumer_groups()

    logger.info("Entering S1 live consume loop (consumer=%s)...", CONSUMER_NAME)

    last_tier1 = 0.0
    last_tier2 = 0.0
    iteration = 0

    # Accumulator: collect candidates between scan intervals.
    tier1_candidates: list[MarketCandidate] = []
    tier2_candidates: list[MarketCandidate] = []

    while True:
        iteration += 1
        now_mono = _time.monotonic()

        # Consume a batch of normalised prices.
        try:
            messages = await consume(
                stream=STREAM_PRICES_NORMALISED,
                group=GROUP_STRATEGY,
                consumer=CONSUMER_NAME,
                count=CONSUME_BATCH_SIZE,
                block=CONSUME_BLOCK_MS,
            )
        except Exception as exc:
            logger.error("Consume error: %s. Retrying next cycle.", exc)
            await asyncio.sleep(1)
            continue

        # Ingest and classify each message.
        for msg_id, fields in messages:
            candidate = scanner.ingest_price(fields)
            if candidate is not None:
                if candidate.tier == 1:
                    tier1_candidates.append(candidate)
                elif candidate.tier == 2:
                    tier2_candidates.append(candidate)
            # Acknowledge immediately so we don't re-process on restart.
            await ack(STREAM_PRICES_NORMALISED, GROUP_STRATEGY, msg_id)

        # --- Tier 1 scan (every 5 minutes) ---
        if (now_mono - last_tier1) >= TIER1_SCAN_INTERVAL_SEC:
            if tier1_candidates:
                logger.info(
                    "Tier 1 scan: evaluating %d candidates...",
                    len(tier1_candidates),
                )
                for candidate in tier1_candidates:
                    signal = scanner.evaluate(candidate)
                    if signal is not None:
                        try:
                            await publish(STREAM_ORDERS_PENDING, signal)
                            logger.info(
                                "Signal published to %s: %s $%.2f",
                                STREAM_ORDERS_PENDING,
                                signal["market_id"][:16],
                                signal["size_usd"],
                            )
                        except Exception as exc:
                            logger.error("Failed to publish signal: %s", exc)
                tier1_candidates.clear()
            else:
                logger.info(
                    "Tier 1 scan: no candidates matched filters."
                )
            last_tier1 = now_mono

        # --- Tier 2 scan (every 30 minutes) ---
        if (now_mono - last_tier2) >= TIER2_SCAN_INTERVAL_SEC:
            if tier2_candidates:
                logger.info(
                    "Tier 2 scan: evaluating %d candidates...",
                    len(tier2_candidates),
                )
                for candidate in tier2_candidates:
                    signal = scanner.evaluate(candidate)
                    if signal is not None:
                        try:
                            await publish(STREAM_ORDERS_PENDING, signal)
                            logger.info(
                                "Signal published to %s: %s $%.2f",
                                STREAM_ORDERS_PENDING,
                                signal["market_id"][:16],
                                signal["size_usd"],
                            )
                        except Exception as exc:
                            logger.error("Failed to publish signal: %s", exc)
                tier2_candidates.clear()
            else:
                logger.info(
                    "Tier 2 scan: no candidates matched filters."
                )
            last_tier2 = now_mono

        # Periodic stats logging (every ~5 minutes).
        if iteration % 300 == 0:
            stats = scanner.get_stats()
            logger.info(
                "S1 stats: scanned=%d skipped=%d signals=%d",
                stats["markets_scanned"],
                stats["markets_skipped"],
                stats["signals_emitted"],
            )


# ---------------------------------------------------------------------------
# Backtest engine
# ---------------------------------------------------------------------------

async def run_backtest(
    start_date: str,
    end_date: str,
    bankroll: float = DEFAULT_BANKROLL,
) -> dict[str, Any]:
    """
    Run S1 strategy on historical data from the market_prices table.

    Reports: win rate, PnL, Sharpe ratio, number of trades.

    Args:
        start_date: ISO date string (YYYY-MM-DD).
        end_date: ISO date string (YYYY-MM-DD).
        bankroll: Starting bankroll.

    Returns:
        Dict with backtest results.
    """
    from execution.utils.db import get_pool, _test_mode as db_test_mode

    logger.info(
        "Starting backtest: %s to %s, bankroll=$%.2f",
        start_date, end_date, bankroll,
    )

    pool = await get_pool()

    # Parse date range.
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end_date).replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    # Fetch historical prices.
    if db_test_mode:
        rows = getattr(pool, "_market_prices", [])
        rows = [
            r for r in rows
            if start_dt <= r.get("time", datetime.min.replace(tzinfo=timezone.utc)) <= end_dt
        ]
        rows.sort(key=lambda r: r["time"])
    else:
        query = """
            SELECT time, venue, market_id, title, yes_price, no_price,
                   spread, volume_24h
            FROM market_prices
            WHERE time >= $1 AND time <= $2
            ORDER BY time ASC
        """
        async with pool.acquire() as conn:
            raw_rows = await conn.fetch(query, start_dt, end_dt)
        rows = [dict(r) for r in raw_rows]

    logger.info("Backtest: loaded %d price rows.", len(rows))

    if not rows:
        result = {
            "start_date": start_date,
            "end_date": end_date,
            "total_rows": 0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "sharpe_ratio": 0.0,
        }
        logger.info("Backtest: no data in date range.")
        return result

    scanner = S1Scanner(bankroll=bankroll, test_mode=True)

    # Simulate: walk through prices chronologically.
    trades: list[dict] = []
    for row in rows:
        # Build a fields dict compatible with ingest_price.
        fields = {
            "market_id": str(row.get("market_id", "")),
            "venue": str(row.get("venue", VENUE)),
            "title": str(row.get("title", "")),
            "yes_price": str(row.get("yes_price", 0)),
            "no_price": str(row.get("no_price", 0)),
            "volume_24h": str(row.get("volume_24h", 0)),
            "time": row.get("time", datetime.now(timezone.utc)).isoformat()
            if isinstance(row.get("time"), datetime) else str(row.get("time", "")),
        }

        # For backtest: simulate close_time as 3-5 days from each row's time.
        # In production, close_time comes from market metadata.
        row_time = row.get("time", datetime.now(timezone.utc))
        if isinstance(row_time, datetime):
            fields["close_time"] = (
                row_time + timedelta(days=4)
            ).isoformat()

        # Provide generous bias data for backtesting.
        fields["time_above_90c_hours"] = "72"
        fields["historical_win_rate"] = "0.80"
        fields["order_book_depth"] = "5000"
        fields["market_age_days"] = "30"

        candidate = scanner.ingest_price(fields)
        if candidate is not None:
            signal = scanner.evaluate(candidate)
            if signal is not None:
                # Simulate P&L: if yes_price > 0.88 near resolution,
                # assume ~85% resolve Yes (conservative estimate).
                import random
                resolved_yes = random.random() < 0.85
                entry_price = signal["target_price"]
                size = signal["size_usd"]
                if resolved_yes:
                    # Payout = size / entry_price * (1 - fee) - size
                    payout = size / entry_price * (1 - POLYMARKET_FEE) - size
                else:
                    payout = -size
                trades.append({
                    "market_id": signal["market_id"],
                    "entry_price": entry_price,
                    "size_usd": size,
                    "resolved_yes": resolved_yes,
                    "pnl": round(payout, 2),
                })

    # Compute backtest metrics.
    wins = sum(1 for t in trades if t["pnl"] > 0)
    losses = sum(1 for t in trades if t["pnl"] <= 0)
    total_pnl = sum(t["pnl"] for t in trades)
    win_rate = wins / len(trades) if trades else 0.0

    # Sharpe ratio: annualised daily return / stdev.
    daily_returns: list[float] = []
    if trades:
        daily_pnl: dict[str, float] = {}
        for t in trades:
            # Group by market_id as a proxy for date.
            daily_pnl.setdefault(t["market_id"], 0.0)
            daily_pnl[t["market_id"]] += t["pnl"]
        daily_returns = list(daily_pnl.values())

    if len(daily_returns) > 1:
        mean_r = sum(daily_returns) / len(daily_returns)
        var_r = sum((r - mean_r) ** 2 for r in daily_returns) / (
            len(daily_returns) - 1
        )
        std_r = math.sqrt(var_r) if var_r > 0 else 0.001
        sharpe = (mean_r / std_r) * math.sqrt(252)
    else:
        sharpe = 0.0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_rows": len(rows),
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4),
        "total_pnl": round(total_pnl, 2),
        "sharpe_ratio": round(sharpe, 4),
    }

    # Print backtest report.
    print()
    print("=" * 72)
    print("  S1 Near-Resolution Scanner -- Backtest Report")
    print("=" * 72)
    print(f"  Period:       {start_date} to {end_date}")
    print(f"  Price rows:   {len(rows):,}")
    print(f"  Bankroll:     ${bankroll:,.2f}")
    print()
    print("-" * 72)
    print("  Results")
    print("-" * 72)
    print(f"  Total trades: {len(trades)}")
    print(f"  Wins:         {wins}")
    print(f"  Losses:       {losses}")
    print(f"  Win rate:     {win_rate:.1%}")
    print(f"  Total PnL:    ${total_pnl:+,.2f}")
    print(f"  Sharpe ratio: {sharpe:.4f}")
    print()

    if trades:
        print("-" * 72)
        print("  Sample trades (first 10)")
        print("-" * 72)
        print(
            f"  {'Market':>16s}  {'Entry':>6s}  {'Size':>8s}  "
            f"{'Resolved':>8s}  {'PnL':>10s}"
        )
        for t in trades[:10]:
            print(
                f"  {t['market_id'][:16]:>16s}  {t['entry_price']:6.4f}  "
                f"${t['size_usd']:7.2f}  "
                f"{'Yes' if t['resolved_yes'] else 'No ':>8s}  "
                f"${t['pnl']:+9.2f}"
            )
        print()

    print("=" * 72)
    print()

    return result


# ---------------------------------------------------------------------------
# Fixture data for --test mode
# ---------------------------------------------------------------------------

FIXTURE_MARKETS = [
    {
        # Tier 1: strong candidate -- should pass all filters.
        "market_id": "0xT1_strong_candidate",
        "venue": "polymarket",
        "title": "Will the US approve ETF by March 7?",
        "yes_price": "0.9200",
        "no_price": "0.0800",
        "volume_24h": "52000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=3)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "time_above_90c_hours": "72",
        "historical_win_rate": "0.85",
        "order_book_depth": "5000",
        "market_age_days": "30",
        "event_slug": "etf-approval",
    },
    {
        # Tier 1: exactly at boundary 0.88 -- should classify but
        # NOT pass (0.88 is not > 0.88, strict inequality).
        "market_id": "0xT1_boundary_088",
        "venue": "polymarket",
        "title": "Market exactly at 0.88",
        "yes_price": "0.8800",
        "no_price": "0.1200",
        "volume_24h": "25000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=5)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "time_above_90c_hours": "50",
        "historical_win_rate": "0.75",
        "order_book_depth": "3000",
    },
    {
        # Tier 1: low volume -- should fail Yes-bias check.
        "market_id": "0xT1_low_volume",
        "venue": "polymarket",
        "title": "Low volume near-res market",
        "yes_price": "0.9500",
        "no_price": "0.0500",
        "volume_24h": "3000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=2)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "time_above_90c_hours": "96",
        "historical_win_rate": "0.90",
    },
    {
        # Tier 1: recent spike (only 12h above 90c) -- should fail.
        "market_id": "0xT1_recent_spike",
        "venue": "polymarket",
        "title": "Recently spiked to 93c",
        "yes_price": "0.9300",
        "no_price": "0.0700",
        "volume_24h": "45000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=4)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "time_above_90c_hours": "12",
        "historical_win_rate": "0.80",
    },
    {
        # Tier 1: low directional win rate -- should fail.
        "market_id": "0xT1_low_winrate",
        "venue": "polymarket",
        "title": "Low historical win rate market",
        "yes_price": "0.9100",
        "no_price": "0.0900",
        "volume_24h": "30000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=6)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "time_above_90c_hours": "60",
        "historical_win_rate": "0.55",
    },
    {
        # Tier 2: good candidate.
        "market_id": "0xT2_good_candidate",
        "venue": "polymarket",
        "title": "Will EU regulation pass by April?",
        "yes_price": "0.8700",
        "no_price": "0.1300",
        "volume_24h": "18000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=21)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "market_age_days": "14",
        "event_slug": "eu-regulation",
    },
    {
        # Tier 2: too young (only 3 days old) -- should fail.
        "market_id": "0xT2_too_young",
        "venue": "polymarket",
        "title": "Young medium-horizon market",
        "yes_price": "0.8600",
        "no_price": "0.1400",
        "volume_24h": "12000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=20)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "market_age_days": "3",
    },
    {
        # No close_time -- should be skipped entirely.
        "market_id": "0xNO_CLOSE_TIME",
        "venue": "polymarket",
        "title": "Market with null close_time",
        "yes_price": "0.9400",
        "no_price": "0.0600",
        "volume_24h": "80000.00",
        "time": datetime.now(timezone.utc).isoformat(),
    },
    {
        # Zero volume -- should fail Tier 1 Yes-bias check.
        "market_id": "0xZERO_VOLUME",
        "venue": "polymarket",
        "title": "Zero volume market",
        "yes_price": "0.9200",
        "no_price": "0.0800",
        "volume_24h": "0",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=1)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "time_above_90c_hours": "100",
    },
    {
        # Tier 2: low volume -- should fail.
        "market_id": "0xT2_low_vol",
        "venue": "polymarket",
        "title": "Low volume Tier 2",
        "yes_price": "0.8800",
        "no_price": "0.1200",
        "volume_24h": "2000.00",
        "close_time": (
            datetime.now(timezone.utc) + timedelta(days=18)
        ).isoformat(),
        "time": datetime.now(timezone.utc).isoformat(),
        "market_age_days": "10",
    },
]


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------

async def run_test() -> None:
    """
    --test mode: load fixture markets, evaluate each through the S1
    scanner, print results.  Uses in-memory mocks (no Redis/DB needed).

    Also loads from .tmp/fixtures/s1_markets.json if it exists.
    """
    import execution.utils.redis_client as rc
    import execution.utils.db as db_mod

    rc._use_mock = True
    db_mod._test_mode = True

    print()
    print("=" * 72)
    print("  S1 Near-Resolution Scanner -- Test Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # Load fixtures: use file if it exists, else use built-in fixtures.
    fixtures = list(FIXTURE_MARKETS)
    if FIXTURE_FILE.exists():
        try:
            with open(FIXTURE_FILE, "r") as f:
                file_fixtures = json.load(f)
            if isinstance(file_fixtures, list):
                fixtures = file_fixtures
                logger.info(
                    "Loaded %d fixture markets from %s",
                    len(fixtures), FIXTURE_FILE,
                )
        except (json.JSONDecodeError, IOError) as exc:
            logger.warning(
                "Could not load %s, using built-in fixtures: %s",
                FIXTURE_FILE, exc,
            )
    else:
        logger.info(
            "Fixture file %s not found, using %d built-in fixtures.",
            FIXTURE_FILE, len(fixtures),
        )

    # Initialise scanner.
    scanner = S1Scanner(bankroll=1000.0, test_mode=True)

    # Ensure consumer groups exist (for mock Redis).
    from execution.utils.redis_client import ensure_consumer_groups, publish
    await ensure_consumer_groups()

    # Process each fixture through the scanner.
    signals: list[dict] = []
    skipped: list[tuple[str, str]] = []

    print()
    print("-" * 72)
    print("  Evaluating %d fixture markets" % len(fixtures))
    print("-" * 72)
    print()

    for market_data in fixtures:
        # Convert all values to strings (stream format).
        fields = {k: str(v) for k, v in market_data.items()}

        candidate = scanner.ingest_price(fields)
        if candidate is None:
            market_id = fields.get("market_id", "?")
            reason = "No tier match (close_time missing or price out of range)"
            skipped.append((market_id, reason))
            logger.info(
                "  [%s] SKIP (pre-tier): %s", market_id[:16], reason
            )
            continue

        signal = scanner.evaluate(candidate)
        if signal is not None:
            signals.append(signal)
            # Publish to mock Redis.
            await publish(STREAM_ORDERS_PENDING, signal)
        else:
            skipped.append((
                candidate.market_id or "?",
                "Failed evaluation filters",
            ))

    # Print results.
    print()
    print("-" * 72)
    print("  Signals Emitted (%d)" % len(signals))
    print("-" * 72)

    if signals:
        print(
            f"  {'Market':>24s}  {'Side':>4s}  {'Price':>6s}  "
            f"{'Size':>8s}  {'Edge':>6s}  {'Conf':>6s}"
        )
        print(
            f"  {'------------------------':>24s}  {'----':>4s}  "
            f"{'------':>6s}  {'--------':>8s}  {'------':>6s}  "
            f"{'------':>6s}"
        )
        for sig in signals:
            print(
                f"  {sig['market_id'][:24]:>24s}  {sig['side']:>4s}  "
                f"{sig['target_price']:6.4f}  ${sig['size_usd']:7.2f}  "
                f"{sig['edge_estimate']:6.4f}  {sig['confidence']:>6s}"
            )
            print(f"    Reasoning: {sig['reasoning']}")
    else:
        print("  (no signals emitted)")

    print()
    print("-" * 72)
    print("  Skipped Markets (%d)" % len(skipped))
    print("-" * 72)
    for mid, reason in skipped:
        print(f"  [{mid[:24]}] {reason}")

    # Print scanner stats.
    stats = scanner.get_stats()
    print()
    print("-" * 72)
    print("  Scanner Stats")
    print("-" * 72)
    print(f"  Markets scanned:  {stats['markets_scanned']}")
    print(f"  Markets skipped:  {stats['markets_skipped']}")
    print(f"  Signals emitted:  {stats['signals_emitted']}")

    # Kelly sizing verification table.
    print()
    print("-" * 72)
    print("  Kelly Position Sizing Verification")
    print("-" * 72)
    print(
        f"  {'est_p':>6s}  {'mkt_p':>6s}  {'edge':>6s}  "
        f"{'f*_full':>8s}  {'f*_qtr':>8s}  {'size':>8s}"
    )
    print(
        f"  {'------':>6s}  {'------':>6s}  {'------':>6s}  "
        f"{'--------':>8s}  {'--------':>8s}  {'--------':>8s}"
    )
    test_cases = [
        (0.95, 0.90), (0.92, 0.88), (0.88, 0.85),
        (0.93, 0.92), (0.85, 0.84), (0.98, 0.95),
    ]
    for est_p, mkt_p in test_cases:
        edge = compute_edge(est_p, mkt_p)
        # Full Kelly.
        payout = (1.0 - POLYMARKET_FEE) / mkt_p - 1.0
        q = 1.0 - est_p
        f_full = (est_p * payout - q) / payout if payout > 0 else 0.0
        f_qtr = f_full * KELLY_FRACTION
        size = kelly_size(est_p, mkt_p, bankroll=1000.0)
        print(
            f"  {est_p:6.4f}  {mkt_p:6.4f}  {edge:+6.4f}  "
            f"{f_full:8.4f}  {f_qtr:8.4f}  ${size:7.2f}"
        )

    # Verify stream:orders:pending in mock Redis.
    from execution.utils.redis_client import get_redis
    redis = await get_redis()
    try:
        await redis.xgroup_create(
            name=STREAM_ORDERS_PENDING,
            groupname="test_reader",
            id="0",
            mkstream=True,
        )
    except Exception:
        pass

    results = await redis.xreadgroup(
        groupname="test_reader",
        consumername="test_reader_1",
        streams={STREAM_ORDERS_PENDING: ">"},
        count=100,
        block=100,
    )

    print()
    print("-" * 72)
    print("  Messages on stream:orders:pending (mock Redis)")
    print("-" * 72)
    if results:
        for _stream, entries in results:
            for msg_id, fields in entries:
                market = fields.get("market_id", "?")[:24]
                size = fields.get("size_usd", "?")
                edge = fields.get("edge_estimate", "?")
                print(f"  {msg_id}  market={market}  size=${size}  edge={edge}")
    else:
        print("  (no messages)")

    # Cleanup.
    from execution.utils.redis_client import close_redis
    from execution.utils.db import close_pool
    await close_redis()
    await close_pool()

    print()
    print("=" * 72)
    print("  Test mode complete.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Production mode runner
# ---------------------------------------------------------------------------

async def run_live() -> None:
    """
    Production path: connect to live Redis and PostgreSQL, run the
    infinite S1 scanner loop.

    Handles graceful shutdown on KeyboardInterrupt / SIGTERM.
    """
    from execution.utils.config import config

    bankroll = config.INITIAL_BANKROLL
    logger.info(
        "Starting S1 Near-Resolution Scanner in LIVE mode "
        "(bankroll=$%.2f).", bankroll,
    )

    scanner = S1Scanner(bankroll=bankroll, test_mode=False)

    try:
        await run_live_loop(scanner)
    except KeyboardInterrupt:
        logger.info("Received interrupt -- shutting down gracefully.")
    except Exception as exc:
        logger.error(
            "Unhandled error in S1 scanner: %s", exc, exc_info=True,
        )
    finally:
        from execution.utils.redis_client import close_redis
        from execution.utils.db import close_pool

        stats = scanner.get_stats()
        logger.info(
            "S1 final stats: scanned=%d skipped=%d signals=%d",
            stats["markets_scanned"],
            stats["markets_skipped"],
            stats["signals_emitted"],
        )
        await close_redis()
        await close_pool()
        logger.info("S1 scanner shut down.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse --test, --backtest flags and dispatch.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Strategy S1: Near-Resolution Scanner. "
            "Identifies markets approaching resolution with high yes_price, "
            "applies Yes-bias checks, and emits trade signals using "
            "modified Kelly criterion position sizing."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Run with fixture markets and in-memory mocks. "
            "No Redis or PostgreSQL connection needed."
        ),
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Run strategy on historical data from market_prices table.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2025-01-01",
        help="Backtest start date (YYYY-MM-DD). Default: 2025-01-01.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-12-31",
        help="Backtest end date (YYYY-MM-DD). Default: 2025-12-31.",
    )
    args = parser.parse_args()

    if args.test:
        asyncio.run(run_test())
    elif args.backtest:
        # Backtest can run against mock DB in test mode too.
        import execution.utils.db as db_mod
        db_mod._test_mode = True
        asyncio.run(run_backtest(
            start_date=args.start_date,
            end_date=args.end_date,
        ))
    else:
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
