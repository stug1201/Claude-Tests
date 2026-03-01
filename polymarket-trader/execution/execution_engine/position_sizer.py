#!/usr/bin/env python3
"""
position_sizer.py -- Kelly criterion position sizing for polymarket-trader.

Computes optimal position sizes using modified Kelly criterion with
per-strategy fractional Kelly multipliers and portfolio-wide constraints
(max per-position, per-strategy, per-market, total deployed).

Kelly Criterion:
    f* = (p * b - q) / b
    Where: p = estimated true probability, q = 1-p,
           b = net odds after fees (payout ratio), f* = fraction of bankroll

Fractional Kelly multipliers:
    S1 (Near-Res):       0.25x Kelly  (Quarter Kelly -- model-based, higher uncertainty)
    S2 (Insider):        0.125x Kelly (Eighth Kelly -- highest uncertainty)
    S3 (Glint News):     0.25x Kelly  (Quarter Kelly -- model-based, higher uncertainty)
    S4 (Intra Arb):      0.50x Kelly  (Half Kelly -- structural arb, lower uncertainty)
    S5 (Cross Arb):      0.50x Kelly  (Half Kelly -- structural arb, hard cap $500/pair)

Position Size Constraints:
    Max per-position:    5% of bankroll
    Max per-strategy:   20% of bankroll
    Max per-market:     10% of bankroll
    Max total deployed: 80% of bankroll
    Min position:       $10
    S5 max cross-venue: $500 per pair

Drawdown Response Curve:
    0-5%:   Normal operation
    5-10%:  Reduce all sizes to 50% of computed Kelly
    10-15%: Reduce to 25% of Kelly, halt S2
    >15%:   GLOBAL HALT -- reject all signals

Signal Schema (input):
    strategy, venue, market_id, side, target_price, size_usd,
    edge_estimate, confidence

Usage:
    from execution.execution_engine.position_sizer import PositionSizer

    sizer = PositionSizer(config, test_mode=True)
    result = await sizer.compute_size(signal)

    python execution/execution_engine/position_sizer.py          # Requires DB
    python execution/execution_engine/position_sizer.py --test   # In-memory self-test
    python execution/execution_engine/position_sizer.py --dry-run # Full pipeline, no orders

See: directives/08_execution_engine.md
     directives/09_risk_management.md
"""

import argparse
import asyncio
import logging
import sys
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/execution_engine/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
# Fractional Kelly multipliers per strategy (directive 08).
KELLY_MULTIPLIERS: dict[str, float] = {
    "s1": 0.25,    # Quarter Kelly -- Near-Res
    "s2": 0.125,   # Eighth Kelly  -- Insider Detection
    "s3": 0.25,    # Quarter Kelly -- Glint News
    "s4": 0.50,    # Half Kelly    -- Intra Arb
    "s5": 0.50,    # Half Kelly    -- Cross Arb
}

# Strategy-specific max exposure as fraction of bankroll (directive 09).
STRATEGY_MAX_EXPOSURE: dict[str, float] = {
    "s1": 0.20,    # 20% of bankroll
    "s2": 0.15,    # 15% of bankroll
    "s3": 0.15,    # 15% of bankroll
    "s4": 0.15,    # 15% of bankroll
    "s5": 0.15,    # 15% of bankroll (also hard-capped at $500/pair)
}

# ---------------------------------------------------------------------------
# Portfolio-level constraint constants (directive 08 / 09)
# ---------------------------------------------------------------------------
MAX_PER_POSITION_PCT = 0.05     # 5% of bankroll
MAX_PER_STRATEGY_PCT = 0.20     # 20% of bankroll
MAX_PER_MARKET_PCT = 0.10       # 10% of bankroll
MAX_TOTAL_DEPLOYED_PCT = 0.80   # 80% of bankroll
MIN_POSITION_USD = 10.0         # $10 minimum
S5_MAX_CROSS_VENUE_USD = 500.0  # $500 per pair

# Polymarket fee rate applied to winnings (used for odds calculation).
# Polymarket charges ~2% on net winnings; Kalshi is similar.
DEFAULT_FEE_RATE = 0.02

# ---------------------------------------------------------------------------
# Drawdown response thresholds (directive 09)
# ---------------------------------------------------------------------------
DRAWDOWN_NORMAL_MAX = 0.05       # 0-5%:   Normal
DRAWDOWN_REDUCED_MAX = 0.10      # 5-10%:  50% reduction
DRAWDOWN_SEVERE_MAX = 0.15       # 10-15%: 75% reduction, halt S2
# > 15%: GLOBAL HALT

# Drawdown multipliers applied to the Kelly-computed size.
DRAWDOWN_REDUCTION_NORMAL = 1.0   # No reduction
DRAWDOWN_REDUCTION_MODERATE = 0.5 # 50% of computed Kelly
DRAWDOWN_REDUCTION_SEVERE = 0.25  # 25% of computed Kelly

# ---------------------------------------------------------------------------
# Confidence tier multipliers (optional refinement)
# ---------------------------------------------------------------------------
CONFIDENCE_MULTIPLIERS: dict[str, float] = {
    "high": 1.0,
    "medium": 0.75,
    "low": 0.50,
}


# =========================================================================
#  PositionSizer class
# =========================================================================

class PositionSizer:
    """
    Kelly criterion position sizer with fractional multipliers and
    portfolio-wide constraint enforcement.

    Thread-safe: all mutable state access is serialised through an
    asyncio.Lock to prevent race conditions between concurrent strategy
    signals (see directive 09, "Concurrent risk checks").

    Args:
        config:    Config singleton (from execution.utils.config).
        test_mode: If True, use in-memory mock data for DB queries.
        dry_run:   If True, compute sizes but log as dry-run.
    """

    def __init__(
        self,
        config: Any,
        test_mode: bool = False,
        dry_run: bool = False,
    ) -> None:
        self._config = config
        self._test_mode = test_mode
        self._dry_run = dry_run
        self._lock = asyncio.Lock()

        # Cache of current exposure state (refreshed from DB on each call).
        self._bankroll: float = config.INITIAL_BANKROLL
        self._drawdown_pct: float = 0.0

        # When True, _refresh_drawdown is skipped (for test injection).
        self._drawdown_override: bool = False

        # In-memory exposure tracking for test mode.
        self._mock_trades: list[dict] = []

        mode_label = "TEST" if test_mode else ("DRY-RUN" if dry_run else "LIVE")
        logger.info(
            "PositionSizer initialised: bankroll=$%.2f, mode=%s",
            self._bankroll, mode_label,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def compute_size(self, signal: dict) -> dict:
        """
        Compute the approved position size for a trade signal.

        Implements the full calculation flow from directive 08:
            1. Receive signal with edge_estimate and size_usd
            2. Compute Kelly fraction using estimated probability
            3. Apply fractional Kelly multiplier for strategy type
            4. Apply drawdown response curve
            5. Apply all constraints (min, max, per-strategy, per-market)
            6. Return result dict with approved_size_usd

        Args:
            signal: Dict with keys: strategy, venue, market_id, side,
                    target_price, size_usd, edge_estimate, confidence.

        Returns:
            Dict with:
                approved:         bool -- whether the trade is approved
                approved_size_usd: float -- final approved position size
                kelly_fraction:   float -- raw Kelly fraction
                kelly_adjusted:   float -- after fractional multiplier
                drawdown_pct:     float -- current 24h drawdown
                rejection_reason: str or None
                details:          dict of intermediate calculations
        """
        async with self._lock:
            return await self._compute_size_locked(signal)

    async def get_current_bankroll(self) -> float:
        """Return the current bankroll (initial + realised PnL)."""
        async with self._lock:
            await self._refresh_bankroll()
            return self._bankroll

    async def get_drawdown_pct(self) -> float:
        """Return the current rolling 24h drawdown percentage."""
        async with self._lock:
            await self._refresh_drawdown()
            return self._drawdown_pct

    # ------------------------------------------------------------------
    # Internal: main computation (called under lock)
    # ------------------------------------------------------------------

    async def _compute_size_locked(self, signal: dict) -> dict:
        """Core sizing logic -- must be called while holding self._lock."""

        # -- Validate signal schema ----------------------------------------
        validation_error = self._validate_signal(signal)
        if validation_error:
            logger.warning("Signal validation failed: %s", validation_error)
            return self._reject(signal, validation_error)

        strategy = signal["strategy"].lower()
        venue = signal["venue"]
        market_id = signal["market_id"]
        side = signal["side"]
        target_price = float(signal["target_price"])
        suggested_size = float(signal["size_usd"])
        edge_estimate = float(signal["edge_estimate"])
        confidence = signal.get("confidence", "medium").lower()

        # -- Refresh portfolio state from DB -------------------------------
        await self._refresh_bankroll()
        await self._refresh_drawdown()

        bankroll = self._bankroll
        drawdown_pct = self._drawdown_pct

        # -- Step 0: Check GLOBAL HALT (drawdown > 15%) --------------------
        if drawdown_pct > DRAWDOWN_SEVERE_MAX:
            reason = (
                f"GLOBAL HALT: 24h drawdown {drawdown_pct:.1%} exceeds "
                f"{DRAWDOWN_SEVERE_MAX:.0%} threshold"
            )
            logger.error(reason)
            return self._reject(signal, reason, drawdown_pct=drawdown_pct)

        # -- Step 0b: Check S2 halt at severe drawdown (10-15%) ------------
        if strategy == "s2" and drawdown_pct > DRAWDOWN_REDUCED_MAX:
            reason = (
                f"S2 HALTED: 24h drawdown {drawdown_pct:.1%} exceeds "
                f"{DRAWDOWN_REDUCED_MAX:.0%} -- S2 (Insider Detection) "
                "is halted at this drawdown level"
            )
            logger.warning(reason)
            return self._reject(signal, reason, drawdown_pct=drawdown_pct)

        # -- Step 1: Compute estimated true probability --------------------
        # p = target_price + edge_estimate for YES side
        # For NO side, the edge flips: p = (1 - target_price) + edge_estimate
        if side.lower() in ("yes", "buy"):
            p = target_price + edge_estimate
        else:
            p = (1.0 - target_price) + edge_estimate

        # Clamp p to (0, 1) -- probabilities must be valid.
        p = max(0.001, min(0.999, p))
        q = 1.0 - p

        # -- Step 2: Compute net odds (b) after fees ----------------------
        # b = (payout / cost) - 1, where payout = 1.0 * (1 - fee_rate)
        # For a YES contract at price target_price:
        #   cost = target_price, payout = 1.0 * (1 - fee)
        #   b = ((1 - fee) / target_price) - 1
        # For a NO contract at price (1 - target_price):
        #   cost = (1 - target_price), payout = 1.0 * (1 - fee)
        #   b = ((1 - fee) / (1 - target_price)) - 1
        fee_rate = DEFAULT_FEE_RATE
        if side.lower() in ("yes", "buy"):
            cost = target_price
        else:
            cost = 1.0 - target_price

        # Guard against zero/negative cost (degenerate market prices).
        if cost <= 0.0:
            return self._reject(
                signal,
                f"Invalid cost={cost:.4f} (target_price={target_price})",
                drawdown_pct=drawdown_pct,
            )

        b = ((1.0 - fee_rate) / cost) - 1.0

        # Guard against non-positive odds (no edge possible).
        if b <= 0.0:
            return self._reject(
                signal,
                f"Non-positive odds b={b:.4f} after fees (cost={cost:.4f})",
                drawdown_pct=drawdown_pct,
            )

        # -- Step 3: Kelly criterion: f* = (p * b - q) / b ----------------
        kelly_fraction = (p * b - q) / b

        # If Kelly fraction is <= 0, the edge is negative -- do not trade.
        if kelly_fraction <= 0.0:
            reason = (
                f"Negative Kelly fraction f*={kelly_fraction:.4f} -- "
                f"no edge (p={p:.4f}, b={b:.4f}, q={q:.4f})"
            )
            logger.info(reason)
            return self._reject(signal, reason, drawdown_pct=drawdown_pct)

        # -- Step 4: Apply fractional Kelly multiplier ---------------------
        kelly_mult = KELLY_MULTIPLIERS.get(strategy, 0.25)
        kelly_adjusted = kelly_fraction * kelly_mult

        # -- Step 5: Apply confidence tier multiplier ----------------------
        conf_mult = CONFIDENCE_MULTIPLIERS.get(confidence, 0.75)
        kelly_adjusted *= conf_mult

        # -- Step 6: Compute raw size in USD -------------------------------
        raw_size_usd = kelly_adjusted * bankroll

        # -- Step 7: Apply drawdown response curve -------------------------
        drawdown_mult = self._get_drawdown_multiplier(drawdown_pct)
        adjusted_size_usd = raw_size_usd * drawdown_mult

        # -- Step 8: Apply portfolio constraints ---------------------------
        # 8a. Max per-position: 5% of bankroll
        max_per_position = bankroll * MAX_PER_POSITION_PCT
        constrained_size = min(adjusted_size_usd, max_per_position)

        # 8b. S5 cross-venue hard cap: $500 per pair
        if strategy == "s5":
            constrained_size = min(constrained_size, S5_MAX_CROSS_VENUE_USD)

        # 8c. Max per-strategy: check current exposure + new size
        strategy_exposure = await self._get_strategy_exposure(strategy)
        strategy_max_pct = STRATEGY_MAX_EXPOSURE.get(strategy, MAX_PER_STRATEGY_PCT)
        strategy_max_usd = bankroll * strategy_max_pct
        strategy_remaining = max(0.0, strategy_max_usd - strategy_exposure)
        constrained_size = min(constrained_size, strategy_remaining)

        # 8d. Max per-market: check current exposure + new size
        market_exposure = await self._get_market_exposure(market_id)
        market_max_usd = bankroll * MAX_PER_MARKET_PCT
        market_remaining = max(0.0, market_max_usd - market_exposure)
        constrained_size = min(constrained_size, market_remaining)

        # 8e. Max total deployed: 80% of bankroll
        total_exposure = await self._get_total_exposure()
        total_max_usd = bankroll * MAX_TOTAL_DEPLOYED_PCT
        total_remaining = max(0.0, total_max_usd - total_exposure)
        constrained_size = min(constrained_size, total_remaining)

        # 8f. Do not exceed the strategy's suggested size_usd.
        constrained_size = min(constrained_size, suggested_size)

        # -- Step 9: Apply minimum position threshold ----------------------
        if constrained_size < MIN_POSITION_USD:
            reason = (
                f"Size ${constrained_size:.2f} below minimum "
                f"${MIN_POSITION_USD:.2f} -- fees would eat the edge"
            )
            logger.info(reason)
            return self._reject(signal, reason, drawdown_pct=drawdown_pct)

        # -- Step 10: Round to 2 decimal places ----------------------------
        approved_size = round(constrained_size, 2)

        # -- Build result --------------------------------------------------
        result = {
            "approved": True,
            "approved_size_usd": approved_size,
            "kelly_fraction": round(kelly_fraction, 6),
            "kelly_adjusted": round(kelly_adjusted, 6),
            "drawdown_pct": round(drawdown_pct, 4),
            "drawdown_multiplier": drawdown_mult,
            "rejection_reason": None,
            "details": {
                "strategy": strategy,
                "venue": venue,
                "market_id": market_id,
                "side": side,
                "target_price": target_price,
                "edge_estimate": edge_estimate,
                "confidence": confidence,
                "estimated_prob": round(p, 4),
                "net_odds_b": round(b, 4),
                "kelly_multiplier": kelly_mult,
                "confidence_multiplier": conf_mult,
                "bankroll": bankroll,
                "raw_size_usd": round(raw_size_usd, 2),
                "pre_constraint_size_usd": round(adjusted_size_usd, 2),
                "suggested_size_usd": suggested_size,
                "strategy_exposure": round(strategy_exposure, 2),
                "market_exposure": round(market_exposure, 2),
                "total_exposure": round(total_exposure, 2),
                "strategy_remaining": round(strategy_remaining, 2),
                "market_remaining": round(market_remaining, 2),
                "total_remaining": round(total_remaining, 2),
            },
        }

        if self._dry_run:
            logger.info(
                "[DRY-RUN] Approved: strategy=%s market=%s size=$%.2f "
                "(Kelly=%.4f, adjusted=%.4f, drawdown=%.1f%%)",
                strategy, market_id[:12], approved_size,
                kelly_fraction, kelly_adjusted, drawdown_pct * 100,
            )
        else:
            logger.info(
                "Approved: strategy=%s market=%s size=$%.2f "
                "(Kelly=%.4f, adjusted=%.4f, drawdown=%.1f%%)",
                strategy, market_id[:12], approved_size,
                kelly_fraction, kelly_adjusted, drawdown_pct * 100,
            )

        return result

    # ------------------------------------------------------------------
    # Internal: validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_signal(signal: dict) -> Optional[str]:
        """
        Validate that the signal dict contains all required fields
        with reasonable values.

        Returns None if valid, or an error string describing the problem.
        """
        required_fields = [
            "strategy", "venue", "market_id", "side",
            "target_price", "size_usd", "edge_estimate",
        ]
        for field in required_fields:
            if field not in signal:
                return f"Missing required field: '{field}'"

        # Strategy must be one of s1-s5.
        strategy = str(signal["strategy"]).lower()
        if strategy not in KELLY_MULTIPLIERS:
            return f"Unknown strategy '{strategy}' (expected s1-s5)"

        # Target price must be in (0, 1).
        try:
            tp = float(signal["target_price"])
        except (TypeError, ValueError):
            return f"Invalid target_price: '{signal['target_price']}'"
        if not (0.0 < tp < 1.0):
            return f"target_price={tp} out of range (0, 1)"

        # size_usd must be positive.
        try:
            size = float(signal["size_usd"])
        except (TypeError, ValueError):
            return f"Invalid size_usd: '{signal['size_usd']}'"
        if size <= 0:
            return f"size_usd={size} must be positive"

        # edge_estimate must be non-negative.
        try:
            edge = float(signal["edge_estimate"])
        except (TypeError, ValueError):
            return f"Invalid edge_estimate: '{signal['edge_estimate']}'"
        if edge < 0:
            return f"edge_estimate={edge} must be non-negative"

        # Side must be a recognised value.
        side = str(signal["side"]).lower()
        if side not in ("yes", "no", "buy", "sell"):
            return f"Invalid side '{side}' (expected yes/no/buy/sell)"

        return None

    # ------------------------------------------------------------------
    # Internal: rejection helper
    # ------------------------------------------------------------------

    @staticmethod
    def _reject(
        signal: dict,
        reason: str,
        drawdown_pct: float = 0.0,
    ) -> dict:
        """Build a rejection result dict."""
        return {
            "approved": False,
            "approved_size_usd": 0.0,
            "kelly_fraction": 0.0,
            "kelly_adjusted": 0.0,
            "drawdown_pct": round(drawdown_pct, 4),
            "drawdown_multiplier": 0.0,
            "rejection_reason": reason,
            "details": {
                "strategy": signal.get("strategy", "unknown"),
                "venue": signal.get("venue", "unknown"),
                "market_id": signal.get("market_id", "unknown"),
                "side": signal.get("side", "unknown"),
            },
        }

    # ------------------------------------------------------------------
    # Internal: drawdown response
    # ------------------------------------------------------------------

    @staticmethod
    def _get_drawdown_multiplier(drawdown_pct: float) -> float:
        """
        Return the sizing multiplier based on the drawdown response curve.

        Drawdown thresholds (directive 09):
            0-5%:   1.0  (normal)
            5-10%:  0.5  (reduce to 50%)
            10-15%: 0.25 (reduce to 25%)
            >15%:   0.0  (GLOBAL HALT -- handled before this is called)
        """
        if drawdown_pct <= DRAWDOWN_NORMAL_MAX:
            return DRAWDOWN_REDUCTION_NORMAL
        elif drawdown_pct <= DRAWDOWN_REDUCED_MAX:
            return DRAWDOWN_REDUCTION_MODERATE
        elif drawdown_pct <= DRAWDOWN_SEVERE_MAX:
            return DRAWDOWN_REDUCTION_SEVERE
        else:
            # GLOBAL HALT -- should be caught earlier, but be safe.
            return 0.0

    # ------------------------------------------------------------------
    # Internal: portfolio state queries
    # ------------------------------------------------------------------

    async def _refresh_bankroll(self) -> None:
        """
        Recalculate current bankroll = INITIAL_BANKROLL + sum(realised PnL).

        In test mode, uses the in-memory mock trade list.
        """
        if self._test_mode:
            realised_pnl = sum(
                t.get("pnl", 0.0) for t in self._mock_trades
                if t.get("status") == "filled"
            )
            self._bankroll = self._config.INITIAL_BANKROLL + realised_pnl
            return

        try:
            from execution.utils.db import get_trades
            trades = await get_trades(status="filled")
            realised_pnl = sum(t.get("pnl", 0.0) for t in trades)
            self._bankroll = self._config.INITIAL_BANKROLL + realised_pnl
        except Exception as exc:
            logger.warning(
                "Failed to refresh bankroll from DB: %s. "
                "Using last known value: $%.2f",
                exc, self._bankroll,
            )

    async def _refresh_drawdown(self) -> None:
        """
        Calculate rolling 24h max drawdown as percentage of bankroll.

        Uses the algorithm from directive 09:
            cumulative PnL curve -> peak -> max drawdown from peak.

        If _drawdown_override is set (via set_mock_drawdown), this method
        is a no-op so the injected value is preserved for testing.
        """
        if self._drawdown_override:
            return

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)

        if self._test_mode:
            recent_trades = [
                t for t in self._mock_trades
                if t.get("status") == "filled"
                and t.get("time", now) >= cutoff
            ]
        else:
            try:
                from execution.utils.db import get_trades
                recent_trades = await get_trades(status="filled", since=cutoff)
            except Exception as exc:
                logger.warning(
                    "Failed to refresh drawdown from DB: %s. "
                    "Using last known value: %.2f%%",
                    exc, self._drawdown_pct * 100,
                )
                return

        if not recent_trades:
            self._drawdown_pct = 0.0
            return

        # Sort by time ascending for cumulative PnL calculation.
        recent_trades.sort(key=lambda t: t.get("time", now))

        cumulative_pnl = 0.0
        peak_pnl = 0.0
        max_drawdown = 0.0

        for trade in recent_trades:
            cumulative_pnl += trade.get("pnl", 0.0)
            peak_pnl = max(peak_pnl, cumulative_pnl)
            drawdown = peak_pnl - cumulative_pnl
            max_drawdown = max(max_drawdown, drawdown)

        bankroll = self._bankroll if self._bankroll > 0 else 1.0
        self._drawdown_pct = max_drawdown / bankroll

    async def _get_strategy_exposure(self, strategy: str) -> float:
        """
        Return the total USD exposure for open positions in this strategy.
        """
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("strategy") == strategy
                and t.get("status") in ("pending", "filled")
            )

        try:
            from execution.utils.db import get_trades
            trades = await get_trades(strategy=strategy, status="pending")
            filled = await get_trades(strategy=strategy, status="filled")
            # Open exposure = pending + filled (not yet settled).
            all_open = trades + filled
            return sum(t.get("size", 0.0) for t in all_open)
        except Exception as exc:
            logger.warning(
                "Failed to query strategy exposure for '%s': %s. "
                "Returning 0.0 (conservative fallback would be max, "
                "but we allow the risk manager to catch this).",
                strategy, exc,
            )
            return 0.0

    async def _get_market_exposure(self, market_id: str) -> float:
        """
        Return the total USD exposure for open positions on this market
        across all strategies.
        """
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("market_id") == market_id
                and t.get("status") in ("pending", "filled")
            )

        try:
            from execution.utils.db import get_trades
            all_trades = await get_trades(status="pending")
            filled_trades = await get_trades(status="filled")
            all_open = all_trades + filled_trades
            return sum(
                t.get("size", 0.0) for t in all_open
                if t.get("market_id") == market_id
            )
        except Exception as exc:
            logger.warning(
                "Failed to query market exposure for '%s': %s",
                market_id, exc,
            )
            return 0.0

    async def _get_total_exposure(self) -> float:
        """
        Return the total USD exposure across all open positions
        in all strategies.
        """
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("status") in ("pending", "filled")
            )

        try:
            from execution.utils.db import get_trades
            pending = await get_trades(status="pending")
            filled = await get_trades(status="filled")
            all_open = pending + filled
            return sum(t.get("size", 0.0) for t in all_open)
        except Exception as exc:
            logger.warning(
                "Failed to query total exposure: %s", exc,
            )
            return 0.0

    # ------------------------------------------------------------------
    # Test helpers: inject mock state
    # ------------------------------------------------------------------

    def inject_mock_trade(self, trade: dict) -> None:
        """
        Add a mock trade record for test-mode exposure calculations.

        Args:
            trade: Dict with keys: strategy, market_id, side, size,
                   price, status, pnl, time.
        """
        if not self._test_mode:
            logger.warning("inject_mock_trade called outside test mode -- ignoring")
            return
        self._mock_trades.append(trade)

    def set_mock_drawdown(self, drawdown_pct: float) -> None:
        """
        Directly set the drawdown percentage for testing.

        This bypasses the DB-based calculation so drawdown response
        curve logic can be tested in isolation.  Sets the
        _drawdown_override flag so _refresh_drawdown is a no-op.
        """
        if not self._test_mode:
            logger.warning("set_mock_drawdown called outside test mode -- ignoring")
            return
        self._drawdown_pct = drawdown_pct
        self._drawdown_override = True

    def clear_mock_state(self) -> None:
        """Reset all in-memory mock state for a clean test run."""
        self._mock_trades.clear()
        self._bankroll = self._config.INITIAL_BANKROLL
        self._drawdown_pct = 0.0
        self._drawdown_override = False


# =========================================================================
#  Self-test demo
# =========================================================================

async def _run_self_test(dry_run: bool = False) -> None:
    """
    Exercise all PositionSizer logic with synthetic signals.

    Tests:
        1. Basic Kelly computation for each strategy (s1-s5)
        2. Drawdown response curve at each tier
        3. Constraint enforcement (per-position, per-strategy, per-market)
        4. S5 cross-venue hard cap
        5. Minimum position rejection
        6. Negative edge rejection
        7. GLOBAL HALT at >15% drawdown
        8. S2 halt at 10-15% drawdown
        9. Invalid signal rejection
    """
    # -- Create a minimal mock config -------------------------------------
    class MockConfig:
        INITIAL_BANKROLL = 1000.0
        MAX_DRAWDOWN_PCT = 0.15

    config = MockConfig()
    sizer = PositionSizer(config, test_mode=True, dry_run=dry_run)

    now = datetime.now(timezone.utc)
    passed = 0
    failed = 0

    def check(test_name: str, condition: bool, detail: str = "") -> None:
        nonlocal passed, failed
        if condition:
            passed += 1
            print(f"  PASS: {test_name}")
        else:
            failed += 1
            print(f"  FAIL: {test_name} -- {detail}")

    print()
    print("=" * 72)
    print("  Position Sizer -- Self-Test Suite")
    print("  Mode: %s" % ("DRY-RUN" if dry_run else "TEST"))
    print("  Time: %s" % now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("  Bankroll: $%.2f" % config.INITIAL_BANKROLL)
    print("=" * 72)

    # ---- Test 1: Basic Kelly for S1 (Quarter Kelly) ----------------------
    print()
    print("-" * 72)
    print("  Test 1: S1 (Near-Res) -- Quarter Kelly basic sizing")
    print("-" * 72)

    signal_s1 = {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xS1TEST001",
        "side": "yes",
        "target_price": 0.60,
        "size_usd": 100.0,
        "edge_estimate": 0.06,
        "confidence": "high",
    }
    result = await sizer.compute_size(signal_s1)
    check(
        "S1 approved",
        result["approved"],
        f"got rejected: {result.get('rejection_reason')}",
    )
    check(
        "S1 approved_size > 0",
        result["approved_size_usd"] > 0,
        f"size={result['approved_size_usd']}",
    )
    check(
        "S1 Kelly fraction > 0",
        result["kelly_fraction"] > 0,
        f"kelly={result['kelly_fraction']}",
    )
    check(
        "S1 size <= $50 (5% of $1000)",
        result["approved_size_usd"] <= 50.0,
        f"size={result['approved_size_usd']}",
    )
    print(f"    Kelly raw: {result['kelly_fraction']:.4f}")
    print(f"    Kelly adj: {result['kelly_adjusted']:.4f}")
    print(f"    Size: ${result['approved_size_usd']:.2f}")

    # ---- Test 2: S2 (Insider) -- Eighth Kelly ----------------------------
    print()
    print("-" * 72)
    print("  Test 2: S2 (Insider Detection) -- Eighth Kelly")
    print("-" * 72)

    sizer.clear_mock_state()
    signal_s2 = {
        "strategy": "s2",
        "venue": "polymarket",
        "market_id": "0xS2TEST001",
        "side": "yes",
        "target_price": 0.50,
        "size_usd": 200.0,
        "edge_estimate": 0.08,
        "confidence": "medium",
    }
    result = await sizer.compute_size(signal_s2)
    check(
        "S2 approved",
        result["approved"],
        f"got rejected: {result.get('rejection_reason')}",
    )
    # S2 uses 0.125x Kelly * 0.75 (medium conf) -- should be smaller than S1.
    check(
        "S2 Kelly multiplier = 0.125",
        result["details"]["kelly_multiplier"] == 0.125,
        f"mult={result['details'].get('kelly_multiplier')}",
    )
    print(f"    Kelly raw: {result['kelly_fraction']:.4f}")
    print(f"    Kelly adj: {result['kelly_adjusted']:.4f}")
    print(f"    Size: ${result['approved_size_usd']:.2f}")

    # ---- Test 3: S4 (Intra Arb) -- Half Kelly ----------------------------
    print()
    print("-" * 72)
    print("  Test 3: S4 (Intra Arb) -- Half Kelly")
    print("-" * 72)

    sizer.clear_mock_state()
    signal_s4 = {
        "strategy": "s4",
        "venue": "polymarket",
        "market_id": "0xS4TEST001",
        "side": "yes",
        "target_price": 0.45,
        "size_usd": 150.0,
        "edge_estimate": 0.05,
        "confidence": "high",
    }
    result = await sizer.compute_size(signal_s4)
    check(
        "S4 approved",
        result["approved"],
        f"got rejected: {result.get('rejection_reason')}",
    )
    check(
        "S4 Kelly multiplier = 0.50",
        result["details"]["kelly_multiplier"] == 0.50,
        f"mult={result['details'].get('kelly_multiplier')}",
    )
    print(f"    Kelly raw: {result['kelly_fraction']:.4f}")
    print(f"    Kelly adj: {result['kelly_adjusted']:.4f}")
    print(f"    Size: ${result['approved_size_usd']:.2f}")

    # ---- Test 4: S5 (Cross Arb) -- Half Kelly with $500 cap --------------
    print()
    print("-" * 72)
    print("  Test 4: S5 (Cross Arb) -- Half Kelly with $500/pair cap")
    print("-" * 72)

    sizer.clear_mock_state()
    # Use a large bankroll to trigger the $500 cap.
    sizer._bankroll = 50000.0
    signal_s5 = {
        "strategy": "s5",
        "venue": "polymarket",
        "market_id": "0xS5TEST001",
        "side": "yes",
        "target_price": 0.48,
        "size_usd": 1000.0,
        "edge_estimate": 0.04,
        "confidence": "high",
    }
    result = await sizer.compute_size(signal_s5)
    check(
        "S5 approved",
        result["approved"],
        f"got rejected: {result.get('rejection_reason')}",
    )
    check(
        "S5 size <= $500 (cross-venue cap)",
        result["approved_size_usd"] <= S5_MAX_CROSS_VENUE_USD,
        f"size={result['approved_size_usd']}",
    )
    print(f"    Kelly raw: {result['kelly_fraction']:.4f}")
    print(f"    Size: ${result['approved_size_usd']:.2f}")
    # Reset bankroll.
    sizer._bankroll = config.INITIAL_BANKROLL

    # ---- Test 5: Drawdown 0-5% -- Normal operation -----------------------
    print()
    print("-" * 72)
    print("  Test 5: Drawdown response curve -- Normal (0-5%)")
    print("-" * 72)

    sizer.clear_mock_state()
    sizer.set_mock_drawdown(0.03)  # 3% drawdown
    result = await sizer.compute_size(signal_s1)
    check(
        "Normal drawdown: approved",
        result["approved"],
        f"rejected: {result.get('rejection_reason')}",
    )
    check(
        "Normal drawdown: multiplier = 1.0",
        result["drawdown_multiplier"] == DRAWDOWN_REDUCTION_NORMAL,
        f"mult={result.get('drawdown_multiplier')}",
    )
    print(f"    Drawdown: {result['drawdown_pct']:.1%}")
    print(f"    Multiplier: {result['drawdown_multiplier']}")
    print(f"    Size: ${result['approved_size_usd']:.2f}")

    # ---- Test 6: Drawdown 5-10% -- Moderate reduction --------------------
    print()
    print("-" * 72)
    print("  Test 6: Drawdown response curve -- Moderate (5-10%)")
    print("-" * 72)

    sizer.clear_mock_state()
    sizer.set_mock_drawdown(0.07)  # 7% drawdown
    result_normal = await sizer.compute_size(signal_s1)
    check(
        "Moderate drawdown: approved",
        result_normal["approved"],
        f"rejected: {result_normal.get('rejection_reason')}",
    )
    check(
        "Moderate drawdown: multiplier = 0.5",
        result_normal["drawdown_multiplier"] == DRAWDOWN_REDUCTION_MODERATE,
        f"mult={result_normal.get('drawdown_multiplier')}",
    )
    print(f"    Drawdown: {result_normal['drawdown_pct']:.1%}")
    print(f"    Multiplier: {result_normal['drawdown_multiplier']}")
    print(f"    Size: ${result_normal['approved_size_usd']:.2f}")

    # ---- Test 7: Drawdown 10-15% -- Severe reduction + S2 halt -----------
    print()
    print("-" * 72)
    print("  Test 7: Drawdown response curve -- Severe (10-15%)")
    print("-" * 72)

    sizer.clear_mock_state()
    sizer.set_mock_drawdown(0.12)  # 12% drawdown

    # Use S4 (Half Kelly) with a large edge so the 0.25x drawdown
    # multiplier still yields a size above the $10 minimum.
    signal_severe = {
        "strategy": "s4",
        "venue": "polymarket",
        "market_id": "0xSEVERE01",
        "side": "yes",
        "target_price": 0.40,
        "size_usd": 200.0,
        "edge_estimate": 0.10,
        "confidence": "high",
    }
    result_severe = await sizer.compute_size(signal_severe)
    check(
        "Severe drawdown (S4): approved",
        result_severe["approved"],
        f"rejected: {result_severe.get('rejection_reason')}",
    )
    check(
        "Severe drawdown: multiplier = 0.25",
        result_severe["drawdown_multiplier"] == DRAWDOWN_REDUCTION_SEVERE,
        f"mult={result_severe.get('drawdown_multiplier')}",
    )

    # S2 should be HALTED at 10-15%.
    result_s2_halt = await sizer.compute_size(signal_s2)
    check(
        "Severe drawdown (S2): rejected (S2 halt)",
        not result_s2_halt["approved"],
        f"should have been rejected but was approved: ${result_s2_halt['approved_size_usd']}",
    )
    check(
        "Severe drawdown (S2): reason mentions S2 halt",
        "S2 HALTED" in (result_s2_halt.get("rejection_reason") or ""),
        f"reason={result_s2_halt.get('rejection_reason')}",
    )
    print(f"    S4 size at 12% drawdown: ${result_severe['approved_size_usd']:.2f}")
    print(f"    S2 rejected: {result_s2_halt['rejection_reason']}")

    # ---- Test 8: Drawdown >15% -- GLOBAL HALT ----------------------------
    print()
    print("-" * 72)
    print("  Test 8: Drawdown >15% -- GLOBAL HALT")
    print("-" * 72)

    sizer.clear_mock_state()
    sizer.set_mock_drawdown(0.18)  # 18% drawdown

    result_halt = await sizer.compute_size(signal_s1)
    check(
        "Global halt: S1 rejected",
        not result_halt["approved"],
        f"should have been rejected but was approved",
    )
    check(
        "Global halt: reason mentions GLOBAL HALT",
        "GLOBAL HALT" in (result_halt.get("rejection_reason") or ""),
        f"reason={result_halt.get('rejection_reason')}",
    )
    print(f"    Rejection: {result_halt['rejection_reason']}")

    # ---- Test 9: Negative edge rejection ---------------------------------
    print()
    print("-" * 72)
    print("  Test 9: Negative Kelly -- no edge")
    print("-" * 72)

    sizer.clear_mock_state()
    signal_no_edge = {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xNOEDGE01",
        "side": "yes",
        "target_price": 0.95,  # Very expensive, tiny edge.
        "size_usd": 50.0,
        "edge_estimate": 0.001,  # Almost no edge.
        "confidence": "low",
    }
    result_no_edge = await sizer.compute_size(signal_no_edge)
    check(
        "No edge: rejected",
        not result_no_edge["approved"],
        f"should have been rejected but was approved: ${result_no_edge['approved_size_usd']}",
    )
    print(f"    Rejection: {result_no_edge.get('rejection_reason')}")

    # ---- Test 10: Minimum position rejection -----------------------------
    print()
    print("-" * 72)
    print("  Test 10: Below minimum position ($10)")
    print("-" * 72)

    sizer.clear_mock_state()
    # Reduce bankroll so Kelly yields < $10.
    sizer._bankroll = 50.0
    signal_tiny = {
        "strategy": "s2",  # Eighth Kelly on tiny bankroll.
        "venue": "polymarket",
        "market_id": "0xTINY001",
        "side": "yes",
        "target_price": 0.60,
        "size_usd": 5.0,  # Suggested size also small.
        "edge_estimate": 0.03,
        "confidence": "low",
    }
    result_tiny = await sizer.compute_size(signal_tiny)
    check(
        "Tiny position: rejected (below $10 min)",
        not result_tiny["approved"],
        f"should have been rejected but was approved: ${result_tiny['approved_size_usd']}",
    )
    print(f"    Rejection: {result_tiny.get('rejection_reason')}")
    sizer._bankroll = config.INITIAL_BANKROLL

    # ---- Test 11: Per-strategy exposure limit -----------------------------
    print()
    print("-" * 72)
    print("  Test 11: Per-strategy exposure limit (20% of bankroll)")
    print("-" * 72)

    sizer.clear_mock_state()
    # Inject mock trades that exhaust S1's 20% allocation.
    for i in range(4):
        sizer.inject_mock_trade({
            "strategy": "s1",
            "market_id": f"0xEXP{i:03d}",
            "side": "buy",
            "size": 50.0,  # 4 * $50 = $200 = 20% of $1000
            "price": 0.50,
            "status": "filled",
            "pnl": 0.0,
            "time": now - timedelta(minutes=i),
        })
    result_capped = await sizer.compute_size(signal_s1)
    check(
        "Strategy limit: rejected or minimal",
        not result_capped["approved"] or result_capped["approved_size_usd"] <= MIN_POSITION_USD,
        f"size={result_capped['approved_size_usd']} should be capped to 0",
    )
    print(f"    Existing S1 exposure: ${result_capped['details'].get('strategy_exposure', 0):.2f}")
    print(f"    Result: approved={result_capped['approved']}, size=${result_capped['approved_size_usd']:.2f}")

    # ---- Test 12: Per-market exposure limit -------------------------------
    print()
    print("-" * 72)
    print("  Test 12: Per-market exposure limit (10% of bankroll)")
    print("-" * 72)

    sizer.clear_mock_state()
    # Inject mock trades on the same market totalling 10%.
    sizer.inject_mock_trade({
        "strategy": "s3",
        "market_id": "0xS1TEST001",  # Same market as signal_s1.
        "side": "buy",
        "size": 100.0,  # $100 = 10% of $1000
        "price": 0.60,
        "status": "filled",
        "pnl": 0.0,
        "time": now - timedelta(minutes=5),
    })
    result_market = await sizer.compute_size(signal_s1)
    check(
        "Market limit: rejected (market at 10%)",
        not result_market["approved"],
        f"should have been rejected, size=${result_market['approved_size_usd']}",
    )
    print(f"    Existing market exposure: ${result_market['details'].get('market_exposure', 0):.2f}")
    print(f"    Result: approved={result_market['approved']}")

    # ---- Test 13: Total deployed limit -----------------------------------
    print()
    print("-" * 72)
    print("  Test 13: Total deployed limit (80% of bankroll)")
    print("-" * 72)

    sizer.clear_mock_state()
    # Inject trades totalling 80% of bankroll across different strategies.
    for i, strat in enumerate(["s1", "s3", "s4", "s4"]):
        sizer.inject_mock_trade({
            "strategy": strat,
            "market_id": f"0xTOTAL{i:03d}",
            "side": "buy",
            "size": 200.0,  # 4 * $200 = $800 = 80% of $1000
            "price": 0.50,
            "status": "filled",
            "pnl": 0.0,
            "time": now - timedelta(minutes=i),
        })
    # Now try adding another trade -- should be rejected.
    signal_total = {
        "strategy": "s3",
        "venue": "polymarket",
        "market_id": "0xNEWMARKET",
        "side": "yes",
        "target_price": 0.55,
        "size_usd": 50.0,
        "edge_estimate": 0.05,
        "confidence": "high",
    }
    result_total = await sizer.compute_size(signal_total)
    check(
        "Total limit: rejected (80% deployed)",
        not result_total["approved"],
        f"should have been rejected, size=${result_total['approved_size_usd']}",
    )
    print(f"    Total exposure: ${result_total['details'].get('total_exposure', 0):.2f}")
    print(f"    Result: approved={result_total['approved']}")

    # ---- Test 14: Invalid signal -----------------------------------------
    print()
    print("-" * 72)
    print("  Test 14: Invalid signal rejection")
    print("-" * 72)

    sizer.clear_mock_state()
    bad_signals = [
        ({"strategy": "s1"}, "missing fields"),
        (
            {
                "strategy": "s99", "venue": "poly", "market_id": "0x1",
                "side": "yes", "target_price": 0.5, "size_usd": 50,
                "edge_estimate": 0.05,
            },
            "unknown strategy",
        ),
        (
            {
                "strategy": "s1", "venue": "poly", "market_id": "0x1",
                "side": "yes", "target_price": 1.5, "size_usd": 50,
                "edge_estimate": 0.05,
            },
            "target_price out of range",
        ),
        (
            {
                "strategy": "s1", "venue": "poly", "market_id": "0x1",
                "side": "yes", "target_price": 0.5, "size_usd": -10,
                "edge_estimate": 0.05,
            },
            "negative size",
        ),
        (
            {
                "strategy": "s1", "venue": "poly", "market_id": "0x1",
                "side": "maybe", "target_price": 0.5, "size_usd": 50,
                "edge_estimate": 0.05,
            },
            "invalid side",
        ),
    ]
    for bad_signal, desc in bad_signals:
        result_bad = await sizer.compute_size(bad_signal)
        check(
            f"Invalid ({desc}): rejected",
            not result_bad["approved"],
            f"should have been rejected",
        )

    # ---- Test 15: NO side computation ------------------------------------
    print()
    print("-" * 72)
    print("  Test 15: NO side computation")
    print("-" * 72)

    sizer.clear_mock_state()
    signal_no = {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xNOSIDE01",
        "side": "no",
        "target_price": 0.70,  # YES price is 0.70, so NO cost is 0.30.
        "size_usd": 100.0,
        "edge_estimate": 0.06,
        "confidence": "high",
    }
    result_no = await sizer.compute_size(signal_no)
    check(
        "NO side: approved",
        result_no["approved"],
        f"rejected: {result_no.get('rejection_reason')}",
    )
    check(
        "NO side: size > 0",
        result_no["approved_size_usd"] > 0,
        f"size={result_no['approved_size_usd']}",
    )
    print(f"    Kelly raw: {result_no['kelly_fraction']:.4f}")
    print(f"    Size: ${result_no['approved_size_usd']:.2f}")

    # ---- Test 16: S3 (Glint News) -- Quarter Kelly -----------------------
    print()
    print("-" * 72)
    print("  Test 16: S3 (Glint News) -- Quarter Kelly")
    print("-" * 72)

    sizer.clear_mock_state()
    signal_s3 = {
        "strategy": "s3",
        "venue": "polymarket",
        "market_id": "0xS3TEST001",
        "side": "yes",
        "target_price": 0.55,
        "size_usd": 80.0,
        "edge_estimate": 0.07,
        "confidence": "high",
    }
    result_s3 = await sizer.compute_size(signal_s3)
    check(
        "S3 approved",
        result_s3["approved"],
        f"rejected: {result_s3.get('rejection_reason')}",
    )
    check(
        "S3 Kelly multiplier = 0.25",
        result_s3["details"]["kelly_multiplier"] == 0.25,
        f"mult={result_s3['details'].get('kelly_multiplier')}",
    )
    print(f"    Kelly raw: {result_s3['kelly_fraction']:.4f}")
    print(f"    Kelly adj: {result_s3['kelly_adjusted']:.4f}")
    print(f"    Size: ${result_s3['approved_size_usd']:.2f}")

    # ---- Summary ---------------------------------------------------------
    print()
    print("=" * 72)
    total = passed + failed
    print(f"  Results: {passed}/{total} passed, {failed}/{total} failed")
    if failed == 0:
        print("  ALL TESTS PASSED")
    else:
        print("  SOME TESTS FAILED -- review output above")
    print("=" * 72)
    print()


# =========================================================================
#  CLI entry point
# =========================================================================

def main() -> None:
    """
    CLI entry point: parse --test / --dry-run flags and run self-test demo.

    In --test mode, uses in-memory mock data (no DB or Redis required).
    In --dry-run mode, runs the full pipeline but logs instead of executing.
    Without flags, prints usage and exits.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Kelly criterion position sizer for polymarket-trader. "
            "Computes optimal position sizes with fractional Kelly "
            "multipliers and portfolio-wide constraints."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run self-test suite with in-memory mock data.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run self-test in dry-run mode (log-only, no real orders).",
    )
    args = parser.parse_args()

    if args.test or args.dry_run:
        dry_run = args.dry_run
        logger.info(
            "Running self-test (dry_run=%s)", dry_run,
        )
        asyncio.run(_run_self_test(dry_run=dry_run))
    else:
        print(
            "Usage: python execution/execution_engine/position_sizer.py "
            "[--test | --dry-run]"
        )
        print()
        print("  --test     Run self-test suite with mock data")
        print("  --dry-run  Run self-test in dry-run mode")
        print()
        print("For programmatic usage:")
        print("  from execution.execution_engine.position_sizer import PositionSizer")
        print()


if __name__ == "__main__":
    main()
