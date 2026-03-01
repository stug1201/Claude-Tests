#!/usr/bin/env python3
"""
risk_manager.py -- Kill switches, drawdown monitoring, and position limits.

Provides the risk gate that every order must pass through before execution.
Monitors rolling 24h drawdown, per-strategy exposure, per-market concentration,
oracle mismatch watchlist, and API health.

Kill switch hierarchy:
    Level 0: INFORMATIONAL   -- log event, continue trading
    Level 1: STRATEGY HALT   -- halt specific strategy, auto-resume after cooldown
    Level 2: VENUE HALT      -- halt all trading on venue, auto-resume when condition clears
    Level 3: GLOBAL HALT     -- halt ALL trading, manual restart required

Kill switch triggers:
    - 24h drawdown > 15%:           Level 3 (GLOBAL HALT, manual restart)
    - Single trade loss > 3%:       Level 1 (halt strategy, 1hr cooldown)
    - 5 consecutive losses:         Level 1 (halt strategy, manual review)
    - Venue API unreachable > 5min: Level 2 (halt venue, auto-resume)
    - Redis connection lost:        Level 2 (halt all strategies, auto-resume)
    - Database connection lost:     Level 2 (halt new trades, auto-resume)

Drawdown response curve:
    0-5%:   Normal operation
    5-10%:  Reduce all position sizes to 50% of computed Kelly
    10-15%: Reduce to 25% of Kelly, halt S2 (highest uncertainty)
    >15%:   GLOBAL HALT (Level 3)

Oracle mismatch watchlist:
    - Monitor cross-venue pair divergence
    - >8c divergence for >30 minutes: quarantine pair, reject S5 signals
    - Remove from quarantine when divergence <3c for >1 hour

Usage:
    from execution.execution_engine.risk_manager import RiskManager

    rm = RiskManager(config, test_mode=True)
    approved, reason = await rm.check_signal(signal)

    python execution/execution_engine/risk_manager.py          # Live monitoring
    python execution/execution_engine/risk_manager.py --test   # Self-test

See: directives/09_risk_management.md
     directives/08_execution_engine.md
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
# Kill switch levels
# ---------------------------------------------------------------------------
LEVEL_INFO = 0
LEVEL_STRATEGY_HALT = 1
LEVEL_VENUE_HALT = 2
LEVEL_GLOBAL_HALT = 3

# ---------------------------------------------------------------------------
# Risk parameter constants (directives 08 / 09)
# ---------------------------------------------------------------------------
MAX_DRAWDOWN_PCT = 0.15             # 15% -- Level 3 trigger
SINGLE_TRADE_LOSS_PCT = 0.03       # 3% of bankroll -- Level 1 trigger
CONSECUTIVE_LOSS_LIMIT = 5          # Level 1 trigger
VENUE_UNREACHABLE_SECONDS = 300     # 5 minutes -- Level 2 trigger
STRATEGY_COOLDOWN_SECONDS = 3600    # 1 hour auto-resume for Level 1

# Portfolio limits
MAX_PER_POSITION_PCT = 0.05         # 5% of bankroll
MAX_PER_STRATEGY_PCT = 0.20         # 20% of bankroll
MAX_PER_MARKET_PCT = 0.10           # 10% of bankroll
MAX_TOTAL_DEPLOYED_PCT = 0.80       # 80% of bankroll
MIN_POSITION_USD = 10.0             # Below this, fees dominate
S5_MAX_PER_PAIR_USD = 500.0         # $500 per cross-venue pair

# Strategy-specific max exposure
STRATEGY_MAX_EXPOSURE: dict[str, float] = {
    "s1": 0.20,
    "s2": 0.15,
    "s3": 0.15,
    "s4": 0.15,
    "s5": 0.15,
}

# Drawdown response curve thresholds
DRAWDOWN_NORMAL_MAX = 0.05
DRAWDOWN_MODERATE_MAX = 0.10
DRAWDOWN_SEVERE_MAX = 0.15

# Oracle mismatch thresholds
ORACLE_DIVERGENCE_THRESHOLD = 0.08   # 8 cents
ORACLE_DIVERGENCE_DURATION = 1800    # 30 minutes in seconds
ORACLE_CONVERGENCE_THRESHOLD = 0.03  # 3 cents
ORACLE_CONVERGENCE_DURATION = 3600   # 1 hour in seconds


# =========================================================================
#  Kill Switch State
# =========================================================================

class KillSwitchState:
    """Tracks the current state of all kill switches."""

    def __init__(self) -> None:
        # Global halt flag -- requires manual restart
        self.global_halt: bool = False
        self.global_halt_reason: str = ""
        self.global_halt_time: Optional[datetime] = None

        # Strategy halts: strategy_name -> (reason, halt_time, auto_resume)
        self.strategy_halts: dict[str, tuple[str, datetime, bool]] = {}

        # Venue halts: venue_name -> (reason, halt_time)
        self.venue_halts: dict[str, tuple[str, datetime]] = {}

        # Consecutive loss tracking: strategy -> count
        self.consecutive_losses: dict[str, int] = {}

    def is_strategy_halted(self, strategy: str) -> tuple[bool, str]:
        """Check if a strategy is currently halted.

        Returns:
            (is_halted, reason) tuple.
        """
        if strategy not in self.strategy_halts:
            return False, ""

        reason, halt_time, auto_resume = self.strategy_halts[strategy]
        if auto_resume:
            elapsed = (datetime.now(timezone.utc) - halt_time).total_seconds()
            if elapsed >= STRATEGY_COOLDOWN_SECONDS:
                del self.strategy_halts[strategy]
                logger.info(
                    "Strategy '%s' auto-resumed after %ds cooldown.",
                    strategy, STRATEGY_COOLDOWN_SECONDS,
                )
                return False, ""
        return True, reason

    def is_venue_halted(self, venue: str) -> tuple[bool, str]:
        """Check if a venue is currently halted."""
        if venue not in self.venue_halts:
            return False, ""
        reason, _ = self.venue_halts[venue]
        return True, reason

    def halt_strategy(self, strategy: str, reason: str, auto_resume: bool = True) -> None:
        """Halt a specific strategy."""
        self.strategy_halts[strategy] = (reason, datetime.now(timezone.utc), auto_resume)
        logger.warning(
            "KILL SWITCH Level 1: Strategy '%s' HALTED. Reason: %s. Auto-resume: %s",
            strategy, reason, auto_resume,
        )

    def halt_venue(self, venue: str, reason: str) -> None:
        """Halt all trading on a venue."""
        self.venue_halts[venue] = (reason, datetime.now(timezone.utc))
        logger.warning(
            "KILL SWITCH Level 2: Venue '%s' HALTED. Reason: %s",
            venue, reason,
        )

    def resume_venue(self, venue: str) -> None:
        """Resume trading on a venue."""
        if venue in self.venue_halts:
            del self.venue_halts[venue]
            logger.info("Venue '%s' resumed.", venue)

    def trigger_global_halt(self, reason: str) -> None:
        """Trigger a global halt (Level 3). Requires manual restart."""
        self.global_halt = True
        self.global_halt_reason = reason
        self.global_halt_time = datetime.now(timezone.utc)
        logger.error(
            "KILL SWITCH Level 3: GLOBAL HALT. Reason: %s. "
            "Manual restart required.",
            reason,
        )

    def record_loss(self, strategy: str) -> None:
        """Record a consecutive loss for a strategy."""
        self.consecutive_losses[strategy] = self.consecutive_losses.get(strategy, 0) + 1

    def record_win(self, strategy: str) -> None:
        """Reset the consecutive loss counter for a strategy."""
        self.consecutive_losses[strategy] = 0

    def get_consecutive_losses(self, strategy: str) -> int:
        return self.consecutive_losses.get(strategy, 0)


# =========================================================================
#  Oracle Quarantine State
# =========================================================================

class OracleQuarantine:
    """Tracks oracle mismatch watchlist and quarantined pairs."""

    def __init__(self) -> None:
        # Divergence tracking: pair_key -> first_divergence_time
        self._divergences: dict[str, datetime] = {}
        # Quarantined pairs: pair_key -> quarantine_time
        self._quarantined: dict[str, datetime] = {}
        # Convergence tracking: pair_key -> first_convergence_time
        self._convergences: dict[str, datetime] = {}

    def is_quarantined(self, pair_key: str) -> bool:
        """Check if a cross-venue pair is quarantined."""
        return pair_key in self._quarantined

    def update_divergence(self, pair_key: str, price_diff: float) -> None:
        """Update divergence tracking for a pair.

        Args:
            pair_key: Unique identifier for the cross-venue pair.
            price_diff: Absolute price difference between venues.
        """
        now = datetime.now(timezone.utc)

        if abs(price_diff) > ORACLE_DIVERGENCE_THRESHOLD:
            # Track divergence start
            if pair_key not in self._divergences:
                self._divergences[pair_key] = now
                logger.info(
                    "Oracle divergence detected: pair=%s diff=%.4f (threshold=%.4f)",
                    pair_key, price_diff, ORACLE_DIVERGENCE_THRESHOLD,
                )
            else:
                elapsed = (now - self._divergences[pair_key]).total_seconds()
                if elapsed >= ORACLE_DIVERGENCE_DURATION and pair_key not in self._quarantined:
                    self._quarantined[pair_key] = now
                    logger.warning(
                        "ORACLE QUARANTINE: pair=%s diverged by %.4f for %ds. "
                        "S5 signals on this pair will be rejected.",
                        pair_key, price_diff, int(elapsed),
                    )
            # Clear convergence tracking
            self._convergences.pop(pair_key, None)

        elif abs(price_diff) < ORACLE_CONVERGENCE_THRESHOLD:
            # Track convergence start
            self._divergences.pop(pair_key, None)

            if pair_key in self._quarantined:
                if pair_key not in self._convergences:
                    self._convergences[pair_key] = now
                else:
                    elapsed = (now - self._convergences[pair_key]).total_seconds()
                    if elapsed >= ORACLE_CONVERGENCE_DURATION:
                        del self._quarantined[pair_key]
                        del self._convergences[pair_key]
                        logger.info(
                            "Oracle quarantine lifted: pair=%s converged within %.4f "
                            "for %ds.",
                            pair_key, ORACLE_CONVERGENCE_THRESHOLD, int(elapsed),
                        )
        else:
            # Between thresholds -- clear divergence tracking but keep quarantine
            self._divergences.pop(pair_key, None)
            self._convergences.pop(pair_key, None)

    def get_quarantined_pairs(self) -> list[str]:
        """Return list of currently quarantined pair keys."""
        return list(self._quarantined.keys())


# =========================================================================
#  RiskManager class
# =========================================================================

class RiskManager:
    """
    Real-time risk management gate for the polymarket-trader execution engine.

    Every trade signal must pass through check_signal() before execution.
    The risk manager enforces:
      - Kill switch hierarchy (Levels 0-3)
      - Position limits (per-strategy, per-market, total deployed)
      - Rolling 24h drawdown monitoring with response curve
      - Oracle mismatch quarantine for S5 cross-venue signals
      - Duplicate signal deduplication

    Thread-safe: all mutable state access is serialised through an
    asyncio.Lock (directive 09: "Concurrent risk checks").

    Args:
        config:    Config singleton (from execution.utils.config).
        test_mode: If True, use in-memory mock data for DB queries.
    """

    def __init__(
        self,
        config: Any = None,
        test_mode: bool = False,
    ) -> None:
        self._config = config
        self._test_mode = test_mode
        self._lock = asyncio.Lock()

        # Kill switch state
        self._kill_switches = KillSwitchState()

        # Oracle quarantine
        self._oracle = OracleQuarantine()

        # Bankroll tracking
        if config is not None:
            self._initial_bankroll: float = config.INITIAL_BANKROLL
        else:
            self._initial_bankroll = 1000.0
        self._current_bankroll: float = self._initial_bankroll

        # In-memory trade tracking for test mode
        self._mock_trades: list[dict] = []

        # Venue balances (tracked separately per directive 08)
        self._venue_balances: dict[str, float] = {
            "polymarket": self._initial_bankroll / 2.0,
            "kalshi": self._initial_bankroll / 2.0,
        }

        # Infrastructure health tracking
        self._venue_last_healthy: dict[str, datetime] = {}
        self._redis_healthy: bool = True
        self._db_healthy: bool = True

        mode_label = "TEST" if test_mode else "LIVE"
        logger.info(
            "RiskManager initialised: bankroll=$%.2f, mode=%s",
            self._initial_bankroll, mode_label,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_signal(self, signal: dict) -> tuple[bool, str]:
        """
        Validate a trade signal against all risk checks.

        This is the primary gate function. Every signal must pass through
        this before order placement.

        Args:
            signal: Trade signal dict with keys: strategy, venue, market_id,
                    side, target_price, size_usd, edge_estimate, confidence.

        Returns:
            Tuple of (approved: bool, reason: str).
            If approved, reason is "approved".
            If rejected, reason describes why.
        """
        async with self._lock:
            return await self._check_signal_locked(signal)

    async def update_on_fill(
        self,
        strategy: str,
        venue: str,
        market_id: str,
        side: str,
        fill_price: float,
        size_usd: float,
        pnl: float = 0.0,
    ) -> None:
        """
        Update risk state after a trade fill.

        Called by the order router after a trade is filled. Updates
        bankroll, consecutive loss tracking, and checks for kill switch
        triggers.

        Args:
            strategy:   Strategy label.
            venue:      Execution venue.
            market_id:  Market identifier.
            side:       Trade side.
            fill_price: Actual fill price.
            size_usd:   Position size in USD.
            pnl:        Realised PnL (positive = profit, negative = loss).
        """
        async with self._lock:
            await self._update_on_fill_locked(
                strategy, venue, market_id, side, fill_price, size_usd, pnl,
            )

    async def record_fill(self, signal: dict, fill_price: float, size_usd: float) -> None:
        """Convenience method matching the interface expected by OrderRouter."""
        await self.update_on_fill(
            strategy=signal.get("strategy", ""),
            venue=signal.get("venue", ""),
            market_id=signal.get("market_id", ""),
            side=signal.get("side", ""),
            fill_price=fill_price,
            size_usd=size_usd,
        )

    def get_drawdown_multiplier(self, drawdown_pct: float) -> float:
        """
        Return the sizing multiplier based on the drawdown response curve.

        Args:
            drawdown_pct: Current 24h rolling drawdown as a fraction (0.0-1.0).

        Returns:
            Multiplier to apply to position sizes (1.0, 0.5, 0.25, or 0.0).
        """
        if drawdown_pct <= DRAWDOWN_NORMAL_MAX:
            return 1.0
        elif drawdown_pct <= DRAWDOWN_MODERATE_MAX:
            return 0.5
        elif drawdown_pct <= DRAWDOWN_SEVERE_MAX:
            return 0.25
        else:
            return 0.0

    def get_kill_switch_state(self) -> dict:
        """Return a summary of all active kill switches."""
        return {
            "global_halt": self._kill_switches.global_halt,
            "global_halt_reason": self._kill_switches.global_halt_reason,
            "strategy_halts": {
                k: v[0] for k, v in self._kill_switches.strategy_halts.items()
            },
            "venue_halts": {
                k: v[0] for k, v in self._kill_switches.venue_halts.items()
            },
            "quarantined_pairs": self._oracle.get_quarantined_pairs(),
        }

    # ------------------------------------------------------------------
    # Infrastructure health reporting
    # ------------------------------------------------------------------

    def report_venue_health(self, venue: str, healthy: bool) -> None:
        """
        Report API health for a venue.

        When healthy, records the last-seen timestamp.  When unhealthy
        for > 5 minutes (VENUE_UNREACHABLE_SECONDS), triggers a Level 2
        (VENUE HALT) kill switch.  Auto-resumes when health is restored.

        Args:
            venue:   Venue name (e.g. 'polymarket', 'kalshi').
            healthy: Whether the venue API is currently reachable.
        """
        now = datetime.now(timezone.utc)

        if healthy:
            self._venue_last_healthy[venue] = now
            # Clear any active venue halt for this venue
            if venue in self._kill_switches.venue_halts:
                self._kill_switches.resume_venue(venue)
        else:
            last_healthy = self._venue_last_healthy.get(venue)
            if last_healthy is not None:
                elapsed = (now - last_healthy).total_seconds()
                if elapsed >= VENUE_UNREACHABLE_SECONDS:
                    halted, _ = self._kill_switches.is_venue_halted(venue)
                    if not halted:
                        self._kill_switches.halt_venue(
                            venue,
                            f"API unreachable for >{VENUE_UNREACHABLE_SECONDS}s",
                        )
            else:
                # First unhealthy report -- record the start time
                self._venue_last_healthy[venue] = now

    def report_redis_health(self, healthy: bool) -> None:
        """
        Report Redis connection health.

        If Redis is lost, triggers Level 2 halt for all strategies.
        Auto-resumes when Redis recovers.
        """
        prev = self._redis_healthy
        self._redis_healthy = healthy

        if not healthy and prev:
            # Just went unhealthy
            self._kill_switches.halt_venue(
                "__redis__", "Redis connection lost"
            )
        elif healthy and not prev:
            # Recovered
            self._kill_switches.resume_venue("__redis__")
            logger.info("Redis recovered -- halt cleared.")

    def report_db_health(self, healthy: bool) -> None:
        """
        Report database connection health.

        If DB is lost, triggers Level 2 halt for new trades.
        Auto-resumes when DB recovers.
        """
        prev = self._db_healthy
        self._db_healthy = healthy

        if not healthy and prev:
            self._kill_switches.halt_venue(
                "__database__", "Database connection lost -- new trades halted"
            )
        elif healthy and not prev:
            self._kill_switches.resume_venue("__database__")
            logger.info("Database recovered -- halt cleared.")

    # ------------------------------------------------------------------
    # Status / reporting
    # ------------------------------------------------------------------

    async def get_status(self) -> dict:
        """
        Return a comprehensive risk status snapshot.

        Includes bankroll, drawdown, exposure, active kill switches,
        quarantined pairs, and infrastructure health.
        """
        drawdown_pct = await self._calculate_drawdown()
        dd_multiplier = self.get_drawdown_multiplier(drawdown_pct)

        strategy_exposures: dict[str, float] = {}
        for strat in ["s1", "s2", "s3", "s4", "s5"]:
            strategy_exposures[strat] = await self._get_strategy_exposure(strat)

        total_exposure = await self._get_total_exposure()

        return {
            "initial_bankroll": self._initial_bankroll,
            "current_bankroll": self._current_bankroll,
            "venue_balances": dict(self._venue_balances),
            "drawdown_24h_pct": round(drawdown_pct * 100, 2),
            "drawdown_multiplier": dd_multiplier,
            "halt_s2": drawdown_pct > DRAWDOWN_MODERATE_MAX,
            "total_exposure_usd": total_exposure,
            "exposure_by_strategy": strategy_exposures,
            "kill_switches": self.get_kill_switch_state(),
            "quarantined_pairs": self._oracle.get_quarantined_pairs(),
            "consecutive_losses": dict(self._kill_switches.consecutive_losses),
            "redis_healthy": self._redis_healthy,
            "db_healthy": self._db_healthy,
        }

    # ------------------------------------------------------------------
    # Internal: main risk check (called under lock)
    # ------------------------------------------------------------------

    async def _check_signal_locked(self, signal: dict) -> tuple[bool, str]:
        """Core risk check logic -- must be called while holding self._lock."""

        strategy = signal.get("strategy", "").lower()
        venue = signal.get("venue", "")
        market_id = signal.get("market_id", "")
        size_usd = float(signal.get("size_usd", 0))

        # -- Check 1: Global halt -------------------------------------------
        if self._kill_switches.global_halt:
            reason = (
                f"GLOBAL HALT active: {self._kill_switches.global_halt_reason}. "
                "Manual restart required."
            )
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 1b: Redis / DB infrastructure halts ----------------------
        redis_halted, redis_reason = self._kill_switches.is_venue_halted("__redis__")
        if redis_halted:
            reason = f"Infrastructure halt: {redis_reason}"
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        db_halted, db_reason = self._kill_switches.is_venue_halted("__database__")
        if db_halted:
            reason = f"Infrastructure halt: {db_reason}"
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 1c: Minimum position size --------------------------------
        if size_usd < MIN_POSITION_USD:
            reason = (
                f"Position size ${size_usd:.2f} below minimum "
                f"${MIN_POSITION_USD:.2f} -- fees would dominate"
            )
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 2: Refresh drawdown and check threshold ------------------
        drawdown_pct = await self._calculate_drawdown()

        if drawdown_pct > MAX_DRAWDOWN_PCT:
            self._kill_switches.trigger_global_halt(
                f"24h drawdown {drawdown_pct:.1%} exceeds {MAX_DRAWDOWN_PCT:.0%}"
            )
            return False, self._kill_switches.global_halt_reason

        # -- Check 3: S2 halt at severe drawdown (10-15%) -------------------
        if strategy == "s2" and drawdown_pct > DRAWDOWN_MODERATE_MAX:
            reason = (
                f"S2 halted: drawdown {drawdown_pct:.1%} exceeds "
                f"{DRAWDOWN_MODERATE_MAX:.0%} -- S2 (Insider Detection) "
                "halted at this drawdown level"
            )
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 4: Strategy halt -----------------------------------------
        halted, halt_reason = self._kill_switches.is_strategy_halted(strategy)
        if halted:
            reason = f"Strategy '{strategy}' is halted: {halt_reason}"
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 5: Venue halt --------------------------------------------
        # For cross-venue (S5), check both venues
        if venue == "cross":
            for v in ("polymarket", "kalshi"):
                v_halted, v_reason = self._kill_switches.is_venue_halted(v)
                if v_halted:
                    reason = f"Venue '{v}' is halted: {v_reason}"
                    logger.warning("Signal REJECTED: %s", reason)
                    return False, reason
        else:
            v_halted, v_reason = self._kill_switches.is_venue_halted(venue)
            if v_halted:
                reason = f"Venue '{venue}' is halted: {v_reason}"
                logger.warning("Signal REJECTED: %s", reason)
                return False, reason

        # -- Check 6: Oracle quarantine (S5 only) ---------------------------
        if strategy == "s5":
            pair_key = market_id  # S5 market_id is "poly_id|kalshi_id"
            if self._oracle.is_quarantined(pair_key):
                reason = (
                    f"S5 signal rejected: pair '{pair_key}' is quarantined "
                    "due to oracle divergence"
                )
                logger.warning("Signal REJECTED: %s", reason)
                return False, reason

        # -- Check 7: Per-strategy exposure limit ---------------------------
        bankroll = self._current_bankroll
        strategy_exposure = await self._get_strategy_exposure(strategy)
        strategy_max_pct = STRATEGY_MAX_EXPOSURE.get(strategy, MAX_PER_STRATEGY_PCT)
        strategy_max_usd = bankroll * strategy_max_pct

        if strategy_exposure + size_usd > strategy_max_usd:
            reason = (
                f"Per-strategy limit exceeded: strategy={strategy} "
                f"current=${strategy_exposure:.2f} + new=${size_usd:.2f} "
                f"> max=${strategy_max_usd:.2f} ({strategy_max_pct:.0%} of bankroll)"
            )
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 8: Per-market exposure limit -----------------------------
        market_exposure = await self._get_market_exposure(market_id)
        market_max_usd = bankroll * MAX_PER_MARKET_PCT

        if market_exposure + size_usd > market_max_usd:
            reason = (
                f"Per-market limit exceeded: market={market_id} "
                f"current=${market_exposure:.2f} + new=${size_usd:.2f} "
                f"> max=${market_max_usd:.2f} ({MAX_PER_MARKET_PCT:.0%} of bankroll)"
            )
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 9: Total deployed limit ----------------------------------
        total_exposure = await self._get_total_exposure()
        total_max_usd = bankroll * MAX_TOTAL_DEPLOYED_PCT

        if total_exposure + size_usd > total_max_usd:
            reason = (
                f"Total deployed limit exceeded: "
                f"current=${total_exposure:.2f} + new=${size_usd:.2f} "
                f"> max=${total_max_usd:.2f} ({MAX_TOTAL_DEPLOYED_PCT:.0%} of bankroll)"
            )
            logger.warning("Signal REJECTED: %s", reason)
            return False, reason

        # -- Check 10: S5 cross-venue pair cap ($500) -----------------------
        if strategy == "s5":
            pair_exposure = await self._get_pair_exposure(market_id)
            if pair_exposure + size_usd > S5_MAX_PER_PAIR_USD:
                reason = (
                    f"S5 per-pair limit exceeded: pair={market_id} "
                    f"current=${pair_exposure:.2f} + new=${size_usd:.2f} "
                    f"> max=${S5_MAX_PER_PAIR_USD:.2f}"
                )
                logger.warning("Signal REJECTED: %s", reason)
                return False, reason

        # -- All checks passed ----------------------------------------------
        logger.info(
            "Signal APPROVED: strategy=%s venue=%s market=%s size=$%.2f "
            "drawdown=%.1f%%",
            strategy, venue, market_id[:20], size_usd, drawdown_pct * 100,
        )
        return True, "approved"

    # ------------------------------------------------------------------
    # Internal: fill processing (called under lock)
    # ------------------------------------------------------------------

    async def _update_on_fill_locked(
        self,
        strategy: str,
        venue: str,
        market_id: str,
        side: str,
        fill_price: float,
        size_usd: float,
        pnl: float,
    ) -> None:
        """Process a fill event and update risk state."""

        now = datetime.now(timezone.utc)

        # Record trade for test mode
        if self._test_mode:
            self._mock_trades.append({
                "strategy": strategy,
                "venue": venue,
                "market_id": market_id,
                "side": side,
                "price": fill_price,
                "size": size_usd,
                "pnl": pnl,
                "status": "filled",
                "time": now,
            })

        # Update bankroll
        self._current_bankroll += pnl
        logger.info(
            "Fill recorded: strategy=%s pnl=$%.2f bankroll=$%.2f",
            strategy, pnl, self._current_bankroll,
        )

        # Track consecutive losses
        if pnl < 0:
            self._kill_switches.record_loss(strategy)
            consecutive = self._kill_switches.get_consecutive_losses(strategy)
            logger.info(
                "Consecutive losses for '%s': %d/%d",
                strategy, consecutive, CONSECUTIVE_LOSS_LIMIT,
            )

            # Check single trade loss trigger (>3% of bankroll)
            if abs(pnl) > self._initial_bankroll * SINGLE_TRADE_LOSS_PCT:
                self._kill_switches.halt_strategy(
                    strategy,
                    f"Single trade loss ${abs(pnl):.2f} exceeds "
                    f"{SINGLE_TRADE_LOSS_PCT:.0%} of bankroll",
                    auto_resume=True,
                )

            # Check consecutive loss trigger (5 in a row)
            if consecutive >= CONSECUTIVE_LOSS_LIMIT:
                self._kill_switches.halt_strategy(
                    strategy,
                    f"{CONSECUTIVE_LOSS_LIMIT} consecutive losses",
                    auto_resume=False,  # manual review required
                )
        else:
            self._kill_switches.record_win(strategy)

        # Check bankroll health
        if self._current_bankroll <= 0:
            self._kill_switches.trigger_global_halt(
                f"Bankroll is negative: ${self._current_bankroll:.2f}. "
                "This indicates a bug in position sizing or risk management."
            )

        # Check drawdown
        drawdown_pct = await self._calculate_drawdown()
        if drawdown_pct > MAX_DRAWDOWN_PCT:
            self._kill_switches.trigger_global_halt(
                f"24h drawdown {drawdown_pct:.1%} exceeds {MAX_DRAWDOWN_PCT:.0%}"
            )

    # ------------------------------------------------------------------
    # Internal: drawdown calculation
    # ------------------------------------------------------------------

    async def _calculate_drawdown(self) -> float:
        """
        Calculate the rolling 24h max drawdown as a percentage of bankroll.

        Uses the algorithm from directive 09:
            cumulative PnL curve -> peak -> max drawdown from peak.
        """
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
                logger.warning("Failed to query trades for drawdown: %s", exc)
                return 0.0

        if not recent_trades:
            return 0.0

        # Sort by time ascending
        recent_trades.sort(key=lambda t: t.get("time", now))

        cumulative_pnl = 0.0
        peak_pnl = 0.0
        max_drawdown = 0.0

        for trade in recent_trades:
            cumulative_pnl += trade.get("pnl", 0.0)
            peak_pnl = max(peak_pnl, cumulative_pnl)
            drawdown = peak_pnl - cumulative_pnl
            max_drawdown = max(max_drawdown, drawdown)

        bankroll = self._current_bankroll if self._current_bankroll > 0 else 1.0
        return max_drawdown / bankroll

    # ------------------------------------------------------------------
    # Internal: exposure queries
    # ------------------------------------------------------------------

    async def _get_strategy_exposure(self, strategy: str) -> float:
        """Return total USD in open positions for this strategy."""
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("strategy") == strategy
                and t.get("status") in ("pending", "filled")
            )
        try:
            from execution.utils.db import get_trades
            pending = await get_trades(strategy=strategy, status="pending")
            filled = await get_trades(strategy=strategy, status="filled")
            return sum(t.get("size", 0.0) for t in pending + filled)
        except Exception as exc:
            logger.warning("Failed to query strategy exposure: %s", exc)
            return 0.0

    async def _get_market_exposure(self, market_id: str) -> float:
        """Return total USD in open positions on this market across strategies."""
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("market_id") == market_id
                and t.get("status") in ("pending", "filled")
            )
        try:
            from execution.utils.db import get_trades
            pending = await get_trades(status="pending")
            filled = await get_trades(status="filled")
            return sum(
                t.get("size", 0.0) for t in pending + filled
                if t.get("market_id") == market_id
            )
        except Exception as exc:
            logger.warning("Failed to query market exposure: %s", exc)
            return 0.0

    async def _get_total_exposure(self) -> float:
        """Return total USD in all open positions."""
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("status") in ("pending", "filled")
            )
        try:
            from execution.utils.db import get_trades
            pending = await get_trades(status="pending")
            filled = await get_trades(status="filled")
            return sum(t.get("size", 0.0) for t in pending + filled)
        except Exception as exc:
            logger.warning("Failed to query total exposure: %s", exc)
            return 0.0

    async def _get_pair_exposure(self, pair_key: str) -> float:
        """Return total USD exposure for a cross-venue pair (S5)."""
        if self._test_mode:
            return sum(
                t.get("size", 0.0) for t in self._mock_trades
                if t.get("market_id") == pair_key
                and t.get("strategy") == "s5"
                and t.get("status") in ("pending", "filled")
            )
        try:
            from execution.utils.db import get_trades
            pending = await get_trades(strategy="s5", status="pending")
            filled = await get_trades(strategy="s5", status="filled")
            return sum(
                t.get("size", 0.0) for t in pending + filled
                if t.get("market_id") == pair_key
            )
        except Exception as exc:
            logger.warning("Failed to query pair exposure: %s", exc)
            return 0.0

    # ------------------------------------------------------------------
    # Test helpers
    # ------------------------------------------------------------------

    def inject_mock_trade(self, trade: dict) -> None:
        """Add a mock trade for test-mode exposure calculations."""
        if not self._test_mode:
            return
        self._mock_trades.append(trade)

    def clear_mock_state(self) -> None:
        """Reset all in-memory state for a clean test."""
        self._mock_trades.clear()
        self._current_bankroll = self._initial_bankroll
        self._kill_switches = KillSwitchState()
        self._oracle = OracleQuarantine()
        self._venue_last_healthy.clear()
        self._redis_healthy = True
        self._db_healthy = True


# =========================================================================
#  Self-test suite
# =========================================================================

async def _run_self_test() -> None:
    """
    Exercise all RiskManager logic with synthetic signals.

    Tests:
        1. Normal signal approval
        2. Global halt at >15% drawdown
        3. Strategy halt on single trade loss >3%
        4. Strategy halt on 5 consecutive losses
        5. Per-strategy exposure limit (20%)
        6. Per-market exposure limit (10%)
        7. Total deployed limit (80%)
        8. S5 per-pair cap ($500)
        9. S5 oracle quarantine
        10. S2 halt at 10-15% drawdown
        11. Venue halt
        12. Drawdown response curve multipliers
    """
    class MockConfig:
        INITIAL_BANKROLL = 1000.0
        MAX_DRAWDOWN_PCT = 0.15

    config = MockConfig()
    rm = RiskManager(config, test_mode=True)

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
    print("  Risk Manager -- Self-Test Suite")
    print("  Bankroll: $%.2f" % config.INITIAL_BANKROLL)
    print("=" * 72)

    # Base signal for testing
    base_signal = {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xTEST001",
        "side": "yes",
        "target_price": 0.60,
        "size_usd": 30.0,
        "edge_estimate": 0.05,
        "confidence": "high",
    }

    # ---- Test 1: Normal approval -----------------------------------------
    print()
    print("-" * 72)
    print("  Test 1: Normal signal approval")
    print("-" * 72)

    approved, reason = await rm.check_signal(base_signal)
    check("Normal signal approved", approved, f"reason={reason}")
    check("Reason is 'approved'", reason == "approved", f"got '{reason}'")

    # ---- Test 2: Global halt on >15% drawdown ----------------------------
    print()
    print("-" * 72)
    print("  Test 2: Global halt at >15% drawdown")
    print("-" * 72)

    rm.clear_mock_state()
    # Inject losing trades to push drawdown over 15%
    for i in range(4):
        rm.inject_mock_trade({
            "strategy": "s1", "venue": "polymarket",
            "market_id": f"0xLOSS{i:03d}", "side": "yes",
            "price": 0.60, "size": 50.0, "pnl": -50.0,
            "status": "filled", "time": now - timedelta(minutes=i * 5),
        })
    rm._current_bankroll = 1000.0 - 200.0  # $200 lost

    approved, reason = await rm.check_signal(base_signal)
    check("Signal rejected at high drawdown", not approved, f"approved={approved}")
    check("Global halt triggered", rm._kill_switches.global_halt, "should be True")

    # ---- Test 3: Strategy halt on single trade loss >3% ------------------
    print()
    print("-" * 72)
    print("  Test 3: Strategy halt on single trade loss >3%")
    print("-" * 72)

    rm.clear_mock_state()
    # Record a fill with >3% loss
    await rm.update_on_fill(
        strategy="s1", venue="polymarket", market_id="0xBIGLOSS",
        side="yes", fill_price=0.60, size_usd=50.0,
        pnl=-40.0,  # $40 loss = 4% of $1000 bankroll
    )
    halted, halt_reason = rm._kill_switches.is_strategy_halted("s1")
    check("Strategy halted after big loss", halted, f"halted={halted}")
    check("Halt reason mentions single trade", "Single trade" in halt_reason, f"reason={halt_reason}")

    # Verify signal is rejected
    approved, reason = await rm.check_signal(base_signal)
    check("Signal rejected when strategy halted", not approved, f"approved={approved}")

    # ---- Test 4: Strategy halt on 5 consecutive losses -------------------
    print()
    print("-" * 72)
    print("  Test 4: Strategy halt on 5 consecutive losses")
    print("-" * 72)

    rm.clear_mock_state()
    for i in range(5):
        await rm.update_on_fill(
            strategy="s3", venue="polymarket", market_id=f"0xLOSS{i}",
            side="yes", fill_price=0.50, size_usd=10.0,
            pnl=-5.0,  # Small losses (< 3% threshold)
        )
    halted, halt_reason = rm._kill_switches.is_strategy_halted("s3")
    check("Strategy halted after 5 losses", halted, f"halted={halted}")
    check("Halt reason mentions consecutive", "consecutive" in halt_reason.lower(), f"reason={halt_reason}")

    # ---- Test 5: Per-strategy exposure limit (20%) -----------------------
    print()
    print("-" * 72)
    print("  Test 5: Per-strategy exposure limit")
    print("-" * 72)

    rm.clear_mock_state()
    # Fill up S1 to exactly 20% = $200
    for i in range(4):
        rm.inject_mock_trade({
            "strategy": "s1", "venue": "polymarket",
            "market_id": f"0xEXP{i:03d}", "side": "yes",
            "price": 0.50, "size": 50.0, "pnl": 0.0,
            "status": "filled", "time": now - timedelta(minutes=i),
        })

    signal_over = dict(base_signal, size_usd=10.0)  # Would exceed 20%
    approved, reason = await rm.check_signal(signal_over)
    check("Rejected at strategy limit", not approved, f"approved={approved}")
    check("Reason mentions per-strategy", "per-strategy" in reason.lower() or "strategy" in reason.lower(), f"reason={reason}")

    # ---- Test 6: Per-market exposure limit (10%) -------------------------
    print()
    print("-" * 72)
    print("  Test 6: Per-market exposure limit")
    print("-" * 72)

    rm.clear_mock_state()
    rm.inject_mock_trade({
        "strategy": "s3", "venue": "polymarket",
        "market_id": "0xTEST001", "side": "yes",
        "price": 0.50, "size": 100.0, "pnl": 0.0,
        "status": "filled", "time": now - timedelta(minutes=1),
    })

    approved, reason = await rm.check_signal(base_signal)
    check("Rejected at market limit", not approved, f"approved={approved}")
    check("Reason mentions market", "market" in reason.lower(), f"reason={reason}")

    # ---- Test 7: Total deployed limit (80%) ------------------------------
    print()
    print("-" * 72)
    print("  Test 7: Total deployed limit (80%)")
    print("-" * 72)

    rm.clear_mock_state()
    # Spread exposure across many strategies and markets to approach 80%
    # without hitting per-strategy (20%) or per-market (10%) limits.
    # $1000 * 80% = $800 total limit.
    # Use 8 strategies (with varying names) x different markets.
    # S1: $190 across 2 markets (under $200 strategy limit)
    # S2: $140 across 2 markets (under $150 strategy limit for s2)
    # S3: $140 across 2 markets (under $150 strategy limit for s3)
    # S4: $140 across 2 markets (under $150 strategy limit for s4)
    # S5: $140 across 2 markets (under $150 strategy limit for s5)
    # Total: $750
    spread_data = [
        ("s1", 95.0), ("s1", 95.0),
        ("s2", 70.0), ("s2", 70.0),
        ("s3", 70.0), ("s3", 70.0),
        ("s4", 70.0), ("s4", 70.0),
        ("s5", 70.0), ("s5", 70.0),
    ]
    for idx, (strat, size) in enumerate(spread_data):
        rm.inject_mock_trade({
            "strategy": strat, "venue": "polymarket",
            "market_id": f"0xTOTAL_{strat}_{idx}", "side": "yes",
            "price": 0.50, "size": size, "pnl": 0.0,
            "status": "filled", "time": now - timedelta(minutes=idx),
        })
    # Total: 190 + 140*4 = 750.  s1 at $190 (under $200 limit).
    # Signal for s1 with size=$10 -> s1 would be $200 exactly (ok),
    # but total would be $760 (under $800). So use a slightly larger size.
    # s1 at $190 + $10 = $200 is exactly AT the limit, not over.
    # Need total to exceed: 750 + X > 800, so X > 50.
    # But s1 at 190 + 51 = 241 > 200 (strategy limit!).
    # Solution: use a strategy that has more headroom.
    # s2 is at $140, limit is $150. Signal size=11 -> s2=$151 > $150.
    # Use s1 at $190, signal size=10 -> s1=$200 (exactly at limit, not over).
    # Total = 750 + 10 = 760 (under 800). So the signal should PASS.
    # We need total 750 + X > 800 while X + strategy_exposure <= strategy_limit.
    # Per-strategy headroom: s1 has 10, s2-s5 each have 10.
    # Best approach: make total much closer to 800.
    # Let's increase totals to 795.
    rm._mock_trades.clear()
    # Fill up to $795 total, with no single strategy > its limit
    spread_data2 = [
        ("s1", 99.0), ("s1", 99.0),   # s1: $198 (limit $200)
        ("s2", 74.0), ("s2", 74.0),   # s2: $148 (limit $150)
        ("s3", 74.0), ("s3", 74.0),   # s3: $148 (limit $150)
        ("s4", 74.0), ("s4", 74.0),   # s4: $148 (limit $150)
    ]
    # Total: 198 + 148*3 = 198 + 444 = 642. Need more.
    # Add s5 pairs to fill up.
    # s5 limit: 15% of $1000 = $150
    spread_data2 += [("s5", 74.0), ("s5", 74.0)]  # s5: $148
    # Total: 642 + 148 = 790
    for idx, (strat, size) in enumerate(spread_data2):
        rm.inject_mock_trade({
            "strategy": strat, "venue": "polymarket",
            "market_id": f"0xTOTAL_{idx:03d}", "side": "yes",
            "price": 0.50, "size": size, "pnl": 0.0,
            "status": "filled", "time": now - timedelta(minutes=idx),
        })
    # Total: 790. s1 at 198. Signal: s1, size=2 -> s1=200 (ok), total=792 (ok).
    # Signal: s1, size=11 -> s1=209 > 200 (per-strategy fires first!).
    # Need to pick a strategy with headroom and push total over 800.
    # s1 headroom: 200-198=2. s2: 150-148=2. etc. All have 2 of headroom.
    # We need size > 10 to exceed total but <= 2 for per-strategy.
    # This is impossible with these parameters: we can't have size > 10 and <= 2.
    # The per-strategy and per-market checks fire first at this bankroll.
    # Best approach: use a higher bankroll where total limit fires first.
    rm._mock_trades.clear()
    rm._initial_bankroll = 10000.0
    rm._current_bankroll = 10000.0
    # $10000 * 80% = $8000 total limit.
    # Per-strategy s1: 20% = $2000. Per-market: 10% = $1000.
    # Fill up total to $7990 across strategies.
    for idx in range(8):
        strat = ["s1", "s2", "s3", "s4"][idx % 4]
        rm.inject_mock_trade({
            "strategy": strat, "venue": "polymarket",
            "market_id": f"0xTOTAL_{idx:03d}", "side": "yes",
            "price": 0.50, "size": 999.0, "pnl": 0.0,
            "status": "filled", "time": now - timedelta(minutes=idx),
        })
    # Total: 8 * 999 = 7992. Per-strategy: s1=$1998, s2=$1998, s3=$1998, s4=$1998
    # Signal: s1, new market, size=$20 -> s1=$2018 > $2000 (per-strategy fires!).
    # Use s5 which has 0 exposure:
    signal_total = {
        "strategy": "s5",
        "venue": "cross",
        "market_id": "0xNEW_TOTAL_TEST",
        "side": "yes",
        "target_price": 0.50,
        "size_usd": 20.0,  # Total: 7992 + 20 = 8012 > 8000
        "edge_estimate": 0.04,
        "confidence": "high",
    }
    approved, reason = await rm.check_signal(signal_total)
    check("Rejected at total limit", not approved, f"approved={approved}")
    check("Reason mentions total", "total" in reason.lower(), f"reason={reason}")

    # Restore
    rm._initial_bankroll = 1000.0
    rm._current_bankroll = 1000.0

    # ---- Test 8: S5 per-pair cap ($500) ----------------------------------
    print()
    print("-" * 72)
    print("  Test 8: S5 per-pair cap ($500)")
    print("-" * 72)

    rm.clear_mock_state()
    # Use a large bankroll so per-strategy limit (15% = $7500) doesn't
    # interfere with the per-pair cap ($500) test.
    rm._initial_bankroll = 50000.0
    rm._current_bankroll = 50000.0

    rm.inject_mock_trade({
        "strategy": "s5", "venue": "cross",
        "market_id": "0xPOLY|KXKALSHI", "side": "yes|no",
        "price": 0.50, "size": 480.0, "pnl": 0.0,
        "status": "filled", "time": now - timedelta(minutes=1),
    })

    s5_signal = {
        "strategy": "s5",
        "venue": "cross",
        "market_id": "0xPOLY|KXKALSHI",
        "side": "yes|no",
        "target_price": 0.50,
        "size_usd": 30.0,  # Would push over $500
        "edge_estimate": 0.04,
        "confidence": "high",
    }
    approved, reason = await rm.check_signal(s5_signal)
    check("S5 rejected at pair cap", not approved, f"approved={approved}")
    check("Reason mentions pair", "pair" in reason.lower(), f"reason={reason}")

    # Restore normal bankroll for subsequent tests
    rm._initial_bankroll = 1000.0
    rm._current_bankroll = 1000.0

    # ---- Test 9: S5 oracle quarantine ------------------------------------
    print()
    print("-" * 72)
    print("  Test 9: S5 oracle quarantine")
    print("-" * 72)

    rm.clear_mock_state()
    pair_key = "0xPOLY|KXKALSHI"
    # Directly quarantine the pair
    rm._oracle._quarantined[pair_key] = now

    s5_signal2 = dict(s5_signal, size_usd=20.0)
    approved, reason = await rm.check_signal(s5_signal2)
    check("S5 rejected when quarantined", not approved, f"approved={approved}")
    check("Reason mentions quarantine", "quarantine" in reason.lower(), f"reason={reason}")

    # ---- Test 10: S2 halt at 10-15% drawdown -----------------------------
    print()
    print("-" * 72)
    print("  Test 10: S2 halt at 10-15% drawdown")
    print("-" * 72)

    rm.clear_mock_state()
    # Inject trades to create ~12% drawdown
    for i in range(3):
        rm.inject_mock_trade({
            "strategy": "s4", "venue": "polymarket",
            "market_id": f"0xDD{i:03d}", "side": "yes",
            "price": 0.50, "size": 50.0, "pnl": -40.0,
            "status": "filled", "time": now - timedelta(minutes=i * 5),
        })

    s2_signal = {
        "strategy": "s2",
        "venue": "polymarket",
        "market_id": "0xS2TEST",
        "side": "yes",
        "target_price": 0.50,
        "size_usd": 20.0,
        "edge_estimate": 0.05,
        "confidence": "medium",
    }
    approved, reason = await rm.check_signal(s2_signal)
    check("S2 rejected at moderate drawdown", not approved, f"approved={approved}")
    check("Reason mentions S2", "S2" in reason or "s2" in reason, f"reason={reason}")

    # S1 should still be approved at this drawdown level
    s1_signal = dict(base_signal, market_id="0xS1NEW", size_usd=20.0)
    approved_s1, _ = await rm.check_signal(s1_signal)
    check("S1 still approved at moderate drawdown", approved_s1, "should be approved")

    # ---- Test 11: Venue halt ---------------------------------------------
    print()
    print("-" * 72)
    print("  Test 11: Venue halt")
    print("-" * 72)

    rm.clear_mock_state()
    rm._kill_switches.halt_venue("polymarket", "API unreachable for >5min")

    approved, reason = await rm.check_signal(base_signal)
    check("Rejected when venue halted", not approved, f"approved={approved}")
    check("Reason mentions venue", "venue" in reason.lower() or "polymarket" in reason.lower(), f"reason={reason}")

    # Resume venue
    rm._kill_switches.resume_venue("polymarket")
    approved2, _ = await rm.check_signal(base_signal)
    check("Approved after venue resumed", approved2, "should be approved")

    # ---- Test 12: Drawdown response curve multipliers --------------------
    print()
    print("-" * 72)
    print("  Test 12: Drawdown response curve multipliers")
    print("-" * 72)

    check("0% drawdown -> 1.0x", rm.get_drawdown_multiplier(0.0) == 1.0, "")
    check("3% drawdown -> 1.0x", rm.get_drawdown_multiplier(0.03) == 1.0, "")
    check("5% drawdown -> 1.0x", rm.get_drawdown_multiplier(0.05) == 1.0, "")
    check("7% drawdown -> 0.5x", rm.get_drawdown_multiplier(0.07) == 0.5, "")
    check("10% drawdown -> 0.5x", rm.get_drawdown_multiplier(0.10) == 0.5, "")
    check("12% drawdown -> 0.25x", rm.get_drawdown_multiplier(0.12) == 0.25, "")
    check("15% drawdown -> 0.25x", rm.get_drawdown_multiplier(0.15) == 0.25, "")
    check("18% drawdown -> 0.0x", rm.get_drawdown_multiplier(0.18) == 0.0, "")

    # ---- Test 13: Win resets consecutive loss counter --------------------
    print()
    print("-" * 72)
    print("  Test 13: Win resets consecutive loss counter")
    print("-" * 72)

    rm.clear_mock_state()
    for i in range(3):
        await rm.update_on_fill(
            strategy="s1", venue="polymarket", market_id=f"0xWIN{i}",
            side="yes", fill_price=0.50, size_usd=10.0, pnl=-5.0,
        )
    check(
        "3 losses recorded",
        rm._kill_switches.get_consecutive_losses("s1") == 3,
        f"got {rm._kill_switches.get_consecutive_losses('s1')}",
    )

    await rm.update_on_fill(
        strategy="s1", venue="polymarket", market_id="0xWINNER",
        side="yes", fill_price=0.50, size_usd=10.0, pnl=10.0,
    )
    check(
        "Win resets counter to 0",
        rm._kill_switches.get_consecutive_losses("s1") == 0,
        f"got {rm._kill_switches.get_consecutive_losses('s1')}",
    )

    # ---- Test 14: Oracle quarantine lifecycle ----------------------------
    print()
    print("-" * 72)
    print("  Test 14: Oracle quarantine lifecycle")
    print("-" * 72)

    oracle = OracleQuarantine()
    test_pair = "0xPOLY|KXTEST"

    # Initial divergence
    oracle.update_divergence(test_pair, 0.10)
    check("Not quarantined immediately", not oracle.is_quarantined(test_pair), "")

    # Simulate time passing
    oracle._divergences[test_pair] = now - timedelta(minutes=35)
    oracle.update_divergence(test_pair, 0.10)
    check("Quarantined after 30min divergence", oracle.is_quarantined(test_pair), "")

    # Convergence starts
    oracle.update_divergence(test_pair, 0.02)
    check("Still quarantined at convergence start", oracle.is_quarantined(test_pair), "")

    # Simulate 1+ hour of convergence
    oracle._convergences[test_pair] = now - timedelta(minutes=65)
    oracle.update_divergence(test_pair, 0.02)
    check("Quarantine lifted after 1hr convergence", not oracle.is_quarantined(test_pair), "")

    # ---- Test 15: Minimum position size ($10) ----------------------------
    print()
    print("-" * 72)
    print("  Test 15: Minimum position size ($10)")
    print("-" * 72)

    rm.clear_mock_state()
    tiny_signal = dict(base_signal, size_usd=5.0)
    approved, reason = await rm.check_signal(tiny_signal)
    check("Tiny position ($5) rejected", not approved, f"approved={approved}")
    check("Reason mentions minimum", "minimum" in reason.lower() or "below" in reason.lower(), f"reason={reason}")

    # ---- Test 16: Infrastructure - venue unreachable > 5 min -----------
    print()
    print("-" * 72)
    print("  Test 16: Infrastructure - venue unreachable > 5 min")
    print("-" * 72)

    rm.clear_mock_state()
    # Report healthy first, then simulate 5+ minutes of unhealthy
    rm.report_venue_health("polymarket", healthy=True)
    rm._venue_last_healthy["polymarket"] = now - timedelta(minutes=6)
    rm.report_venue_health("polymarket", healthy=False)

    halted_v, _ = rm._kill_switches.is_venue_halted("polymarket")
    check("Venue halted after 5+ min unreachable", halted_v, "")

    approved, reason = await rm.check_signal(base_signal)
    check("Signal rejected on halted venue", not approved, f"approved={approved}")

    # Venue recovers
    rm.report_venue_health("polymarket", healthy=True)
    halted_v2, _ = rm._kill_switches.is_venue_halted("polymarket")
    check("Venue resumed after recovery", not halted_v2, "")

    approved2, _ = await rm.check_signal(base_signal)
    check("Signal approved after venue recovery", approved2, "")

    # ---- Test 17: Infrastructure - Redis halt --------------------------
    print()
    print("-" * 72)
    print("  Test 17: Infrastructure - Redis halt")
    print("-" * 72)

    rm.clear_mock_state()
    rm.report_redis_health(healthy=False)

    approved, reason = await rm.check_signal(base_signal)
    check("Signal rejected when Redis is down", not approved, f"approved={approved}")
    check("Reason mentions Redis or infrastructure", "redis" in reason.lower() or "infrastructure" in reason.lower(), f"reason={reason}")

    rm.report_redis_health(healthy=True)
    approved2, _ = await rm.check_signal(base_signal)
    check("Signal approved after Redis recovery", approved2, "")

    # ---- Test 18: Infrastructure - DB halt -----------------------------
    print()
    print("-" * 72)
    print("  Test 18: Infrastructure - DB halt")
    print("-" * 72)

    rm.clear_mock_state()
    rm.report_db_health(healthy=False)

    approved, reason = await rm.check_signal(base_signal)
    check("Signal rejected when DB is down", not approved, f"approved={approved}")
    check("Reason mentions database or infrastructure", "database" in reason.lower() or "infrastructure" in reason.lower(), f"reason={reason}")

    rm.report_db_health(healthy=True)
    approved2, _ = await rm.check_signal(base_signal)
    check("Signal approved after DB recovery", approved2, "")

    # ---- Test 19: get_status() returns comprehensive snapshot ----------
    print()
    print("-" * 72)
    print("  Test 19: get_status() returns comprehensive snapshot")
    print("-" * 72)

    rm.clear_mock_state()
    status = await rm.get_status()
    check("Status has initial_bankroll", "initial_bankroll" in status, "")
    check("Status has current_bankroll", "current_bankroll" in status, "")
    check("Status has drawdown_24h_pct", "drawdown_24h_pct" in status, "")
    check("Status has kill_switches", "kill_switches" in status, "")
    check("Status has quarantined_pairs", "quarantined_pairs" in status, "")
    check("Status has redis_healthy", "redis_healthy" in status, "")
    check("Status has db_healthy", "db_healthy" in status, "")
    check("Status has exposure_by_strategy", "exposure_by_strategy" in status, "")

    # ---- Test 20: Negative bankroll triggers global halt ---------------
    print()
    print("-" * 72)
    print("  Test 20: Negative bankroll triggers global halt")
    print("-" * 72)

    rm.clear_mock_state()
    rm._initial_bankroll = 100.0
    rm._current_bankroll = 100.0
    await rm.update_on_fill(
        strategy="s1", venue="polymarket", market_id="0xBAD",
        side="yes", fill_price=0.50, size_usd=200.0, pnl=-150.0,
    )
    check("Global halt on negative bankroll", rm._kill_switches.global_halt, "")
    rm._initial_bankroll = 1000.0  # Restore

    # ---- Test 21: Cooldown auto-resume ---------------------------------
    print()
    print("-" * 72)
    print("  Test 21: Kill switch cooldown auto-resume")
    print("-" * 72)

    rm.clear_mock_state()
    # Manually trigger a strategy halt with a past timestamp so cooldown expired
    past_time = now - timedelta(seconds=STRATEGY_COOLDOWN_SECONDS + 10)
    rm._kill_switches.strategy_halts["s1"] = (
        "test_cooldown", past_time, True  # auto_resume=True
    )
    halted, _ = rm._kill_switches.is_strategy_halted("s1")
    check("Strategy auto-resumed after cooldown expired", not halted, "")

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
    CLI entry point: parse --test flag and run self-test suite.

    --test:    Run all risk manager tests with in-memory mock data.
    (default): Print usage information.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Risk manager for polymarket-trader. "
            "Enforces kill switches, drawdown monitoring, position limits, "
            "and oracle mismatch quarantine."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run self-test suite with in-memory mock data.",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running self-test suite.")
        asyncio.run(_run_self_test())
    else:
        print(
            "Usage: python execution/execution_engine/risk_manager.py --test"
        )
        print()
        print("  --test  Run self-test suite with mock data")
        print()
        print("For programmatic usage:")
        print("  from execution.execution_engine.risk_manager import RiskManager")
        print()


if __name__ == "__main__":
    main()
