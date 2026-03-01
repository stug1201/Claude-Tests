#!/usr/bin/env python3
"""
Unit tests for execution.execution_engine.risk_manager -- RiskManager.

Tests cover:
    - Normal signal approval
    - Global halt at >15% drawdown
    - Strategy halt on single trade loss >3% of bankroll
    - Strategy halt on 5 consecutive losses
    - Per-strategy exposure limit rejection
    - Per-market exposure limit rejection
    - Total deployed limit rejection
    - S5 per-pair $500 cap
    - Oracle quarantine blocks S5 signals
    - Venue halt blocks signals
    - Drawdown multiplier values at each tier
    - Win resets consecutive loss counter

Run:
    python -m unittest tests/unit/test_risk_manager.py
"""

import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone

# Ensure --test is in sys.argv BEFORE importing execution modules.
# The config module (imported transitively) creates a singleton at import
# time and checks sys.argv for --test to avoid requiring a .env file.
if "--test" not in sys.argv:
    sys.argv.append("--test")

from execution.execution_engine.risk_manager import (  # noqa: E402
    CONSECUTIVE_LOSS_LIMIT,
    DRAWDOWN_MODERATE_MAX,
    DRAWDOWN_NORMAL_MAX,
    DRAWDOWN_SEVERE_MAX,
    MAX_DRAWDOWN_PCT,
    MAX_PER_MARKET_PCT,
    MAX_PER_STRATEGY_PCT,
    MAX_TOTAL_DEPLOYED_PCT,
    S5_MAX_PER_PAIR_USD,
    SINGLE_TRADE_LOSS_PCT,
    KillSwitchState,
    OracleQuarantine,
    RiskManager,
)


# ---------------------------------------------------------------------------
# Mock config
# ---------------------------------------------------------------------------

class MockConfig:
    INITIAL_BANKROLL = 1000.0
    MAX_DRAWDOWN_PCT = 0.15


def _make_signal(**overrides) -> dict:
    """Build a valid base signal dict with optional overrides."""
    signal = {
        "strategy": "s1",
        "venue": "polymarket",
        "market_id": "0xTEST001",
        "side": "yes",
        "target_price": 0.60,
        "size_usd": 30.0,
        "edge_estimate": 0.05,
        "confidence": "high",
    }
    signal.update(overrides)
    return signal


# =========================================================================
#  Test: Normal signal approval
# =========================================================================

class TestNormalSignalApproval(unittest.TestCase):
    """A clean signal with no constraint violations should be approved."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_normal_signal_approved(self):
        """A standard signal on a clean risk manager should be approved."""
        signal = _make_signal()
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval, got rejection: {reason}")
        self.assertEqual(reason, "approved")

    def test_approved_reason_string(self):
        """The reason string for an approved signal should be exactly 'approved'."""
        signal = _make_signal()
        _, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertEqual(reason, "approved")


# =========================================================================
#  Test: Global halt at >15% drawdown
# =========================================================================

class TestGlobalHaltDrawdown(unittest.TestCase):
    """Signals should be rejected and global halt triggered at >15% drawdown."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_global_halt_triggered_at_high_drawdown(self):
        """Injecting enough losses to exceed 15% drawdown should trigger global halt."""
        now = datetime.now(timezone.utc)
        # Inject losing trades totalling $200 loss on $1000 bankroll.
        for i in range(4):
            self.rm.inject_mock_trade({
                "strategy": "s1",
                "venue": "polymarket",
                "market_id": f"0xLOSS{i:03d}",
                "side": "yes",
                "price": 0.60,
                "size": 50.0,
                "pnl": -50.0,
                "status": "filled",
                "time": now - timedelta(minutes=i * 5),
            })
        # Adjust bankroll to reflect the losses.
        self.rm._current_bankroll = 1000.0 - 200.0

        signal = _make_signal()
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertTrue(self.rm._kill_switches.global_halt)

    def test_subsequent_signals_rejected_after_global_halt(self):
        """Once global halt is set, all further signals should be rejected."""
        self.rm._kill_switches.trigger_global_halt("Test: manual global halt")
        signal = _make_signal()
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertIn("GLOBAL HALT", reason)

    def test_constant_value(self):
        """MAX_DRAWDOWN_PCT should be 0.15."""
        self.assertEqual(MAX_DRAWDOWN_PCT, 0.15)


# =========================================================================
#  Test: Strategy halt on single trade loss >3% of bankroll
# =========================================================================

class TestSingleTradeLossHalt(unittest.TestCase):
    """A single trade loss >3% of bankroll should halt the strategy."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_strategy_halted_after_big_loss(self):
        """A fill with PnL > 3% of bankroll should halt the strategy."""
        # 4% loss = $40 on $1000 bankroll.
        asyncio.run(self.rm.update_on_fill(
            strategy="s1",
            venue="polymarket",
            market_id="0xBIGLOSS",
            side="yes",
            fill_price=0.60,
            size_usd=50.0,
            pnl=-40.0,
        ))
        halted, halt_reason = self.rm._kill_switches.is_strategy_halted("s1")
        self.assertTrue(halted)
        self.assertIn("Single trade", halt_reason)

    def test_signal_rejected_after_strategy_halt(self):
        """Signals for the halted strategy should be rejected."""
        asyncio.run(self.rm.update_on_fill(
            strategy="s1",
            venue="polymarket",
            market_id="0xBIGLOSS",
            side="yes",
            fill_price=0.60,
            size_usd=50.0,
            pnl=-40.0,
        ))
        signal = _make_signal()
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertIn("halted", reason.lower())

    def test_small_loss_does_not_halt(self):
        """A loss under 3% should not halt the strategy."""
        asyncio.run(self.rm.update_on_fill(
            strategy="s1",
            venue="polymarket",
            market_id="0xSMALL",
            side="yes",
            fill_price=0.60,
            size_usd=20.0,
            pnl=-20.0,  # 2% of $1000 -- below threshold
        ))
        halted, _ = self.rm._kill_switches.is_strategy_halted("s1")
        self.assertFalse(halted)

    def test_constant_value(self):
        """SINGLE_TRADE_LOSS_PCT should be 0.03."""
        self.assertEqual(SINGLE_TRADE_LOSS_PCT, 0.03)


# =========================================================================
#  Test: Strategy halt on 5 consecutive losses
# =========================================================================

class TestConsecutiveLossHalt(unittest.TestCase):
    """5 consecutive losses on a strategy should halt it (manual review)."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_halt_after_five_consecutive_losses(self):
        """Five consecutive small losses should trigger strategy halt."""
        for i in range(5):
            asyncio.run(self.rm.update_on_fill(
                strategy="s3",
                venue="polymarket",
                market_id=f"0xLOSS{i}",
                side="yes",
                fill_price=0.50,
                size_usd=10.0,
                pnl=-5.0,  # Small losses (< 3% threshold)
            ))

        halted, halt_reason = self.rm._kill_switches.is_strategy_halted("s3")
        self.assertTrue(halted)
        self.assertIn("consecutive", halt_reason.lower())

    def test_four_losses_not_enough(self):
        """Four consecutive losses should NOT trigger a halt."""
        for i in range(4):
            asyncio.run(self.rm.update_on_fill(
                strategy="s3",
                venue="polymarket",
                market_id=f"0xLOSS{i}",
                side="yes",
                fill_price=0.50,
                size_usd=10.0,
                pnl=-5.0,
            ))

        halted, _ = self.rm._kill_switches.is_strategy_halted("s3")
        self.assertFalse(halted)

    def test_consecutive_loss_requires_manual_review(self):
        """Strategy halted by consecutive losses should NOT auto-resume."""
        for i in range(5):
            asyncio.run(self.rm.update_on_fill(
                strategy="s3",
                venue="polymarket",
                market_id=f"0xLOSS{i}",
                side="yes",
                fill_price=0.50,
                size_usd=10.0,
                pnl=-5.0,
            ))

        # The halt should have auto_resume=False.
        _, _, auto_resume = self.rm._kill_switches.strategy_halts["s3"]
        self.assertFalse(auto_resume)

    def test_constant_value(self):
        """CONSECUTIVE_LOSS_LIMIT should be 5."""
        self.assertEqual(CONSECUTIVE_LOSS_LIMIT, 5)


# =========================================================================
#  Test: Per-strategy exposure limit rejection
# =========================================================================

class TestPerStrategyExposureLimit(unittest.TestCase):
    """Signals should be rejected when strategy exposure exceeds the limit."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_rejected_at_strategy_limit(self):
        """A signal that would push S1 past 20% of bankroll should be rejected."""
        now = datetime.now(timezone.utc)
        # Fill S1 to $200 (20% of $1000).
        for i in range(4):
            self.rm.inject_mock_trade({
                "strategy": "s1",
                "venue": "polymarket",
                "market_id": f"0xEXP{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": 50.0,
                "pnl": 0.0,
                "status": "filled",
                "time": now - timedelta(minutes=i),
            })

        signal = _make_signal(size_usd=10.0)
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertTrue(
            "strategy" in reason.lower() or "per-strategy" in reason.lower(),
            f"Expected strategy-related rejection, got: {reason}",
        )

    def test_approved_below_strategy_limit(self):
        """A signal well under the strategy limit should be approved."""
        now = datetime.now(timezone.utc)
        # Only $50 deployed for S1 (well under 20%).
        self.rm.inject_mock_trade({
            "strategy": "s1",
            "venue": "polymarket",
            "market_id": "0xSMALL",
            "side": "yes",
            "price": 0.50,
            "size": 50.0,
            "pnl": 0.0,
            "status": "filled",
            "time": now - timedelta(minutes=1),
        })

        signal = _make_signal(size_usd=30.0)
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval, got: {reason}")


# =========================================================================
#  Test: Per-market exposure limit rejection
# =========================================================================

class TestPerMarketExposureLimit(unittest.TestCase):
    """Signals should be rejected when market exposure exceeds 10% of bankroll."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_rejected_at_market_limit(self):
        """A signal that would push a market past 10% should be rejected."""
        now = datetime.now(timezone.utc)
        # Fill market 0xTEST001 to $100 (10% of $1000).
        self.rm.inject_mock_trade({
            "strategy": "s3",
            "venue": "polymarket",
            "market_id": "0xTEST001",
            "side": "yes",
            "price": 0.50,
            "size": 100.0,
            "pnl": 0.0,
            "status": "filled",
            "time": now - timedelta(minutes=1),
        })

        signal = _make_signal(market_id="0xTEST001", size_usd=30.0)
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertIn("market", reason.lower())

    def test_constant_value(self):
        """MAX_PER_MARKET_PCT should be 0.10."""
        self.assertEqual(MAX_PER_MARKET_PCT, 0.10)


# =========================================================================
#  Test: Total deployed limit rejection
# =========================================================================

class TestTotalDeployedLimit(unittest.TestCase):
    """Signals should be rejected when total deployed exceeds 80% of bankroll."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_rejected_at_total_limit(self):
        """A signal that would push total deployed past 80% should be rejected."""
        now = datetime.now(timezone.utc)
        # Deploy $800 across 5 strategies, each within their individual limits.
        # With $1000 bankroll: s1 limit=20%=$200, others=15%=$150 each.
        # 200 + 150 + 150 + 150 + 150 = 800 = exactly 80%.
        entries = [
            ("s1", 200.0),
            ("s3", 150.0),
            ("s4", 150.0),
            ("s5", 150.0),
            ("s2", 150.0),
        ]
        for i, (strat, size) in enumerate(entries):
            self.rm.inject_mock_trade({
                "strategy": strat,
                "venue": "polymarket",
                "market_id": f"0xTOTAL{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": size,
                "pnl": 0.0,
                "status": "filled",
                "time": now - timedelta(minutes=i),
            })

        # Use a strategy (s1) that has no room due to per-strategy limit.
        # Instead, craft a signal on a strategy that still has room per-strategy
        # but no room per-total. We've used all per-strategy capacity so any
        # signal should hit a limit. Let's try s1 which is at exactly $200 (20%).
        # The total is exactly at $800 (80%). A new signal of $10 for s1 would
        # hit per-strategy first. To specifically test total deployed, reduce s1
        # to leave per-strategy room but keep total at 80%.
        self.rm._mock_trades.clear()
        entries2 = [
            ("s1", 160.0),  # under 20% ($200)
            ("s3", 150.0),
            ("s4", 150.0),
            ("s5", 150.0),
            ("s2", 150.0),  # under 15% ($150) -- but we need a 6th slot
        ]
        for i, (strat, size) in enumerate(entries2):
            self.rm.inject_mock_trade({
                "strategy": strat,
                "venue": "polymarket",
                "market_id": f"0xTOT2_{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": size,
                "pnl": 0.0,
                "status": "filled",
                "time": now - timedelta(minutes=i),
            })
        # Total deployed: 160 + 150 + 150 + 150 + 150 = 760.
        # S1 has $40 remaining per-strategy. Total has $40 remaining (800 - 760).
        # We need a signal that fits within per-strategy but exceeds total.
        # S1 at $160, limit $200, so $39 is fine per-strategy.
        # But $760 + $50 = $810 > $800 total limit.
        # Use $41 which fits per-strategy ($160+$41=$201 > $200). Too much.
        # Use $39 which fits per-strategy ($160+$39=$199 < $200).
        # Total: $760 + $39 = $799 < $800. Still under total!
        # We need total at exactly $800. Add a dummy trade:
        self.rm.inject_mock_trade({
            "strategy": "s4",
            "venue": "polymarket",
            "market_id": "0xTOT2_EXTRA",
            "side": "yes",
            "price": 0.50,
            "size": 40.0,
            "pnl": 0.0,
            "status": "filled",
            "time": now - timedelta(minutes=10),
        })
        # Now total = 160 + 150 + 190 + 150 + 150 = 800. S1 at $160, room=$40.
        # A $30 signal for S1: per-strategy OK ($190 < $200), but total $830 > $800.
        signal = _make_signal(market_id="0xNEWMARKET", size_usd=30.0)
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertIn("total", reason.lower())

    def test_constant_value(self):
        """MAX_TOTAL_DEPLOYED_PCT should be 0.80."""
        self.assertEqual(MAX_TOTAL_DEPLOYED_PCT, 0.80)


# =========================================================================
#  Test: S5 per-pair $500 cap
# =========================================================================

class TestS5PerPairCap(unittest.TestCase):
    """S5 cross-venue pairs should be capped at $500 per pair."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_s5_rejected_at_pair_cap(self):
        """An S5 signal exceeding $500 per pair should be rejected."""
        now = datetime.now(timezone.utc)
        pair_key = "0xPOLY|KXKALSHI"
        # Use a larger bankroll so that per-strategy limits (15% = $15,000)
        # are not hit before the $500 per-pair cap.
        self.rm._current_bankroll = 100_000.0
        self.rm._initial_bankroll = 100_000.0

        # Inject $480 existing exposure on this pair.
        self.rm.inject_mock_trade({
            "strategy": "s5",
            "venue": "cross",
            "market_id": pair_key,
            "side": "yes|no",
            "price": 0.50,
            "size": 480.0,
            "pnl": 0.0,
            "status": "filled",
            "time": now - timedelta(minutes=1),
        })

        signal = _make_signal(
            strategy="s5",
            venue="cross",
            market_id=pair_key,
            side="yes|no",
            size_usd=30.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertIn("pair", reason.lower())

    def test_s5_approved_below_pair_cap(self):
        """An S5 signal well under $500 should be approved."""
        signal = _make_signal(
            strategy="s5",
            venue="cross",
            market_id="0xPOLY|KXNEW",
            side="yes|no",
            size_usd=30.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval, got: {reason}")

    def test_constant_value(self):
        """S5_MAX_PER_PAIR_USD should be 500.0."""
        self.assertEqual(S5_MAX_PER_PAIR_USD, 500.0)


# =========================================================================
#  Test: Oracle quarantine blocks S5 signals
# =========================================================================

class TestOracleQuarantineBlocksS5(unittest.TestCase):
    """Quarantined oracle pairs should block S5 signals."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_quarantined_pair_rejects_s5(self):
        """An S5 signal for a quarantined pair should be rejected."""
        pair_key = "0xPOLY|KXKALSHI"
        now = datetime.now(timezone.utc)
        # Directly quarantine the pair.
        self.rm._oracle._quarantined[pair_key] = now

        signal = _make_signal(
            strategy="s5",
            venue="cross",
            market_id=pair_key,
            side="yes|no",
            size_usd=20.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertIn("quarantine", reason.lower())

    def test_non_quarantined_pair_approves_s5(self):
        """An S5 signal for a non-quarantined pair should be approved."""
        signal = _make_signal(
            strategy="s5",
            venue="cross",
            market_id="0xCLEAN_PAIR",
            side="yes|no",
            size_usd=20.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval, got: {reason}")

    def test_s1_not_affected_by_quarantine(self):
        """Non-S5 strategies should not be affected by oracle quarantine."""
        pair_key = "0xPOLY|KXKALSHI"
        self.rm._oracle._quarantined[pair_key] = datetime.now(timezone.utc)

        # S1 signal to a different market should still be approved.
        signal = _make_signal(strategy="s1", market_id="0xOTHER")
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval, got: {reason}")


# =========================================================================
#  Test: Oracle quarantine lifecycle
# =========================================================================

class TestOracleQuarantineLifecycle(unittest.TestCase):
    """OracleQuarantine should track divergence/convergence correctly."""

    def test_not_quarantined_immediately(self):
        """A divergent pair should NOT be quarantined immediately."""
        oracle = OracleQuarantine()
        oracle.update_divergence("pair1", 0.10)
        self.assertFalse(oracle.is_quarantined("pair1"))

    def test_quarantined_after_30min_divergence(self):
        """A divergent pair should be quarantined after 30 minutes."""
        oracle = OracleQuarantine()
        now = datetime.now(timezone.utc)
        # Initial divergence.
        oracle.update_divergence("pair1", 0.10)
        # Simulate 35 minutes of divergence.
        oracle._divergences["pair1"] = now - timedelta(minutes=35)
        oracle.update_divergence("pair1", 0.10)
        self.assertTrue(oracle.is_quarantined("pair1"))

    def test_quarantine_lifted_after_convergence(self):
        """Quarantine should be lifted after 1 hour of convergence."""
        oracle = OracleQuarantine()
        now = datetime.now(timezone.utc)
        # Quarantine the pair.
        oracle._quarantined["pair1"] = now - timedelta(minutes=90)
        # Start convergence tracking.
        oracle.update_divergence("pair1", 0.02)
        # Simulate 65 minutes of convergence.
        oracle._convergences["pair1"] = now - timedelta(minutes=65)
        oracle.update_divergence("pair1", 0.02)
        self.assertFalse(oracle.is_quarantined("pair1"))


# =========================================================================
#  Test: Venue halt blocks signals
# =========================================================================

class TestVenueHaltBlocksSignals(unittest.TestCase):
    """Signals for halted venues should be rejected."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_venue_halt_rejects_signals(self):
        """Signals on a halted venue should be rejected."""
        self.rm._kill_switches.halt_venue("polymarket", "API unreachable for >5min")

        signal = _make_signal(venue="polymarket")
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)
        self.assertTrue(
            "venue" in reason.lower() or "polymarket" in reason.lower(),
            f"Expected venue-related rejection, got: {reason}",
        )

    def test_venue_resume_allows_signals(self):
        """Signals should be approved after a venue is resumed."""
        self.rm._kill_switches.halt_venue("polymarket", "API unreachable")
        self.rm._kill_switches.resume_venue("polymarket")

        signal = _make_signal(venue="polymarket")
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval after resume, got: {reason}")

    def test_other_venue_not_affected(self):
        """Halting one venue should not affect signals on a different venue."""
        self.rm._kill_switches.halt_venue("kalshi", "API down")

        signal = _make_signal(venue="polymarket")
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertTrue(approved, f"Expected approval for polymarket, got: {reason}")

    def test_cross_venue_blocked_if_either_halted(self):
        """S5 cross-venue signals should be blocked if either venue is halted."""
        self.rm._kill_switches.halt_venue("kalshi", "API down")

        signal = _make_signal(
            strategy="s5",
            venue="cross",
            market_id="0xPOLY|KXTEST",
            side="yes|no",
            size_usd=20.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(signal))
        self.assertFalse(approved)


# =========================================================================
#  Test: Drawdown multiplier values at each tier
# =========================================================================

class TestDrawdownMultiplierValues(unittest.TestCase):
    """get_drawdown_multiplier should return correct values for each tier."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_0_pct_returns_1(self):
        """0% drawdown -> 1.0x."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.0), 1.0)

    def test_3_pct_returns_1(self):
        """3% drawdown -> 1.0x."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.03), 1.0)

    def test_5_pct_returns_1(self):
        """5% drawdown -> 1.0x (boundary of normal tier)."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.05), 1.0)

    def test_7_pct_returns_05(self):
        """7% drawdown -> 0.5x."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.07), 0.5)

    def test_10_pct_returns_05(self):
        """10% drawdown -> 0.5x (boundary of moderate tier)."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.10), 0.5)

    def test_12_pct_returns_025(self):
        """12% drawdown -> 0.25x."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.12), 0.25)

    def test_15_pct_returns_025(self):
        """15% drawdown -> 0.25x (boundary of severe tier)."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.15), 0.25)

    def test_18_pct_returns_0(self):
        """18% drawdown -> 0.0x (GLOBAL HALT)."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.18), 0.0)

    def test_25_pct_returns_0(self):
        """25% drawdown -> 0.0x (GLOBAL HALT)."""
        self.assertEqual(self.rm.get_drawdown_multiplier(0.25), 0.0)

    def test_drawdown_tier_constants(self):
        """Drawdown tier constants should match expected values."""
        self.assertEqual(DRAWDOWN_NORMAL_MAX, 0.05)
        self.assertEqual(DRAWDOWN_MODERATE_MAX, 0.10)
        self.assertEqual(DRAWDOWN_SEVERE_MAX, 0.15)


# =========================================================================
#  Test: Win resets consecutive loss counter
# =========================================================================

class TestWinResetsConsecutiveLossCounter(unittest.TestCase):
    """A win should reset the consecutive loss counter to zero."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_losses_accumulate(self):
        """Consecutive losses should be tracked correctly."""
        for i in range(3):
            asyncio.run(self.rm.update_on_fill(
                strategy="s1",
                venue="polymarket",
                market_id=f"0xWIN{i}",
                side="yes",
                fill_price=0.50,
                size_usd=10.0,
                pnl=-5.0,
            ))
        self.assertEqual(
            self.rm._kill_switches.get_consecutive_losses("s1"), 3,
        )

    def test_win_resets_counter(self):
        """A winning trade should reset consecutive losses to 0."""
        for i in range(3):
            asyncio.run(self.rm.update_on_fill(
                strategy="s1",
                venue="polymarket",
                market_id=f"0xLOSE{i}",
                side="yes",
                fill_price=0.50,
                size_usd=10.0,
                pnl=-5.0,
            ))
        # Now record a win.
        asyncio.run(self.rm.update_on_fill(
            strategy="s1",
            venue="polymarket",
            market_id="0xWINNER",
            side="yes",
            fill_price=0.50,
            size_usd=10.0,
            pnl=10.0,
        ))
        self.assertEqual(
            self.rm._kill_switches.get_consecutive_losses("s1"), 0,
        )

    def test_losses_after_win_restart_from_zero(self):
        """After a win resets the counter, losses should start from zero again."""
        # 3 losses.
        for i in range(3):
            asyncio.run(self.rm.update_on_fill(
                strategy="s1", venue="polymarket", market_id=f"0xL{i}",
                side="yes", fill_price=0.50, size_usd=10.0, pnl=-5.0,
            ))
        # Win.
        asyncio.run(self.rm.update_on_fill(
            strategy="s1", venue="polymarket", market_id="0xW",
            side="yes", fill_price=0.50, size_usd=10.0, pnl=5.0,
        ))
        # 2 more losses.
        for i in range(2):
            asyncio.run(self.rm.update_on_fill(
                strategy="s1", venue="polymarket", market_id=f"0xL2{i}",
                side="yes", fill_price=0.50, size_usd=10.0, pnl=-5.0,
            ))
        self.assertEqual(
            self.rm._kill_switches.get_consecutive_losses("s1"), 2,
        )


# =========================================================================
#  Test: S2 halted at 10-15% drawdown (via check_signal)
# =========================================================================

class TestS2HaltAtModerateDrawdown(unittest.TestCase):
    """S2 signals should be rejected when drawdown is between 10-15%."""

    def setUp(self):
        self.config = MockConfig()
        self.rm = RiskManager(self.config, test_mode=True)

    def test_s2_rejected_at_12pct_drawdown(self):
        """S2 should be rejected at ~12% drawdown."""
        now = datetime.now(timezone.utc)
        # Inject trades to create ~12% drawdown.
        for i in range(3):
            self.rm.inject_mock_trade({
                "strategy": "s4",
                "venue": "polymarket",
                "market_id": f"0xDD{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": 50.0,
                "pnl": -40.0,
                "status": "filled",
                "time": now - timedelta(minutes=i * 5),
            })

        s2_signal = _make_signal(
            strategy="s2",
            market_id="0xS2TEST",
            size_usd=20.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(s2_signal))
        self.assertFalse(approved)
        self.assertTrue(
            "S2" in reason or "s2" in reason,
            f"Expected S2-specific rejection, got: {reason}",
        )

    def test_s1_still_approved_at_12pct_drawdown(self):
        """S1 should still be approved at ~12% drawdown."""
        now = datetime.now(timezone.utc)
        for i in range(3):
            self.rm.inject_mock_trade({
                "strategy": "s4",
                "venue": "polymarket",
                "market_id": f"0xDD{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": 50.0,
                "pnl": -40.0,
                "status": "filled",
                "time": now - timedelta(minutes=i * 5),
            })

        s1_signal = _make_signal(
            strategy="s1",
            market_id="0xS1NEW",
            size_usd=20.0,
        )
        approved, reason = asyncio.run(self.rm.check_signal(s1_signal))
        self.assertTrue(approved, f"S1 should be approved at moderate drawdown, got: {reason}")


# =========================================================================
#  Test: KillSwitchState unit tests
# =========================================================================

class TestKillSwitchState(unittest.TestCase):
    """Unit tests for the KillSwitchState helper class."""

    def test_initial_state_no_halts(self):
        """A fresh KillSwitchState should have no halts."""
        ks = KillSwitchState()
        self.assertFalse(ks.global_halt)
        self.assertEqual(len(ks.strategy_halts), 0)
        self.assertEqual(len(ks.venue_halts), 0)

    def test_global_halt(self):
        """trigger_global_halt should set the global halt flag."""
        ks = KillSwitchState()
        ks.trigger_global_halt("Test halt")
        self.assertTrue(ks.global_halt)
        self.assertEqual(ks.global_halt_reason, "Test halt")
        self.assertIsNotNone(ks.global_halt_time)

    def test_strategy_halt_and_check(self):
        """halt_strategy / is_strategy_halted should work correctly."""
        ks = KillSwitchState()
        ks.halt_strategy("s1", "Test reason", auto_resume=False)
        halted, reason = ks.is_strategy_halted("s1")
        self.assertTrue(halted)
        self.assertEqual(reason, "Test reason")

    def test_venue_halt_and_resume(self):
        """halt_venue / resume_venue / is_venue_halted should work correctly."""
        ks = KillSwitchState()
        ks.halt_venue("polymarket", "API down")
        halted, reason = ks.is_venue_halted("polymarket")
        self.assertTrue(halted)
        self.assertEqual(reason, "API down")

        ks.resume_venue("polymarket")
        halted, _ = ks.is_venue_halted("polymarket")
        self.assertFalse(halted)

    def test_loss_tracking(self):
        """record_loss / record_win / get_consecutive_losses should work."""
        ks = KillSwitchState()
        ks.record_loss("s1")
        ks.record_loss("s1")
        self.assertEqual(ks.get_consecutive_losses("s1"), 2)
        ks.record_win("s1")
        self.assertEqual(ks.get_consecutive_losses("s1"), 0)


# =========================================================================
#  Test: clear_mock_state
# =========================================================================

class TestClearMockState(unittest.TestCase):
    """clear_mock_state should fully reset the RiskManager for clean tests."""

    def test_clear_resets_all_state(self):
        """After clear_mock_state, all internal state should be fresh."""
        config = MockConfig()
        rm = RiskManager(config, test_mode=True)

        # Dirty up the state.
        rm.inject_mock_trade({
            "strategy": "s1", "venue": "polymarket", "market_id": "0x1",
            "side": "yes", "price": 0.5, "size": 100.0, "pnl": -50.0,
            "status": "filled", "time": datetime.now(timezone.utc),
        })
        rm._kill_switches.trigger_global_halt("test")
        rm._oracle._quarantined["pair1"] = datetime.now(timezone.utc)

        rm.clear_mock_state()

        self.assertEqual(len(rm._mock_trades), 0)
        self.assertEqual(rm._current_bankroll, config.INITIAL_BANKROLL)
        self.assertFalse(rm._kill_switches.global_halt)
        self.assertEqual(len(rm._oracle.get_quarantined_pairs()), 0)


if __name__ == "__main__":
    unittest.main()
