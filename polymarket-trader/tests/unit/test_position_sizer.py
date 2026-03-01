#!/usr/bin/env python3
"""
Unit tests for execution.execution_engine.position_sizer -- PositionSizer.

Tests cover:
    - Kelly fraction is positive for a signal with positive edge
    - Kelly fraction is negative/zero for signal with no edge (rejected)
    - Fractional Kelly multipliers per strategy
    - Per-position cap at 5% of bankroll
    - Per-strategy cap at 20% of bankroll
    - Per-market cap at 10% of bankroll
    - Total deployed cap at 80%
    - S5 cross-venue cap at $500
    - Minimum position $10 rejection
    - Drawdown response curve at all tiers
    - S2 halted at 10-15% drawdown
    - Invalid signal validation (missing fields, bad values)

Run:
    python -m unittest tests/unit/test_position_sizer.py
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

from execution.execution_engine.position_sizer import (  # noqa: E402
    DRAWDOWN_NORMAL_MAX,
    DRAWDOWN_REDUCED_MAX,
    DRAWDOWN_REDUCTION_MODERATE,
    DRAWDOWN_REDUCTION_NORMAL,
    DRAWDOWN_REDUCTION_SEVERE,
    DRAWDOWN_SEVERE_MAX,
    KELLY_MULTIPLIERS,
    MAX_PER_MARKET_PCT,
    MAX_PER_POSITION_PCT,
    MAX_PER_STRATEGY_PCT,
    MAX_TOTAL_DEPLOYED_PCT,
    MIN_POSITION_USD,
    S5_MAX_CROSS_VENUE_USD,
    PositionSizer,
)


# ---------------------------------------------------------------------------
# Mock config that mirrors Config in test mode
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
        "size_usd": 100.0,
        "edge_estimate": 0.06,
        "confidence": "high",
    }
    signal.update(overrides)
    return signal


class TestKellyFractionPositiveEdge(unittest.TestCase):
    """Kelly fraction should be positive for a signal with positive edge."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_kelly_fraction_positive_for_positive_edge(self):
        """A signal with target_price=0.60, edge=0.06 should yield f* > 0."""
        signal = _make_signal()
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        self.assertGreater(result["kelly_fraction"], 0.0)

    def test_approved_size_positive(self):
        """Approved size should be > 0 for a valid signal with positive edge."""
        signal = _make_signal()
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertTrue(result["approved"])
        self.assertGreater(result["approved_size_usd"], 0.0)


class TestKellyFractionNoEdge(unittest.TestCase):
    """Kelly fraction should be <= 0 (rejected) when there is no edge."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_zero_edge_rejected(self):
        """A signal with edge_estimate=0 should be rejected (no edge)."""
        signal = _make_signal(edge_estimate=0.0)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIsNotNone(result["rejection_reason"])

    def test_tiny_edge_at_high_fee_rejected(self):
        """A signal with a tiny edge on an expensive contract should be rejected."""
        # target_price = 0.98 -> cost = 0.98, b = (0.98/0.98)-1 ~ 0.0
        # Even with small positive edge, Kelly may still be negative at near-1 prices.
        signal = _make_signal(target_price=0.98, edge_estimate=0.001)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])


class TestFractionalKellyMultipliers(unittest.TestCase):
    """Verify that the correct fractional Kelly multiplier is applied per strategy."""

    def setUp(self):
        self.config = MockConfig()

    def test_s1_quarter_kelly(self):
        """S1 (Near-Res) should use 0.25x Kelly multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        signal = _make_signal(strategy="s1")
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        self.assertEqual(result["details"]["kelly_multiplier"], 0.25)

    def test_s2_eighth_kelly(self):
        """S2 (Insider) should use 0.125x Kelly multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        signal = _make_signal(strategy="s2")
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        self.assertEqual(result["details"]["kelly_multiplier"], 0.125)

    def test_s3_quarter_kelly(self):
        """S3 (Glint News) should use 0.25x Kelly multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        signal = _make_signal(strategy="s3")
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        self.assertEqual(result["details"]["kelly_multiplier"], 0.25)

    def test_s4_half_kelly(self):
        """S4 (Intra Arb) should use 0.50x Kelly multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        signal = _make_signal(strategy="s4")
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        self.assertEqual(result["details"]["kelly_multiplier"], 0.50)

    def test_s5_half_kelly(self):
        """S5 (Cross Arb) should use 0.50x Kelly multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        signal = _make_signal(strategy="s5", venue="cross")
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        self.assertEqual(result["details"]["kelly_multiplier"], 0.50)

    def test_multiplier_constants_match_expected(self):
        """KELLY_MULTIPLIERS dict should match the directive values exactly."""
        self.assertEqual(KELLY_MULTIPLIERS["s1"], 0.25)
        self.assertEqual(KELLY_MULTIPLIERS["s2"], 0.125)
        self.assertEqual(KELLY_MULTIPLIERS["s3"], 0.25)
        self.assertEqual(KELLY_MULTIPLIERS["s4"], 0.50)
        self.assertEqual(KELLY_MULTIPLIERS["s5"], 0.50)


class TestPerPositionCap(unittest.TestCase):
    """Per-position size should be capped at 5% of bankroll."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_per_position_cap_enforced(self):
        """A signal requesting more than 5% of bankroll should be capped at $50."""
        # With $1000 bankroll, 5% = $50.
        signal = _make_signal(size_usd=500.0, edge_estimate=0.10)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))
        max_allowed = self.config.INITIAL_BANKROLL * MAX_PER_POSITION_PCT
        self.assertLessEqual(result["approved_size_usd"], max_allowed)

    def test_constant_value(self):
        """MAX_PER_POSITION_PCT should be 0.05."""
        self.assertEqual(MAX_PER_POSITION_PCT, 0.05)


class TestPerStrategyCap(unittest.TestCase):
    """Per-strategy exposure should be capped at 20% of bankroll."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_per_strategy_cap_rejects_when_full(self):
        """Signals should be rejected when strategy exposure reaches 20% of bankroll."""
        now = datetime.now(timezone.utc)
        bankroll = self.config.INITIAL_BANKROLL
        # Fill up S1 to exactly 20% = $200 with 4 trades of $50 each.
        for i in range(4):
            self.sizer.inject_mock_trade({
                "strategy": "s1",
                "venue": "polymarket",
                "market_id": f"0xSTRAT{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": 50.0,
                "pnl": 0.0,
                "status": "filled",
                "time": now - timedelta(minutes=i),
            })

        # This signal would push S1 past 20%.
        signal = _make_signal(market_id="0xNEWMARKET", size_usd=50.0, edge_estimate=0.06)
        result = asyncio.run(self.sizer.compute_size(signal))
        # The signal should be rejected. The strategy constraint limits remaining
        # size to $0, which falls below the $10 minimum, so the rejection reason
        # may mention "minimum" instead of "strategy" -- either way, it must be
        # rejected when the strategy is at capacity.
        self.assertFalse(
            result["approved"],
            f"Signal should be rejected at strategy capacity, got approved "
            f"with size=${result['approved_size_usd']}",
        )

    def test_constant_value(self):
        """MAX_PER_STRATEGY_PCT should be 0.20."""
        self.assertEqual(MAX_PER_STRATEGY_PCT, 0.20)


class TestPerMarketCap(unittest.TestCase):
    """Per-market exposure should be capped at 10% of bankroll."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_per_market_cap_constrains_size(self):
        """A signal to a market already near the 10% cap should be constrained."""
        now = datetime.now(timezone.utc)
        bankroll = self.config.INITIAL_BANKROLL
        # Inject $95 of existing exposure on market 0xTEST001.
        self.sizer.inject_mock_trade({
            "strategy": "s3",
            "venue": "polymarket",
            "market_id": "0xTEST001",
            "side": "yes",
            "price": 0.50,
            "size": 95.0,
            "pnl": 0.0,
            "status": "filled",
            "time": now - timedelta(minutes=1),
        })

        # Request $50 on the same market. Only $5 of room remains.
        signal = _make_signal(market_id="0xTEST001", size_usd=50.0)
        result = asyncio.run(self.sizer.compute_size(signal))
        if result["approved"]:
            total_market = 95.0 + result["approved_size_usd"]
            self.assertLessEqual(total_market, bankroll * MAX_PER_MARKET_PCT + 0.01)
        else:
            # Rejected because remaining room < $10 minimum.
            self.assertIsNotNone(result["rejection_reason"])

    def test_constant_value(self):
        """MAX_PER_MARKET_PCT should be 0.10."""
        self.assertEqual(MAX_PER_MARKET_PCT, 0.10)


class TestTotalDeployedCap(unittest.TestCase):
    """Total deployed capital should be capped at 80% of bankroll."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_total_deployed_cap_rejects_when_full(self):
        """Signals should be rejected when total deployment reaches 80%."""
        now = datetime.now(timezone.utc)
        bankroll = self.config.INITIAL_BANKROLL
        # Deploy $790 across various strategies and markets.
        strategies = ["s1", "s3", "s4", "s4"]
        for i, strat in enumerate(strategies):
            self.sizer.inject_mock_trade({
                "strategy": strat,
                "venue": "polymarket",
                "market_id": f"0xTOTAL{i:03d}",
                "side": "yes",
                "price": 0.50,
                "size": 197.5,  # 4 * 197.5 = 790
                "pnl": 0.0,
                "status": "filled",
                "time": now - timedelta(minutes=i),
            })

        # Request another $50 -- total would be $840 > $800 (80%).
        signal = _make_signal(
            strategy="s1",
            market_id="0xNEWMARKET2",
            size_usd=50.0,
        )
        result = asyncio.run(self.sizer.compute_size(signal))
        if result["approved"]:
            total = 790.0 + result["approved_size_usd"]
            self.assertLessEqual(total, bankroll * MAX_TOTAL_DEPLOYED_PCT + 0.01)
        else:
            self.assertIsNotNone(result["rejection_reason"])

    def test_constant_value(self):
        """MAX_TOTAL_DEPLOYED_PCT should be 0.80."""
        self.assertEqual(MAX_TOTAL_DEPLOYED_PCT, 0.80)


class TestS5CrossVenueCap(unittest.TestCase):
    """S5 cross-venue positions should be hard-capped at $500 per pair."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_s5_cap_constrains_large_size(self):
        """An S5 signal for a large amount should be capped at $500."""
        signal = _make_signal(
            strategy="s5",
            venue="cross",
            market_id="0xPOLY|KXKALSHI",
            size_usd=1000.0,
            edge_estimate=0.10,
        )
        result = asyncio.run(self.sizer.compute_size(signal))
        if result["approved"]:
            self.assertLessEqual(result["approved_size_usd"], S5_MAX_CROSS_VENUE_USD)

    def test_constant_value(self):
        """S5_MAX_CROSS_VENUE_USD should be 500.0."""
        self.assertEqual(S5_MAX_CROSS_VENUE_USD, 500.0)


class TestMinimumPositionRejection(unittest.TestCase):
    """Positions below $10 should be rejected."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_below_minimum_rejected(self):
        """A signal whose computed size falls below $10 should be rejected."""
        # Use a small edge that produces a positive but tiny Kelly fraction.
        # S2 uses 0.125x Kelly and "low" confidence applies 0.50x on top.
        # With target_price=0.60, edge=0.02: p=0.62, the Kelly fraction
        # will be small, and after 0.125 * 0.50 multiplier the USD size
        # will fall below $10.
        signal = _make_signal(
            strategy="s2",
            edge_estimate=0.02,
            size_usd=100.0,
            target_price=0.60,
            confidence="low",
        )
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("minimum", result["rejection_reason"].lower())

    def test_constant_value(self):
        """MIN_POSITION_USD should be 10.0."""
        self.assertEqual(MIN_POSITION_USD, 10.0)


class TestDrawdownResponseCurve(unittest.TestCase):
    """Drawdown response curve should apply the correct multiplier at each tier."""

    def setUp(self):
        self.config = MockConfig()

    def test_0_to_5_pct_normal(self):
        """0-5% drawdown should yield 1.0x multiplier (normal operation)."""
        sizer = PositionSizer(self.config, test_mode=True)
        self.assertEqual(sizer._get_drawdown_multiplier(0.0), 1.0)
        self.assertEqual(sizer._get_drawdown_multiplier(0.03), 1.0)
        self.assertEqual(sizer._get_drawdown_multiplier(0.05), 1.0)

    def test_5_to_10_pct_reduced(self):
        """5-10% drawdown should yield 0.5x multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        self.assertEqual(sizer._get_drawdown_multiplier(0.07), 0.5)
        self.assertEqual(sizer._get_drawdown_multiplier(0.10), 0.5)

    def test_10_to_15_pct_severe(self):
        """10-15% drawdown should yield 0.25x multiplier."""
        sizer = PositionSizer(self.config, test_mode=True)
        self.assertEqual(sizer._get_drawdown_multiplier(0.12), 0.25)
        self.assertEqual(sizer._get_drawdown_multiplier(0.15), 0.25)

    def test_over_15_pct_global_halt(self):
        """Over 15% drawdown should yield 0.0x multiplier (global halt)."""
        sizer = PositionSizer(self.config, test_mode=True)
        self.assertEqual(sizer._get_drawdown_multiplier(0.18), 0.0)
        self.assertEqual(sizer._get_drawdown_multiplier(0.25), 0.0)

    def test_drawdown_signal_rejected_over_15(self):
        """A signal at >15% drawdown should be outright rejected (GLOBAL HALT)."""
        sizer = PositionSizer(self.config, test_mode=True)
        sizer.set_mock_drawdown(0.16)
        signal = _make_signal()
        result = asyncio.run(sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("GLOBAL HALT", result["rejection_reason"])

    def test_drawdown_reduces_size_at_moderate(self):
        """At 7% drawdown the approved size should be roughly half the normal size."""
        sizer_normal = PositionSizer(self.config, test_mode=True)
        sizer_normal.set_mock_drawdown(0.0)
        signal = _make_signal()
        result_normal = asyncio.run(sizer_normal.compute_size(signal))

        sizer_mod = PositionSizer(self.config, test_mode=True)
        sizer_mod.set_mock_drawdown(0.07)
        result_mod = asyncio.run(sizer_mod.compute_size(signal))

        if result_normal["approved"] and result_mod["approved"]:
            # The moderate drawdown size should be <= normal size.
            self.assertLessEqual(
                result_mod["approved_size_usd"],
                result_normal["approved_size_usd"],
            )
            # Check the drawdown multiplier is applied.
            self.assertEqual(result_mod["drawdown_multiplier"], 0.5)

    def test_drawdown_constant_values(self):
        """Drawdown threshold constants should match directive values."""
        self.assertEqual(DRAWDOWN_NORMAL_MAX, 0.05)
        self.assertEqual(DRAWDOWN_REDUCED_MAX, 0.10)
        self.assertEqual(DRAWDOWN_SEVERE_MAX, 0.15)
        self.assertEqual(DRAWDOWN_REDUCTION_NORMAL, 1.0)
        self.assertEqual(DRAWDOWN_REDUCTION_MODERATE, 0.5)
        self.assertEqual(DRAWDOWN_REDUCTION_SEVERE, 0.25)


class TestS2HaltAtSevereDrawdown(unittest.TestCase):
    """S2 should be halted when drawdown is between 10-15%."""

    def setUp(self):
        self.config = MockConfig()

    def test_s2_rejected_at_12_pct_drawdown(self):
        """S2 signal should be rejected at 12% drawdown."""
        sizer = PositionSizer(self.config, test_mode=True)
        sizer.set_mock_drawdown(0.12)
        signal = _make_signal(strategy="s2")
        result = asyncio.run(sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("S2", result["rejection_reason"])

    def test_s1_still_approved_at_12_pct_drawdown(self):
        """S1 should still be approved (with reduced size) at 12% drawdown."""
        sizer = PositionSizer(self.config, test_mode=True)
        sizer.set_mock_drawdown(0.12)
        # Use a larger edge so that after 0.25x drawdown multiplier the size
        # stays above the $10 minimum.
        signal = _make_signal(strategy="s1", edge_estimate=0.15, size_usd=200.0)
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))

    def test_s2_approved_below_10_pct_drawdown(self):
        """S2 should be approved when drawdown is below 10%."""
        sizer = PositionSizer(self.config, test_mode=True)
        sizer.set_mock_drawdown(0.08)
        # S2 uses 0.125x Kelly. With 0.5x drawdown reduction that is 0.0625x.
        # Use a big edge to ensure the result exceeds $10 minimum.
        signal = _make_signal(strategy="s2", edge_estimate=0.15, size_usd=200.0)
        result = asyncio.run(sizer.compute_size(signal))
        self.assertTrue(result["approved"], result.get("rejection_reason"))


class TestInvalidSignalValidation(unittest.TestCase):
    """Invalid signals should be rejected with clear error messages."""

    def setUp(self):
        self.config = MockConfig()
        self.sizer = PositionSizer(self.config, test_mode=True)

    def test_missing_strategy(self):
        """Signal missing 'strategy' field should be rejected."""
        signal = _make_signal()
        del signal["strategy"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("strategy", result["rejection_reason"].lower())

    def test_missing_venue(self):
        """Signal missing 'venue' field should be rejected."""
        signal = _make_signal()
        del signal["venue"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("venue", result["rejection_reason"].lower())

    def test_missing_market_id(self):
        """Signal missing 'market_id' field should be rejected."""
        signal = _make_signal()
        del signal["market_id"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("market_id", result["rejection_reason"].lower())

    def test_missing_side(self):
        """Signal missing 'side' field should be rejected."""
        signal = _make_signal()
        del signal["side"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("side", result["rejection_reason"].lower())

    def test_missing_target_price(self):
        """Signal missing 'target_price' field should be rejected."""
        signal = _make_signal()
        del signal["target_price"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("target_price", result["rejection_reason"].lower())

    def test_missing_size_usd(self):
        """Signal missing 'size_usd' field should be rejected."""
        signal = _make_signal()
        del signal["size_usd"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("size_usd", result["rejection_reason"].lower())

    def test_missing_edge_estimate(self):
        """Signal missing 'edge_estimate' field should be rejected."""
        signal = _make_signal()
        del signal["edge_estimate"]
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("edge_estimate", result["rejection_reason"].lower())

    def test_invalid_strategy_name(self):
        """Unknown strategy should be rejected."""
        signal = _make_signal(strategy="s99")
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("unknown strategy", result["rejection_reason"].lower())

    def test_target_price_out_of_range_high(self):
        """target_price >= 1.0 should be rejected."""
        signal = _make_signal(target_price=1.0)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("target_price", result["rejection_reason"].lower())

    def test_target_price_out_of_range_low(self):
        """target_price <= 0.0 should be rejected."""
        signal = _make_signal(target_price=0.0)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("target_price", result["rejection_reason"].lower())

    def test_negative_size_usd(self):
        """Negative size_usd should be rejected."""
        signal = _make_signal(size_usd=-10.0)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("size_usd", result["rejection_reason"].lower())

    def test_negative_edge_estimate(self):
        """Negative edge_estimate should be rejected."""
        signal = _make_signal(edge_estimate=-0.05)
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("edge_estimate", result["rejection_reason"].lower())

    def test_invalid_side(self):
        """An unrecognised side value should be rejected."""
        signal = _make_signal(side="sideways")
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("side", result["rejection_reason"].lower())

    def test_non_numeric_target_price(self):
        """A non-numeric target_price should be rejected."""
        signal = _make_signal(target_price="abc")
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("target_price", result["rejection_reason"].lower())

    def test_non_numeric_size_usd(self):
        """A non-numeric size_usd should be rejected."""
        signal = _make_signal(size_usd="big")
        result = asyncio.run(self.sizer.compute_size(signal))
        self.assertFalse(result["approved"])
        self.assertIn("size_usd", result["rejection_reason"].lower())


class TestClearMockState(unittest.TestCase):
    """The clear_mock_state helper should reset all in-memory state."""

    def test_clear_resets_trades_and_drawdown(self):
        """After clear_mock_state, all mock trades and drawdown should be zero."""
        config = MockConfig()
        sizer = PositionSizer(config, test_mode=True)
        sizer.inject_mock_trade({
            "strategy": "s1", "venue": "polymarket", "market_id": "0x1",
            "side": "yes", "price": 0.5, "size": 100.0, "pnl": 0.0,
            "status": "filled",
            "time": datetime.now(timezone.utc),
        })
        sizer.set_mock_drawdown(0.10)

        sizer.clear_mock_state()

        self.assertEqual(len(sizer._mock_trades), 0)
        self.assertEqual(sizer._drawdown_pct, 0.0)
        self.assertFalse(sizer._drawdown_override)
        self.assertEqual(sizer._bankroll, config.INITIAL_BANKROLL)


if __name__ == "__main__":
    unittest.main()
