#!/usr/bin/env python3
"""
test_pipeline.py -- Phase 6 full-pipeline integration test.

Exercises the complete order-routing pipeline end-to-end using only
in-memory mocks (InMemoryRedis for Redis, _MockPool for PostgreSQL).
No external services are required.

Pipeline under test:
    fixture signals -> stream:orders:pending -> OrderRouter._run_test_mode()
        -> validation -> deduplication -> risk check -> position sizing
        -> mock connector -> insert_trade (mock DB) -> update_trade
        -> stream:orders:filled

Verified behaviours:
    1. Valid signals flow through and produce trades in the mock DB.
    2. Fill confirmations are published to stream:orders:filled.
    3. Duplicate signals (same strategy, market_id, side within 60s) are
       deduplicated and only the first instance produces a trade.
    4. Invalid signals (missing required fields) are rejected and do NOT
       produce trades.
    5. Cleanup (close_redis, close_pool) completes without error.

Usage:
    python -m pytest tests/integration/test_pipeline.py -v
    python -m unittest tests.integration.test_pipeline -v
"""

import asyncio
import sys
import unittest
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so that 'execution.*' imports work.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Force --test mode on the Config singleton *before* any execution.* import
# touches it.  This prevents the config module from requiring a .env file.
# ---------------------------------------------------------------------------
if "--test" not in sys.argv:
    sys.argv.append("--test")

# Now safe to import execution modules.
import execution.utils.redis_client as rc
import execution.utils.db as db_mod
from execution.utils.redis_client import (
    STREAM_ORDERS_PENDING,
    STREAM_ORDERS_FILLED,
    GROUP_EXECUTION,
    publish,
    close_redis,
)
from execution.utils.db import close_pool, get_pool
from execution.execution_engine.order_router import (
    OrderRouter,
    _FIXTURE_SIGNALS,
)


# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

# The canonical fixture signals are defined in order_router._FIXTURE_SIGNALS.
# We reference them directly to stay in sync with the source of truth.
#
# Layout (7 signals):
#   [0] s1  polymarket  0xabc123def456    yes   -- valid
#   [1] s2  polymarket  0x789ghi012jkl    yes   -- valid
#   [2] s3  kalshi      KXBTC-26MAR28-100K yes  -- valid
#   [3] s4  polymarket  0xmno345pqr678    no    -- valid
#   [4] s5  cross       ...               yes|no -- valid (cross-venue)
#   [5] s1  polymarket  0xabc123def456    yes   -- DUPLICATE of [0]
#   [6] s1  polymarket  (missing market_id)      -- INVALID

VALID_SIGNAL_COUNT = 5       # signals [0]...[4]
DUPLICATE_INDEX = 5          # signal [5] is a dup of [0]
INVALID_INDEX = 6            # signal [6] is missing market_id
TOTAL_FIXTURE_COUNT = 7      # len(_FIXTURE_SIGNALS)


# ===========================================================================
#  Test case
# ===========================================================================

class TestFullPipeline(unittest.TestCase):
    """End-to-end integration test for the order-routing pipeline.

    Runs the entire pipeline with in-memory mocks and verifies that:
      - valid signals produce trade rows in the mock DB
      - fill events appear on stream:orders:filled
      - duplicate signals are deduplicated
      - invalid signals are rejected
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _enable_test_backends() -> None:
        """Switch redis_client and db to their in-memory mocks."""
        # Redis: use InMemoryRedis
        rc._use_mock = True
        rc._redis_instance = None

        # DB: use _MockPool
        db_mod._test_mode = True
        db_mod._pool = None
        db_mod._pool_lock = None

    @staticmethod
    async def _cleanup() -> None:
        """Tear down singleton connections."""
        await close_redis()
        await close_pool()

    # ------------------------------------------------------------------
    # The actual async pipeline exercise
    # ------------------------------------------------------------------

    @staticmethod
    async def _run_pipeline() -> dict:
        """Execute the full pipeline and return collected assertions data.

        Returns a dict with:
            trades           -- list[dict] of rows from mock DB
            filled_stream    -- list of (msg_id, fields) on stream:orders:filled
            pending_len      -- int, length of stream:orders:pending after run
            processed_count  -- int, total signals consumed by the router
        """

        # 1. Enable mock backends ----------------------------------------
        TestFullPipeline._enable_test_backends()

        # 2. Create and run the OrderRouter in test mode -----------------
        #    _run_test_mode() publishes the fixture signals to
        #    stream:orders:pending, then consumes and processes them.
        router = OrderRouter(test_mode=True, dry_run=False)
        await router.run()

        # 3. Collect trades from the mock DB pool ------------------------
        pool = await get_pool()
        trades = list(pool._trades)

        # 4. Read fill events from stream:orders:filled ------------------
        #    The InMemoryRedis stores raw stream entries; read them directly
        #    so we do not need a consumer group on the filled stream.
        redis = await rc.get_redis()
        filled_entries = redis._streams.get(STREAM_ORDERS_FILLED, [])

        # 5. Read pending stream length ----------------------------------
        pending_len = len(redis._streams.get(STREAM_ORDERS_PENDING, []))

        return {
            "trades": trades,
            "filled_stream": filled_entries,
            "pending_len": pending_len,
        }

    # ------------------------------------------------------------------
    # Test method
    # ------------------------------------------------------------------

    def test_full_pipeline(self) -> None:
        """Run the full pipeline and verify all expectations."""

        async def _run() -> dict:
            try:
                return await self._run_pipeline()
            finally:
                await self._cleanup()

        data = asyncio.run(_run())

        trades = data["trades"]
        filled = data["filled_stream"]

        # -- Assertion group 1: trades were logged to the mock DB --------
        # We expect exactly VALID_SIGNAL_COUNT trades (5 unique valid
        # signals).  The duplicate (index 5) and invalid (index 6) should
        # NOT produce trade rows.
        self.assertEqual(
            len(trades),
            VALID_SIGNAL_COUNT,
            f"Expected {VALID_SIGNAL_COUNT} trades in mock DB, got {len(trades)}. "
            f"Trades: {trades}",
        )

        # Verify every trade has a positive id and expected fields.
        for trade in trades:
            self.assertIn("id", trade)
            self.assertGreater(trade["id"], 0)
            self.assertIn("venue", trade)
            self.assertIn("market_id", trade)
            self.assertIn("strategy", trade)
            self.assertIn("side", trade)
            self.assertIn("price", trade)
            self.assertIn("size", trade)
            self.assertIn("status", trade)

        # All trades should have been updated to a terminal status
        # (filled, partial, failed, timeout) -- none should remain
        # 'pending' because the router always calls update_trade.
        for trade in trades:
            self.assertNotEqual(
                trade["status"],
                "pending",
                f"Trade id={trade['id']} still has status 'pending' -- "
                "update_trade was not called.",
            )

        # Verify the strategies that were routed match the valid fixture
        # signals.  The first 5 fixture signals have strategies
        # s1, s2, s3, s4, s5.
        expected_strategies = {"s1", "s2", "s3", "s4", "s5"}
        actual_strategies = {t["strategy"] for t in trades}
        self.assertEqual(
            actual_strategies,
            expected_strategies,
            f"Expected strategies {expected_strategies}, got {actual_strategies}",
        )

        # -- Assertion group 2: fill events on stream:orders:filled ------
        # Every successfully filled/partial trade should have a
        # corresponding event.  At a minimum, all 5 valid signals are
        # expected to reach the 'filled' status because the mock
        # connector always returns status='filled'.
        self.assertGreaterEqual(
            len(filled),
            VALID_SIGNAL_COUNT,
            f"Expected >= {VALID_SIGNAL_COUNT} fill events on "
            f"stream:orders:filled, got {len(filled)}.",
        )

        # Each fill event should contain key fields.
        for _msg_id, fields in filled:
            self.assertIn("trade_id", fields)
            self.assertIn("strategy", fields)
            self.assertIn("venue", fields)
            self.assertIn("market_id", fields)
            self.assertIn("side", fields)
            self.assertIn("fill_price", fields)
            self.assertIn("size_usd", fields)
            self.assertIn("status", fields)
            self.assertIn("timestamp", fields)
            # status should be 'filled' or 'partial'
            self.assertIn(
                fields["status"],
                ("filled", "partial"),
                f"Fill event status should be 'filled' or 'partial', "
                f"got {fields['status']!r}",
            )

        # -- Assertion group 3: duplicate signals were deduplicated ------
        # Signal at DUPLICATE_INDEX has the same (strategy='s1',
        # market_id='0xabc123def456', side='yes') as signal [0].
        # Only one trade for that (strategy, market_id, side) tuple
        # should exist.
        dup_signal = _FIXTURE_SIGNALS[DUPLICATE_INDEX]
        dup_key = (
            dup_signal["strategy"],
            dup_signal["market_id"],
            dup_signal["side"],
        )
        matching_trades = [
            t for t in trades
            if (t["strategy"], t["market_id"], t["side"]) == dup_key
        ]
        self.assertEqual(
            len(matching_trades),
            1,
            f"Expected exactly 1 trade for dedup key {dup_key}, "
            f"got {len(matching_trades)}. Deduplication failed.",
        )

        # -- Assertion group 4: invalid signals were rejected ------------
        # Signal at INVALID_INDEX is missing 'market_id'.  It should NOT
        # appear in the trades table at all.
        invalid_signal = _FIXTURE_SIGNALS[INVALID_INDEX]
        # The invalid signal has no market_id key, so there should be no
        # trade with a matching strategy + side that lacks a real
        # market_id.
        #
        # More robustly: the total trade count already asserts this
        # (5 trades for 7 signals means 2 were rejected), but we also
        # confirm that no trade has the exact field set of the invalid
        # signal.
        invalid_strategy = invalid_signal.get("strategy", "")
        invalid_side = invalid_signal.get("side", "")
        # The invalid signal has no market_id at all; any trade that
        # exists for s1 must have a real market_id.
        for trade in trades:
            if trade["strategy"] == invalid_strategy and trade["side"] == invalid_side:
                self.assertNotEqual(
                    trade["market_id"],
                    "?",
                    "A trade was created for the invalid signal "
                    "(missing market_id). Validation failed.",
                )

        # -- Assertion group 5: pending stream was populated -------------
        # The fixture signals were all published to stream:orders:pending.
        self.assertEqual(
            data["pending_len"],
            TOTAL_FIXTURE_COUNT,
            f"Expected {TOTAL_FIXTURE_COUNT} entries on "
            f"stream:orders:pending, got {data['pending_len']}.",
        )


# ===========================================================================
#  Entry point
# ===========================================================================

if __name__ == "__main__":
    unittest.main()
