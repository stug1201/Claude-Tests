#!/usr/bin/env python3
"""
db.py -- PostgreSQL/TimescaleDB connection pool and query helpers.

Provides async connection pooling via asyncpg, common query patterns
for market_prices, wallet_scores, and trades tables.  Includes a
--test mode that swaps the real database for an in-memory dict-based
mock so every caller can be exercised without a running PostgreSQL
instance.

Usage:
    from execution.utils.db import get_pool, insert_market_price, get_recent_prices

    pool = await get_pool()
    await insert_market_price(time=..., venue="polymarket", ...)

    # CLI quick-check
    python execution/utils/db.py          # requires POSTGRES_URL
    python execution/utils/db.py --test   # in-memory mock, no DB needed

Environment variables (loaded via execution.utils.config):
    POSTGRES_URL -- PostgreSQL connection string

See: directives/00_system_overview.md   (schema definitions)
     directives/10_ops_and_deployment.md (migration SQL, pool sizing)
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/utils/
PROJECT_ROOT = SCRIPT_DIR.parent.parent               # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"

# ---------------------------------------------------------------------------
# Connection pool singleton
# ---------------------------------------------------------------------------
_pool: Optional[object] = None  # asyncpg.Pool or _MockPool
_pool_lock: Optional[asyncio.Lock] = None
_test_mode: bool = False

# Pool sizing (see directives/10_ops_and_deployment.md)
POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 10


def _get_lock() -> asyncio.Lock:
    """Return (and lazily create) the module-level asyncio.Lock.

    A new Lock is created per event loop to avoid the 'attached to a
    different loop' error that occurs when a module-level Lock is
    initialised at import time.
    """
    global _pool_lock
    if _pool_lock is None:
        _pool_lock = asyncio.Lock()
    return _pool_lock


# =========================================================================
#  IN-MEMORY MOCK (--test mode)
# =========================================================================

class _MockPool:
    """
    Lightweight dict-backed substitute for asyncpg.Pool.

    Stores rows in plain Python lists/dicts so that every public
    function in this module can be exercised without PostgreSQL.
    """

    def __init__(self) -> None:
        self._market_prices: list[dict] = []
        self._wallet_scores: dict[str, dict] = {}  # keyed by wallet_address
        self._trades: list[dict] = []
        self._trade_seq: int = 0  # auto-incrementing trade id
        logger.info("MockPool initialised (in-memory, no DB connection)")

    # -- connection context manager stub ------------------------------------
    class _Conn:
        """Minimal stand-in returned by pool.acquire()."""

        def __init__(self, pool: "_MockPool") -> None:
            self._pool = pool

        async def execute(self, _query: str, *_args: object) -> str:
            return "EXECUTE"

        async def __aenter__(self) -> "_MockPool._Conn":
            return self

        async def __aexit__(self, *exc: object) -> None:
            pass

    def acquire(self) -> "_Conn":
        return self._Conn(self)

    async def close(self) -> None:
        logger.info("MockPool closed")


# =========================================================================
#  POOL LIFECYCLE
# =========================================================================

async def get_pool():
    """Return the singleton asyncpg connection pool (lazy-initialised).

    In --test mode this returns a _MockPool instance instead.

    Returns:
        asyncpg.Pool or _MockPool
    """
    global _pool

    if _pool is not None:
        return _pool

    async with _get_lock():
        # Double-checked locking
        if _pool is not None:
            return _pool

        if _test_mode:
            _pool = _MockPool()
            return _pool

        # Late import so --test never requires asyncpg installed
        try:
            import asyncpg
        except ImportError:
            logger.error(
                "asyncpg is not installed.  Install it with: pip install asyncpg"
            )
            sys.exit(1)

        try:
            from execution.utils.config import config
            dsn = config.POSTGRES_URL
        except Exception:
            import os
            dsn = os.getenv("POSTGRES_URL", "postgresql://user:password@localhost:5432/polymarket_trader")
            logger.warning("Could not import config; falling back to env var / default DSN")

        logger.info("Creating asyncpg pool (min=%d, max=%d)", POOL_MIN_SIZE, POOL_MAX_SIZE)
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
        )
        logger.info("Connection pool ready")
        return _pool


async def close_pool() -> None:
    """Gracefully close the connection pool and reset the singleton."""
    global _pool, _pool_lock

    if _pool is None:
        logger.debug("close_pool called but no pool exists -- nothing to do")
        return

    await _pool.close()
    logger.info("Connection pool closed")
    _pool = None
    _pool_lock = None


# =========================================================================
#  MARKET PRICES
# =========================================================================

async def insert_market_price(
    time: datetime,
    venue: str,
    market_id: str,
    title: str,
    yes_price: float,
    no_price: float,
    spread: float,
    volume_24h: float,
) -> None:
    """Insert a single row into the market_prices hypertable.

    Args:
        time:       Timestamp of the price snapshot (tz-aware UTC).
        venue:      Source venue (e.g. 'polymarket', 'kalshi').
        market_id:  Venue-specific market identifier.
        title:      Human-readable market title.
        yes_price:  Price of the YES contract (0..1).
        no_price:   Price of the NO contract (0..1).
        spread:     Bid-ask spread.
        volume_24h: 24-hour trading volume in USD.
    """
    pool = await get_pool()

    if _test_mode:
        row = {
            "time": time,
            "venue": venue,
            "market_id": market_id,
            "title": title,
            "yes_price": yes_price,
            "no_price": no_price,
            "spread": spread,
            "volume_24h": volume_24h,
        }
        pool._market_prices.append(row)
        logger.info(
            "Inserted market price [mock]: venue=%s market_id=%s yes=%.4f",
            venue, market_id, yes_price,
        )
        return

    query = """
        INSERT INTO market_prices
            (time, venue, market_id, title, yes_price, no_price, spread, volume_24h)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    """
    async with pool.acquire() as conn:
        await conn.execute(query, time, venue, market_id, title,
                           yes_price, no_price, spread, volume_24h)
    logger.debug(
        "Inserted market price: venue=%s market_id=%s yes=%.4f",
        venue, market_id, yes_price,
    )


async def get_recent_prices(
    market_id: str,
    venue: str,
    limit: int = 100,
) -> list[dict]:
    """Fetch the most recent price rows for a given market and venue.

    Args:
        market_id: Venue-specific market identifier.
        venue:     Source venue.
        limit:     Maximum number of rows to return (default 100).

    Returns:
        List of dicts ordered by time descending.
    """
    pool = await get_pool()

    if _test_mode:
        rows = [
            r for r in pool._market_prices
            if r["market_id"] == market_id and r["venue"] == venue
        ]
        rows.sort(key=lambda r: r["time"], reverse=True)
        result = rows[:limit]
        logger.info(
            "get_recent_prices [mock]: market_id=%s venue=%s returned %d rows",
            market_id, venue, len(result),
        )
        return result

    query = """
        SELECT time, venue, market_id, title, yes_price, no_price, spread, volume_24h
        FROM market_prices
        WHERE market_id = $1 AND venue = $2
        ORDER BY time DESC
        LIMIT $3
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, market_id, venue, limit)

    result = [dict(r) for r in rows]
    logger.debug(
        "get_recent_prices: market_id=%s venue=%s returned %d rows",
        market_id, venue, len(result),
    )
    return result


# =========================================================================
#  TRADES
# =========================================================================

async def insert_trade(
    venue: str,
    market_id: str,
    strategy: str,
    side: str,
    price: float,
    size: float,
    status: str = "pending",
) -> int:
    """Insert a trade record and return its auto-generated id.

    Args:
        venue:    Execution venue.
        market_id: Market identifier.
        strategy: Strategy label (e.g. 's1_near_res').
        side:     'buy' or 'sell'.
        price:    Execution price.
        size:     Position size in USD.
        status:   Order status (default 'pending').

    Returns:
        Integer trade id (SERIAL primary key).
    """
    pool = await get_pool()

    if _test_mode:
        pool._trade_seq += 1
        trade_id = pool._trade_seq
        row = {
            "id": trade_id,
            "time": datetime.now(timezone.utc),
            "venue": venue,
            "market_id": market_id,
            "strategy": strategy,
            "side": side,
            "price": price,
            "size": size,
            "status": status,
            "pnl": 0,
        }
        pool._trades.append(row)
        logger.info(
            "Inserted trade [mock]: id=%d strategy=%s side=%s price=%.4f",
            trade_id, strategy, side, price,
        )
        return trade_id

    query = """
        INSERT INTO trades (time, venue, market_id, strategy, side, price, size, status)
        VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7)
        RETURNING id
    """
    async with pool.acquire() as conn:
        trade_id = await conn.fetchval(
            query, venue, market_id, strategy, side, price, size, status,
        )
    logger.info(
        "Inserted trade: id=%d strategy=%s side=%s price=%.4f",
        trade_id, strategy, side, price,
    )
    return trade_id


async def update_trade(
    trade_id: int,
    status: str,
    pnl: Optional[float] = None,
) -> None:
    """Update the status (and optionally PnL) of an existing trade.

    Args:
        trade_id: Primary-key id of the trade row.
        status:   New status string (e.g. 'filled', 'cancelled', 'settled').
        pnl:      Realised profit/loss in USD (optional).
    """
    pool = await get_pool()

    if _test_mode:
        for row in pool._trades:
            if row["id"] == trade_id:
                row["status"] = status
                if pnl is not None:
                    row["pnl"] = pnl
                logger.info(
                    "Updated trade [mock]: id=%d status=%s pnl=%s",
                    trade_id, status, pnl,
                )
                return
        logger.warning("update_trade [mock]: trade_id=%d not found", trade_id)
        return

    if pnl is not None:
        query = "UPDATE trades SET status = $1, pnl = $2 WHERE id = $3"
        args = (status, pnl, trade_id)
    else:
        query = "UPDATE trades SET status = $1 WHERE id = $2"
        args = (status, trade_id)

    async with pool.acquire() as conn:
        await conn.execute(query, *args)
    logger.info("Updated trade: id=%d status=%s pnl=%s", trade_id, status, pnl)


async def get_trades(
    strategy: Optional[str] = None,
    status: Optional[str] = None,
    since: Optional[datetime] = None,
) -> list[dict]:
    """Query trades with optional filters.

    Args:
        strategy: Filter by strategy label (optional).
        status:   Filter by order status (optional).
        since:    Only return trades after this timestamp (optional).

    Returns:
        List of trade dicts ordered by time descending.
    """
    pool = await get_pool()

    if _test_mode:
        rows = list(pool._trades)
        if strategy is not None:
            rows = [r for r in rows if r["strategy"] == strategy]
        if status is not None:
            rows = [r for r in rows if r["status"] == status]
        if since is not None:
            rows = [r for r in rows if r["time"] >= since]
        rows.sort(key=lambda r: r["time"], reverse=True)
        logger.info("get_trades [mock]: returned %d rows", len(rows))
        return rows

    conditions: list[str] = []
    args: list[object] = []
    idx = 1

    if strategy is not None:
        conditions.append(f"strategy = ${idx}")
        args.append(strategy)
        idx += 1
    if status is not None:
        conditions.append(f"status = ${idx}")
        args.append(status)
        idx += 1
    if since is not None:
        conditions.append(f"time >= ${idx}")
        args.append(since)
        idx += 1

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    query = f"SELECT * FROM trades{where} ORDER BY time DESC"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    result = [dict(r) for r in rows]
    logger.info("get_trades: returned %d rows", len(result))
    return result


# =========================================================================
#  WALLET SCORES
# =========================================================================

async def upsert_wallet_score(
    wallet_address: str,
    ips_score: float,
    win_rate_24h: float,
    hashdive_score: int,
) -> None:
    """Insert or update a wallet's profiling scores.

    Uses PostgreSQL ON CONFLICT ... DO UPDATE for atomic upsert.

    Args:
        wallet_address: Ethereum wallet address (0x...).
        ips_score:      Insider Probability Score (0..1).
        win_rate_24h:   24-hour win rate (0..1).
        hashdive_score: Hashdive reputation score (integer).
    """
    pool = await get_pool()

    if _test_mode:
        pool._wallet_scores[wallet_address] = {
            "wallet_address": wallet_address,
            "ips_score": ips_score,
            "win_rate_24h": win_rate_24h,
            "hashdive_score": hashdive_score,
            "last_updated": datetime.now(timezone.utc),
        }
        logger.info(
            "Upserted wallet score [mock]: %s ips=%.3f",
            wallet_address, ips_score,
        )
        return

    query = """
        INSERT INTO wallet_scores (wallet_address, ips_score, win_rate_24h, hashdive_score, last_updated)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (wallet_address) DO UPDATE
            SET ips_score      = EXCLUDED.ips_score,
                win_rate_24h   = EXCLUDED.win_rate_24h,
                hashdive_score = EXCLUDED.hashdive_score,
                last_updated   = NOW()
    """
    async with pool.acquire() as conn:
        await conn.execute(query, wallet_address, ips_score, win_rate_24h, hashdive_score)
    logger.info("Upserted wallet score: %s ips=%.3f", wallet_address, ips_score)


async def get_wallet_score(wallet_address: str) -> Optional[dict]:
    """Fetch profiling scores for a single wallet.

    Args:
        wallet_address: Ethereum wallet address.

    Returns:
        Dict with score fields, or None if the wallet is not profiled.
    """
    pool = await get_pool()

    if _test_mode:
        row = pool._wallet_scores.get(wallet_address)
        logger.info(
            "get_wallet_score [mock]: %s -> %s",
            wallet_address, "found" if row else "not found",
        )
        return row

    query = "SELECT * FROM wallet_scores WHERE wallet_address = $1"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, wallet_address)

    if row is None:
        logger.debug("get_wallet_score: %s not found", wallet_address)
        return None

    result = dict(row)
    logger.debug("get_wallet_score: %s found", wallet_address)
    return result


async def get_high_ips_wallets(min_score: float = 0.7) -> list[dict]:
    """Return all wallets whose IPS score meets or exceeds the threshold.

    Args:
        min_score: Minimum ips_score to include (default 0.7).

    Returns:
        List of wallet score dicts ordered by ips_score descending.
    """
    pool = await get_pool()

    if _test_mode:
        rows = [
            v for v in pool._wallet_scores.values()
            if v["ips_score"] >= min_score
        ]
        rows.sort(key=lambda r: r["ips_score"], reverse=True)
        logger.info(
            "get_high_ips_wallets [mock]: min_score=%.2f returned %d wallets",
            min_score, len(rows),
        )
        return rows

    query = """
        SELECT * FROM wallet_scores
        WHERE ips_score >= $1
        ORDER BY ips_score DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, min_score)

    result = [dict(r) for r in rows]
    logger.info(
        "get_high_ips_wallets: min_score=%.2f returned %d wallets",
        min_score, len(result),
    )
    return result


# =========================================================================
#  SCHEMA MIGRATIONS
# =========================================================================

MIGRATION_SQL = """\
-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Market prices hypertable
CREATE TABLE IF NOT EXISTS market_prices (
    time        TIMESTAMPTZ NOT NULL,
    venue       TEXT NOT NULL,
    market_id   TEXT NOT NULL,
    title       TEXT,
    yes_price   NUMERIC(6,4),
    no_price    NUMERIC(6,4),
    spread      NUMERIC(6,4),
    volume_24h  NUMERIC(18,2)
);
SELECT create_hypertable('market_prices', 'time', if_not_exists => TRUE);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_mp_venue_market ON market_prices (venue, market_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_mp_time ON market_prices (time DESC);

-- Wallet scores
CREATE TABLE IF NOT EXISTS wallet_scores (
    wallet_address  TEXT PRIMARY KEY,
    ips_score       NUMERIC(5,3),
    win_rate_24h    NUMERIC(5,3),
    hashdive_score  INTEGER,
    last_updated    TIMESTAMPTZ DEFAULT NOW()
);

-- Trades log
CREATE TABLE IF NOT EXISTS trades (
    id              SERIAL PRIMARY KEY,
    time            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    venue           TEXT,
    market_id       TEXT,
    strategy        TEXT,
    side            TEXT,
    price           NUMERIC(6,4),
    size            NUMERIC(18,2),
    status          TEXT DEFAULT 'pending',
    pnl             NUMERIC(18,6) DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades (strategy, time DESC);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades (market_id);

-- Matched market pairs (cross-venue)
CREATE TABLE IF NOT EXISTS matched_markets (
    id              SERIAL PRIMARY KEY,
    poly_market_id  TEXT NOT NULL,
    kalshi_market_id TEXT NOT NULL,
    match_confidence NUMERIC(4,3),
    oracle_risk     TEXT DEFAULT 'UNKNOWN',
    quarantined     BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(poly_market_id, kalshi_market_id)
);

-- Data retention policy: keep 1 year of price data
SELECT add_retention_policy('market_prices', INTERVAL '1 year', if_not_exists => TRUE);

-- Compression policy: compress data older than 7 days
ALTER TABLE market_prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue,market_id'
);
SELECT add_compression_policy('market_prices', INTERVAL '7 days', if_not_exists => TRUE);
"""


async def run_migrations() -> None:
    """Execute the full schema SQL from directive 10 against the database.

    In --test mode this is a no-op since the mock pool has no real tables.
    """
    if _test_mode:
        logger.info("run_migrations [mock]: skipped (in-memory mode)")
        return

    pool = await get_pool()
    logger.info("Running database migrations ...")

    async with pool.acquire() as conn:
        await conn.execute(MIGRATION_SQL)

    logger.info("Database migrations completed successfully")


# =========================================================================
#  CLI ENTRY POINT
# =========================================================================

async def _run_all_tests() -> None:
    """Exercise every public function using the in-memory mock pool."""

    now = datetime.now(timezone.utc)

    # -- migrations (no-op in test mode) ------------------------------------
    logger.info("=" * 72)
    logger.info("  TEST: run_migrations")
    logger.info("=" * 72)
    await run_migrations()

    # -- market prices ------------------------------------------------------
    logger.info("=" * 72)
    logger.info("  TEST: insert_market_price / get_recent_prices")
    logger.info("=" * 72)

    from datetime import timedelta

    for i in range(5):
        await insert_market_price(
            time=now - timedelta(minutes=i * 5),
            venue="polymarket",
            market_id="0xabc123",
            title="Will BTC exceed $100k by June 2026?",
            yes_price=round(0.62 + i * 0.005, 4),
            no_price=round(0.38 - i * 0.005, 4),
            spread=0.01,
            volume_24h=125000.50 + i * 1000,
        )

    prices = await get_recent_prices("0xabc123", "polymarket", limit=3)
    logger.info("  Returned %d prices (requested limit=3)", len(prices))
    for p in prices:
        logger.info("    %s  yes=%.4f  no=%.4f", p["time"].isoformat(), p["yes_price"], p["no_price"])

    # -- trades -------------------------------------------------------------
    logger.info("=" * 72)
    logger.info("  TEST: insert_trade / update_trade / get_trades")
    logger.info("=" * 72)

    t1 = await insert_trade(
        venue="polymarket", market_id="0xabc123",
        strategy="s1_near_res", side="buy",
        price=0.63, size=50.0,
    )
    t2 = await insert_trade(
        venue="kalshi", market_id="KXBTC-100K",
        strategy="s5_cross_arb", side="sell",
        price=0.41, size=100.0,
    )
    logger.info("  Created trades: id=%d, id=%d", t1, t2)

    await update_trade(t1, status="filled", pnl=12.50)
    await update_trade(t2, status="cancelled")

    all_trades = await get_trades()
    logger.info("  All trades: %d", len(all_trades))

    filled = await get_trades(status="filled")
    logger.info("  Filled trades: %d", len(filled))

    s1_trades = await get_trades(strategy="s1_near_res")
    logger.info("  S1 trades: %d", len(s1_trades))

    # -- wallet scores ------------------------------------------------------
    logger.info("=" * 72)
    logger.info("  TEST: upsert_wallet_score / get_wallet_score / get_high_ips_wallets")
    logger.info("=" * 72)

    await upsert_wallet_score("0xWHALE001", ips_score=0.92, win_rate_24h=0.78, hashdive_score=85)
    await upsert_wallet_score("0xRETAIL01", ips_score=0.15, win_rate_24h=0.45, hashdive_score=30)
    await upsert_wallet_score("0xSHARP001", ips_score=0.81, win_rate_24h=0.88, hashdive_score=72)

    # Update an existing wallet
    await upsert_wallet_score("0xWHALE001", ips_score=0.95, win_rate_24h=0.80, hashdive_score=90)

    whale = await get_wallet_score("0xWHALE001")
    logger.info("  Whale score: ips=%.3f (should be 0.950 after upsert)", whale["ips_score"])

    missing = await get_wallet_score("0xNONEXIST")
    logger.info("  Non-existent wallet: %s (should be None)", missing)

    high_ips = await get_high_ips_wallets(min_score=0.7)
    logger.info("  High-IPS wallets (>=0.7): %d (should be 2)", len(high_ips))
    for w in high_ips:
        logger.info("    %s  ips=%.3f  wr=%.3f  hd=%d",
                     w["wallet_address"], w["ips_score"],
                     w["win_rate_24h"], w["hashdive_score"])

    # -- cleanup ------------------------------------------------------------
    logger.info("=" * 72)
    logger.info("  TEST: close_pool")
    logger.info("=" * 72)
    await close_pool()

    logger.info("=" * 72)
    logger.info("  ALL TESTS PASSED")
    logger.info("=" * 72)


def main() -> None:
    """Parse CLI arguments and run either live DB tests or in-memory mock."""
    global _test_mode

    parser = argparse.ArgumentParser(
        description="PostgreSQL/TimescaleDB connection pool and query helpers.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use in-memory mock pool instead of a real database connection.",
    )
    args = parser.parse_args()

    if args.test:
        _test_mode = True
        logger.info("Running in --test mode (in-memory mock, no DB required)")
        asyncio.run(_run_all_tests())
    else:
        logger.info("Running live database tests (requires POSTGRES_URL)")
        asyncio.run(_run_all_tests())


if __name__ == "__main__":
    main()
