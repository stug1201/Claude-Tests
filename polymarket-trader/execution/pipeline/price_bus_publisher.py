#!/usr/bin/env python3
"""
price_bus_publisher.py -- Unified Price Bus Publisher.

Consumes raw price ticks from venue-specific Redis Streams
(stream:prices:polymarket and stream:prices:kalshi), normalises them
into unified MarketPrice objects, enriches with metadata from
PostgreSQL, validates price ranges, deduplicates within a 1-second
window, publishes to stream:prices:normalised, and persists every
normalised tick to the market_prices hypertable for historical queries.

Runs as a long-lived process with an infinite consume loop.

MarketPrice schema (normalised):
    {
        "time":       "ISO 8601 timestamp",
        "venue":      "polymarket" | "kalshi",
        "market_id":  "string",
        "title":      "string",
        "yes_price":  float  (0.0 - 1.0),
        "no_price":   float  (0.0 - 1.0),
        "spread":     float  (yes + no - 1.0, i.e. overround),
        "volume_24h": float
    }

Venue-specific quirks handled:
    - Polymarket: prices in USDC (6 decimals, already 0-1 range).
      Fields: token_id / condition_id, outcome_prices (list), title,
      volume / volume_24h, timestamp.
    - Kalshi: prices in cents (integer 0-100).  Divided by 100 to get
      [0.0, 1.0].  Fields: ticker / market_ticker, yes_price, no_price,
      title, volume / volume_24h, ts.

Deduplication: same (venue, market_id) within 1 second is skipped.
Validation: yes_price and no_price must be in [0.0, 1.0]; NaN/None rejected.
Clock skew: ticks >5 s in the future use server time with a warning.

Usage:
    python execution/pipeline/price_bus_publisher.py          # Live mode
    python execution/pipeline/price_bus_publisher.py --test   # Fixture mode

See: directives/02_unified_price_bus.md
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/pipeline/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"

# ---------------------------------------------------------------------------
# Stream & group constants (mirrors redis_client.py)
# ---------------------------------------------------------------------------
STREAM_PRICES_POLYMARKET = "stream:prices:polymarket"
STREAM_PRICES_KALSHI = "stream:prices:kalshi"
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
GROUP_PRICE_BUS = "price_bus_group"
CONSUMER_NAME = "price_bus_publisher_1"

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------
CONSUME_BATCH_SIZE = 50          # messages per XREADGROUP call
CONSUME_BLOCK_MS = 1000          # block timeout in milliseconds
STREAM_MAXLEN = 100_000          # approximate max entries per stream
DEDUP_WINDOW_SEC = 1.0           # dedup window in seconds
CLOCK_SKEW_LIMIT_SEC = 5.0      # warn if tick is this far in the future
METADATA_REFRESH_SEC = 300       # refresh PostgreSQL metadata cache every 5 min

# ---------------------------------------------------------------------------
# Venue identifiers
# ---------------------------------------------------------------------------
VENUE_POLYMARKET = "polymarket"
VENUE_KALSHI = "kalshi"

VENUE_STREAM_MAP = {
    STREAM_PRICES_POLYMARKET: VENUE_POLYMARKET,
    STREAM_PRICES_KALSHI: VENUE_KALSHI,
}


# ---------------------------------------------------------------------------
# Deduplication tracker
# ---------------------------------------------------------------------------

class DeduplicationTracker:
    """
    Track recently published (venue, market_id) keys to suppress
    duplicate ticks within a configurable time window.

    Uses a dict of ``(venue, market_id) -> last_published_epoch``.
    Stale entries are purged lazily every 1000 checks.
    """

    def __init__(self, window_sec: float = DEDUP_WINDOW_SEC) -> None:
        self._window = window_sec
        self._seen: dict[tuple[str, str], float] = {}
        self._check_count = 0

    def is_duplicate(self, venue: str, market_id: str) -> bool:
        """Return True if the same key was seen within the dedup window."""
        key = (venue, market_id)
        now = _time.monotonic()
        last = self._seen.get(key)
        if last is not None and (now - last) < self._window:
            return True
        self._seen[key] = now
        self._check_count += 1
        if self._check_count % 1000 == 0:
            self._purge(now)
        return False

    def _purge(self, now: float) -> None:
        """Remove entries older than twice the dedup window."""
        cutoff = now - (self._window * 2)
        stale = [k for k, v in self._seen.items() if v < cutoff]
        for k in stale:
            del self._seen[k]
        if stale:
            logger.debug("Dedup tracker purged %d stale entries.", len(stale))


# ---------------------------------------------------------------------------
# Metadata cache (market titles / close times from PostgreSQL)
# ---------------------------------------------------------------------------

class MetadataCache:
    """
    In-memory cache of market metadata fetched from PostgreSQL.

    Refreshes every METADATA_REFRESH_SEC.  On failure, continues
    with stale data and logs an ERROR.
    """

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._last_refresh: float = 0.0

    async def get(self, venue: str, market_id: str) -> dict[str, Any]:
        """Return metadata dict for the given market, or empty dict."""
        await self._maybe_refresh()
        return self._cache.get((venue, market_id), {})

    async def _maybe_refresh(self) -> None:
        """Refresh cache from PostgreSQL if stale."""
        now = _time.monotonic()
        if (now - self._last_refresh) < METADATA_REFRESH_SEC:
            return
        try:
            await self._refresh()
            self._last_refresh = now
        except Exception as exc:
            logger.error(
                "Metadata cache refresh failed (using stale data): %s", exc
            )

    async def _refresh(self) -> None:
        """
        Fetch recent prices from PostgreSQL to populate title / close_time.

        This is a lightweight approach: we pull the most recent row per
        (venue, market_id) from the market_prices table.  For a production
        system with a dedicated markets table this would query that instead.
        """
        try:
            from execution.utils.db import get_pool, _test_mode as db_test_mode
        except ImportError:
            logger.debug("db module not available; metadata cache stays empty.")
            return

        pool = await get_pool()

        if db_test_mode:
            # In test mode, pull from the mock pool's market_prices list.
            for row in getattr(pool, "_market_prices", []):
                key = (row.get("venue", ""), row.get("market_id", ""))
                self._cache[key] = {
                    "title": row.get("title", ""),
                    "close_time": row.get("close_time"),
                }
            logger.info(
                "Metadata cache refreshed [mock]: %d entries.", len(self._cache)
            )
            return

        # Live query -- get latest title per (venue, market_id).
        query = """
            SELECT DISTINCT ON (venue, market_id)
                   venue, market_id, title
            FROM market_prices
            ORDER BY venue, market_id, time DESC
        """
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query)
            for row in rows:
                key = (row["venue"], row["market_id"])
                self._cache[key] = {
                    "title": row.get("title", ""),
                    "close_time": row.get("close_time"),
                }
            logger.info(
                "Metadata cache refreshed: %d entries.", len(self._cache)
            )
        except Exception as exc:
            logger.error("Metadata cache SQL query failed: %s", exc)
            raise


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Convert a value to float, returning *default* on failure or NaN.

    Handles string, int, float, and None inputs gracefully.
    """
    if value is None or value == "":
        return default
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _parse_timestamp(raw: Any) -> Optional[datetime]:
    """
    Parse a timestamp from various formats into a tz-aware UTC datetime.

    Accepts ISO 8601 strings, Unix epoch (seconds or milliseconds),
    and None (returns None).
    """
    if raw is None or raw == "":
        return None

    # Try numeric epoch first.
    try:
        epoch = float(raw)
        # If it looks like milliseconds (> year 2100 in seconds), convert.
        if epoch > 4_102_444_800:
            epoch = epoch / 1000.0
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    except (TypeError, ValueError):
        pass

    # Try ISO 8601 string.
    if isinstance(raw, str):
        raw_str = raw.strip()
        # Handle 'Z' suffix.
        if raw_str.endswith("Z"):
            raw_str = raw_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

    logger.warning("Could not parse timestamp: %r", raw)
    return None


def normalise_polymarket(fields: dict[str, str]) -> Optional[dict[str, Any]]:
    """
    Normalise a raw Polymarket price tick into the unified schema.

    Polymarket field names (expected in stream):
        - market_id / condition_id / token_id  -> market_id
        - title / question                     -> title
        - yes_price / outcome_prices[0]        -> yes_price (already 0-1)
        - no_price / outcome_prices[1]         -> no_price  (already 0-1)
        - volume_24h / volume                  -> volume_24h
        - timestamp / time / ts                -> time

    Returns None if the tick is malformed (missing prices).
    """
    # Market ID: try several field names.
    market_id = (
        fields.get("market_id")
        or fields.get("condition_id")
        or fields.get("token_id")
        or fields.get("asset_id")
        or ""
    )
    if not market_id:
        logger.warning("Polymarket tick missing market_id: %s", fields)
        return None

    # Title.
    title = fields.get("title") or fields.get("question") or ""

    # Prices -- Polymarket stores in USDC units [0.0, 1.0].
    yes_price = _safe_float(fields.get("yes_price"))
    no_price = _safe_float(fields.get("no_price"))

    # Some connectors publish outcome_prices as a JSON list.
    if yes_price is None or no_price is None:
        raw_outcomes = fields.get("outcome_prices") or fields.get("outcomes")
        if raw_outcomes:
            try:
                outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
                if isinstance(outcomes, list) and len(outcomes) >= 2:
                    if yes_price is None:
                        yes_price = _safe_float(outcomes[0])
                    if no_price is None:
                        no_price = _safe_float(outcomes[1])
            except (json.JSONDecodeError, TypeError):
                pass

    # If we still lack prices, derive no from yes (or vice versa).
    if yes_price is not None and no_price is None:
        no_price = round(1.0 - yes_price, 6)
    elif no_price is not None and yes_price is None:
        yes_price = round(1.0 - no_price, 6)

    if yes_price is None or no_price is None:
        logger.warning(
            "Polymarket tick missing prices: market_id=%s fields=%s",
            market_id, fields,
        )
        return None

    # Volume.
    volume_24h = _safe_float(
        fields.get("volume_24h") or fields.get("volume"), default=0.0
    )

    # Timestamp.
    raw_ts = (
        fields.get("timestamp")
        or fields.get("time")
        or fields.get("ts")
    )
    ts = _parse_timestamp(raw_ts) or datetime.now(timezone.utc)

    return {
        "venue": VENUE_POLYMARKET,
        "market_id": market_id,
        "title": title,
        "yes_price": yes_price,
        "no_price": no_price,
        "volume_24h": volume_24h,
        "time": ts,
    }


def normalise_kalshi(fields: dict[str, str]) -> Optional[dict[str, Any]]:
    """
    Normalise a raw Kalshi price tick into the unified schema.

    Kalshi field names (expected in stream):
        - market_id / ticker / market_ticker / event_ticker -> market_id
        - title / market_title                              -> title
        - yes_price / yes_ask                               -> yes_price (cents -> 0-1)
        - no_price / no_ask                                 -> no_price  (cents -> 0-1)
        - volume_24h / volume                               -> volume_24h
        - timestamp / ts / last_traded_at                   -> time

    Kalshi prices are in cents (0-100 integer).  We divide by 100.

    Returns None if the tick is malformed.
    """
    # Market ID.
    market_id = (
        fields.get("market_id")
        or fields.get("ticker")
        or fields.get("market_ticker")
        or fields.get("event_ticker")
        or ""
    )
    if not market_id:
        logger.warning("Kalshi tick missing market_id: %s", fields)
        return None

    # Title.
    title = fields.get("title") or fields.get("market_title") or ""

    # Prices -- Kalshi in cents.  Detect if already in [0,1] or [0,100].
    yes_raw = _safe_float(fields.get("yes_price") or fields.get("yes_ask"))
    no_raw = _safe_float(fields.get("no_price") or fields.get("no_ask"))

    if yes_raw is not None:
        # Heuristic: if > 1.0, assume cents.
        yes_price = yes_raw / 100.0 if yes_raw > 1.0 else yes_raw
    else:
        yes_price = None

    if no_raw is not None:
        no_price = no_raw / 100.0 if no_raw > 1.0 else no_raw
    else:
        no_price = None

    # Derive missing side if possible.
    if yes_price is not None and no_price is None:
        no_price = round(1.0 - yes_price, 6)
    elif no_price is not None and yes_price is None:
        yes_price = round(1.0 - no_price, 6)

    if yes_price is None or no_price is None:
        logger.warning(
            "Kalshi tick missing prices: market_id=%s fields=%s",
            market_id, fields,
        )
        return None

    # Volume.
    volume_24h = _safe_float(
        fields.get("volume_24h") or fields.get("volume"), default=0.0
    )

    # Timestamp.
    raw_ts = (
        fields.get("timestamp")
        or fields.get("ts")
        or fields.get("last_traded_at")
        or fields.get("time")
    )
    ts = _parse_timestamp(raw_ts) or datetime.now(timezone.utc)

    return {
        "venue": VENUE_KALSHI,
        "market_id": market_id,
        "title": title,
        "yes_price": yes_price,
        "no_price": no_price,
        "volume_24h": volume_24h,
        "time": ts,
    }


def validate_and_finalise(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Validate price ranges, handle clock skew, and compute derived fields.

    Returns a finalised MarketPrice dict ready for publishing, or None
    if validation fails.
    """
    yes = record.get("yes_price")
    no = record.get("no_price")

    # Validate price range [0.0, 1.0].
    if yes is None or no is None:
        return None
    if not (0.0 <= yes <= 1.0):
        logger.warning(
            "Dropping tick: yes_price=%.6f out of range for %s/%s",
            yes, record.get("venue"), record.get("market_id"),
        )
        return None
    if not (0.0 <= no <= 1.0):
        logger.warning(
            "Dropping tick: no_price=%.6f out of range for %s/%s",
            no, record.get("venue"), record.get("market_id"),
        )
        return None

    # Clock skew check.
    now = datetime.now(timezone.utc)
    tick_time = record.get("time")
    if tick_time is not None and isinstance(tick_time, datetime):
        skew = (tick_time - now).total_seconds()
        if skew > CLOCK_SKEW_LIMIT_SEC:
            logger.warning(
                "Tick timestamp %.1fs in the future for %s/%s -- using server time.",
                skew, record.get("venue"), record.get("market_id"),
            )
            tick_time = now
    else:
        tick_time = now

    # Compute spread (overround): yes + no - 1.0
    spread = round(yes + no - 1.0, 6)

    return {
        "time": tick_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "venue": record["venue"],
        "market_id": record["market_id"],
        "title": record.get("title", ""),
        "yes_price": round(yes, 6),
        "no_price": round(no, 6),
        "spread": spread,
        "volume_24h": round(record.get("volume_24h", 0.0), 2),
    }


# ---------------------------------------------------------------------------
# Core consume loop
# ---------------------------------------------------------------------------

async def consume_and_publish(test_mode: bool = False) -> None:
    """
    Main consume loop: read from venue streams, normalise, publish, persist.

    In live mode, runs indefinitely.  In test mode, publishes fixture data
    to the in-memory mock and then exits.
    """
    # Late imports to avoid requiring redis/asyncpg when not needed.
    from execution.utils.redis_client import (
        get_redis, ensure_consumer_groups, publish, consume, ack,
        _use_mock,
    )
    from execution.utils.db import (
        insert_market_price, get_pool,
    )

    logger.info("Initialising Price Bus Publisher (test_mode=%s)...", test_mode)

    # Ensure consumer groups exist on both venue streams.
    await ensure_consumer_groups()

    dedup = DeduplicationTracker()
    metadata = MetadataCache()

    # Stats counters.
    stats = {
        "consumed": 0,
        "published": 0,
        "dropped_invalid": 0,
        "dropped_dedup": 0,
        "db_persisted": 0,
        "db_errors": 0,
    }

    iteration = 0
    max_iterations = None  # infinite in live mode

    if test_mode:
        # In test mode, seed venue streams with fixture data, then run
        # a limited number of iterations to drain them.
        await _seed_fixture_data()
        max_iterations = 5  # enough to drain fixtures

    logger.info("Entering consume loop (consumer=%s)...", CONSUMER_NAME)

    while True:
        iteration += 1

        # Exit condition for test mode.
        if max_iterations is not None and iteration > max_iterations:
            logger.info("Test mode: max iterations reached, exiting loop.")
            break

        # Consume from both venue streams.
        for stream_name, venue in VENUE_STREAM_MAP.items():
            try:
                messages = await consume(
                    stream=stream_name,
                    group=GROUP_PRICE_BUS,
                    consumer=CONSUMER_NAME,
                    count=CONSUME_BATCH_SIZE,
                    block=CONSUME_BLOCK_MS if max_iterations is None else 100,
                )
            except Exception as exc:
                logger.error(
                    "Error consuming from %s: %s. Retrying next cycle.",
                    stream_name, exc,
                )
                continue

            for msg_id, fields in messages:
                stats["consumed"] += 1

                # ---- Normalise ----
                if venue == VENUE_POLYMARKET:
                    record = normalise_polymarket(fields)
                elif venue == VENUE_KALSHI:
                    record = normalise_kalshi(fields)
                else:
                    logger.warning("Unknown venue for stream %s", stream_name)
                    record = None

                if record is None:
                    stats["dropped_invalid"] += 1
                    await ack(stream_name, GROUP_PRICE_BUS, msg_id)
                    continue

                # ---- Enrich with metadata ----
                meta = await metadata.get(venue, record["market_id"])
                if meta.get("title") and not record.get("title"):
                    record["title"] = meta["title"]

                # ---- Validate & finalise ----
                normalised = validate_and_finalise(record)
                if normalised is None:
                    stats["dropped_invalid"] += 1
                    await ack(stream_name, GROUP_PRICE_BUS, msg_id)
                    continue

                # ---- Deduplication ----
                if dedup.is_duplicate(normalised["venue"], normalised["market_id"]):
                    stats["dropped_dedup"] += 1
                    await ack(stream_name, GROUP_PRICE_BUS, msg_id)
                    logger.debug(
                        "Dedup: skipping %s/%s",
                        normalised["venue"], normalised["market_id"],
                    )
                    continue

                # ---- Publish to normalised stream ----
                try:
                    await publish(
                        STREAM_PRICES_NORMALISED,
                        normalised,
                        maxlen=STREAM_MAXLEN,
                    )
                    stats["published"] += 1
                    logger.debug(
                        "Published normalised tick: venue=%s market_id=%s "
                        "yes=%.4f no=%.4f spread=%.4f",
                        normalised["venue"],
                        normalised["market_id"],
                        normalised["yes_price"],
                        normalised["no_price"],
                        normalised["spread"],
                    )
                except Exception as exc:
                    logger.error(
                        "Failed to publish normalised tick: %s", exc,
                    )

                # ---- Persist to PostgreSQL ----
                try:
                    tick_dt = _parse_timestamp(normalised["time"]) or datetime.now(timezone.utc)
                    await insert_market_price(
                        time=tick_dt,
                        venue=normalised["venue"],
                        market_id=normalised["market_id"],
                        title=normalised.get("title", ""),
                        yes_price=normalised["yes_price"],
                        no_price=normalised["no_price"],
                        spread=normalised["spread"],
                        volume_24h=normalised["volume_24h"],
                    )
                    stats["db_persisted"] += 1
                except Exception as exc:
                    stats["db_errors"] += 1
                    logger.error(
                        "Failed to persist market price to DB: %s", exc,
                    )

                # ---- Acknowledge ----
                await ack(stream_name, GROUP_PRICE_BUS, msg_id)

        # Periodic stats logging (every 60 iterations ~ 60s with 1s block).
        if iteration % 60 == 0:
            logger.info(
                "Stats: consumed=%d published=%d dropped_invalid=%d "
                "dropped_dedup=%d db_persisted=%d db_errors=%d",
                stats["consumed"],
                stats["published"],
                stats["dropped_invalid"],
                stats["dropped_dedup"],
                stats["db_persisted"],
                stats["db_errors"],
            )

    # Final stats on exit (test mode).
    logger.info(
        "Final stats: consumed=%d published=%d dropped_invalid=%d "
        "dropped_dedup=%d db_persisted=%d db_errors=%d",
        stats["consumed"],
        stats["published"],
        stats["dropped_invalid"],
        stats["dropped_dedup"],
        stats["db_persisted"],
        stats["db_errors"],
    )

    return stats  # useful for test assertions


# ---------------------------------------------------------------------------
# Fixture data for --test mode
# ---------------------------------------------------------------------------

FIXTURE_POLYMARKET_TICKS = [
    {
        "timestamp": "2026-02-28T14:30:00.123Z",
        "market_id": "0xabc123def456",
        "title": "Will BTC exceed $100k by March 2026?",
        "yes_price": "0.6500",
        "no_price": "0.3500",
        "volume_24h": "125000.00",
    },
    {
        "timestamp": "2026-02-28T14:30:01.456Z",
        "market_id": "0x789ghi012jkl",
        "title": "Will ETH reach $5k by Q2 2026?",
        "yes_price": "0.4200",
        "no_price": "0.5800",
        "volume_24h": "89000.00",
    },
    {
        "timestamp": "2026-02-28T14:30:02.789Z",
        "market_id": "0xmno345pqr678",
        "title": "US unemployment rate above 4.5% in March 2026?",
        "outcome_prices": "[0.3100, 0.6900]",
        "volume": "45000.00",
    },
    {
        # Duplicate of the first tick -- should be deduplicated.
        "timestamp": "2026-02-28T14:30:00.500Z",
        "market_id": "0xabc123def456",
        "title": "Will BTC exceed $100k by March 2026?",
        "yes_price": "0.6510",
        "no_price": "0.3490",
        "volume_24h": "125100.00",
    },
    {
        # Invalid: yes_price > 1.0 -- should be dropped.
        "timestamp": "2026-02-28T14:30:03.000Z",
        "market_id": "0xinvalid001",
        "title": "Invalid price tick",
        "yes_price": "1.5000",
        "no_price": "0.3000",
        "volume_24h": "1000.00",
    },
]

FIXTURE_KALSHI_TICKS = [
    {
        "ts": "2026-02-28T14:30:00.200Z",
        "ticker": "KXBTC-100K-MAR26",
        "title": "Bitcoin above $100,000 on March 31?",
        "yes_price": "63",
        "no_price": "37",
        "volume_24h": "98000",
    },
    {
        "ts": "2026-02-28T14:30:01.800Z",
        "market_ticker": "KXETH-5K-Q2-26",
        "market_title": "ETH above $5,000 on June 30?",
        "yes_ask": "40",
        "no_ask": "60",
        "volume": "54000",
    },
    {
        # Kalshi tick with prices already in [0,1] range (connector pre-normalised).
        "ts": "2026-02-28T14:30:02.100Z",
        "ticker": "KXUNRATE-45-MAR26",
        "title": "US unemployment above 4.5% in March?",
        "yes_price": "0.29",
        "no_price": "0.71",
        "volume_24h": "32000",
    },
]


async def _seed_fixture_data() -> None:
    """Publish fixture ticks to venue streams for --test mode consumption."""
    from execution.utils.redis_client import publish

    logger.info("Seeding fixture data into venue streams...")

    for tick in FIXTURE_POLYMARKET_TICKS:
        await publish(STREAM_PRICES_POLYMARKET, tick)
    logger.info(
        "Seeded %d Polymarket fixture ticks.", len(FIXTURE_POLYMARKET_TICKS)
    )

    for tick in FIXTURE_KALSHI_TICKS:
        await publish(STREAM_PRICES_KALSHI, tick)
    logger.info(
        "Seeded %d Kalshi fixture ticks.", len(FIXTURE_KALSHI_TICKS)
    )


# ---------------------------------------------------------------------------
# Test mode runner
# ---------------------------------------------------------------------------

async def run_test() -> None:
    """
    --test mode: use InMemoryRedis and MockPool, seed fixture data,
    consume/normalise/publish, and print results to stdout.
    """
    # Enable mocks before any imports that touch redis/db.
    import execution.utils.redis_client as rc
    import execution.utils.db as db_mod

    rc._use_mock = True
    db_mod._test_mode = True

    print()
    print("=" * 72)
    print("  Price Bus Publisher -- Test Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    stats = await consume_and_publish(test_mode=True)

    # Read back from normalised stream to display results.
    from execution.utils.redis_client import get_redis
    redis = await get_redis()

    # Ensure consumer group exists on normalised stream for read-back.
    try:
        await redis.xgroup_create(
            name=STREAM_PRICES_NORMALISED,
            groupname="test_reader_group",
            id="0",
            mkstream=True,
        )
    except Exception:
        pass  # already exists

    results = await redis.xreadgroup(
        groupname="test_reader_group",
        consumername="test_reader",
        streams={STREAM_PRICES_NORMALISED: ">"},
        count=100,
        block=100,
    )

    print()
    print("-" * 72)
    print("  Normalised prices published to stream:prices:normalised:")
    print("-" * 72)

    if results:
        for _stream, entries in results:
            for msg_id, fields in entries:
                venue = fields.get("venue", "?")
                mid = fields.get("market_id", "?")
                yes_p = fields.get("yes_price", "?")
                no_p = fields.get("no_price", "?")
                spread = fields.get("spread", "?")
                title = fields.get("title", "")[:50]
                print(
                    f"  {msg_id}  {venue:<12} {mid:<24} "
                    f"yes={yes_p:<8} no={no_p:<8} spread={spread:<8} "
                    f"{title}"
                )
    else:
        print("  (no messages found)")

    # Display DB persisted rows.
    db_pool = await db_mod.get_pool()
    db_rows = getattr(db_pool, "_market_prices", [])
    print()
    print("-" * 72)
    print("  Rows persisted to market_prices (mock DB):")
    print("-" * 72)
    for row in db_rows:
        print(
            f"  {row['venue']:<12} {row['market_id']:<24} "
            f"yes={row['yes_price']:<8.4f} no={row['no_price']:<8.4f} "
            f"spread={row['spread']:<8.4f}"
        )

    print()
    print("-" * 72)
    print(f"  Consumed: {stats['consumed']}  "
          f"Published: {stats['published']}  "
          f"Invalid: {stats['dropped_invalid']}  "
          f"Dedup: {stats['dropped_dedup']}  "
          f"DB: {stats['db_persisted']}")
    print("=" * 72)
    print()

    # Cleanup.
    from execution.utils.redis_client import close_redis
    from execution.utils.db import close_pool

    await close_redis()
    await close_pool()


# ---------------------------------------------------------------------------
# Production mode runner
# ---------------------------------------------------------------------------

async def run_live() -> None:
    """
    Production path: connect to live Redis and PostgreSQL, run the
    infinite consume loop.

    Handles graceful shutdown on KeyboardInterrupt / SIGTERM.
    """
    logger.info("Starting Price Bus Publisher in LIVE mode.")

    try:
        await consume_and_publish(test_mode=False)
    except KeyboardInterrupt:
        logger.info("Received interrupt -- shutting down gracefully.")
    except Exception as exc:
        logger.error("Unhandled error in consume loop: %s", exc, exc_info=True)
    finally:
        from execution.utils.redis_client import close_redis
        from execution.utils.db import close_pool

        await close_redis()
        await close_pool()
        logger.info("Price Bus Publisher shut down.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse --test flag and launch the appropriate mode.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Unified Price Bus Publisher -- consumes raw venue price ticks, "
            "normalises to MarketPrice schema, publishes to "
            "stream:prices:normalised, and persists to PostgreSQL."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Use in-memory mocks (no Redis/PostgreSQL required). "
            "Seeds fixture data, runs a limited consume loop, prints results."
        ),
    )
    args = parser.parse_args()

    if args.test:
        asyncio.run(run_test())
    else:
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
