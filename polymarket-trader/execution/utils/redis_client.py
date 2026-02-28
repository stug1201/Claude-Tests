#!/usr/bin/env python3
"""
redis_client.py -- Redis Streams helper functions for polymarket-trader.

Provides an async Redis connection (singleton with lazy init), consumer group
management, stream publishing (XADD), consuming (XREADGROUP), and
acknowledgement (XACK).  All public functions are async.

Streams:
    stream:prices:polymarket    -- Raw Polymarket price ticks
    stream:prices:kalshi        -- Raw Kalshi price ticks
    stream:prices:normalised    -- Unified MarketPrice objects
    stream:arb:signals          -- Arb opportunity signals
    stream:insider:alerts       -- High-IPS wallet trade events
    stream:news:signals         -- Parsed Glint signals
    stream:orders:pending       -- Orders queued for execution
    stream:orders:filled        -- Execution confirmations

Data serialisation:
    Each dict key becomes a Redis Stream field.  Scalar values (str, int,
    float, bool, None) are stored as-is; complex values (lists, nested dicts)
    are JSON-encoded so that the field value is always a string.

Reconnection:
    On connection loss the module retries with exponential backoff
    (1 s -> 2 s -> 4 s -> 8 s max) before raising.

Usage:
    from execution.utils.redis_client import get_redis, publish, consume

    redis = await get_redis()
    msg_id = await publish(STREAM_PRICES_POLYMARKET, {"yes_price": "0.65"})
    messages = await consume(STREAM_PRICES_NORMALISED, GROUP_PRICE_BUS, "worker-1")

    python execution/utils/redis_client.py          # Live round-trip demo
    python execution/utils/redis_client.py --test   # In-memory mock demo

Environment variables (via execution.utils.config):
    REDIS_URL -- Redis connection string (e.g. redis://localhost:6379/0)

See: directives/02_unified_price_bus.md
     directives/00_system_overview.md
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/utils/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"

# ---------------------------------------------------------------------------
# Stream name constants
# ---------------------------------------------------------------------------
STREAM_PRICES_POLYMARKET = "stream:prices:polymarket"
STREAM_PRICES_KALSHI = "stream:prices:kalshi"
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
STREAM_ARB_SIGNALS = "stream:arb:signals"
STREAM_INSIDER_ALERTS = "stream:insider:alerts"
STREAM_NEWS_SIGNALS = "stream:news:signals"
STREAM_ORDERS_PENDING = "stream:orders:pending"
STREAM_ORDERS_FILLED = "stream:orders:filled"

ALL_STREAMS = [
    STREAM_PRICES_POLYMARKET,
    STREAM_PRICES_KALSHI,
    STREAM_PRICES_NORMALISED,
    STREAM_ARB_SIGNALS,
    STREAM_INSIDER_ALERTS,
    STREAM_NEWS_SIGNALS,
    STREAM_ORDERS_PENDING,
    STREAM_ORDERS_FILLED,
]

# ---------------------------------------------------------------------------
# Consumer group constants
# ---------------------------------------------------------------------------
GROUP_PRICE_BUS = "price_bus_group"
GROUP_STRATEGY = "strategy_group"
GROUP_EXECUTION = "execution_group"
GROUP_AUDIT = "audit_group"
GROUP_ARB = "arb_group"
GROUP_INSIDER = "insider_group"
GROUP_NEWS = "news_group"

# Default group-to-stream mappings used by ensure_consumer_groups().
# Each group is created on the streams it is expected to consume from.
_GROUP_STREAM_MAP: dict[str, list[str]] = {
    GROUP_PRICE_BUS: [STREAM_PRICES_POLYMARKET, STREAM_PRICES_KALSHI],
    GROUP_STRATEGY: [STREAM_PRICES_NORMALISED],
    GROUP_EXECUTION: [STREAM_ORDERS_PENDING],
    GROUP_AUDIT: [STREAM_ORDERS_FILLED],
    GROUP_ARB: [STREAM_ARB_SIGNALS],
    GROUP_INSIDER: [STREAM_INSIDER_ALERTS],
    GROUP_NEWS: [STREAM_NEWS_SIGNALS],
}

# ---------------------------------------------------------------------------
# Reconnection configuration
# ---------------------------------------------------------------------------
_BACKOFF_BASE = 1       # seconds
_BACKOFF_MAX = 8        # seconds
_MAX_RETRIES = 4        # 1 + 2 + 4 + 8 = 15 seconds total worst case

# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_redis_instance: Optional[Any] = None
_use_mock: bool = False


# ---------------------------------------------------------------------------
# In-memory mock Redis (for --test mode)
# ---------------------------------------------------------------------------

class InMemoryRedis:
    """
    Minimal in-memory mock that implements the subset of redis.asyncio.Redis
    used by this module.  No actual Redis connection is made.

    Streams are stored as ``dict[str, list[tuple[str, dict]]]`` where each
    entry is ``(message_id, field_dict)``.  Consumer groups and pending
    entries are tracked with simple dicts/sets.
    """

    def __init__(self) -> None:
        self._streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self._groups: dict[str, set[str]] = {}       # stream -> set of group names
        self._pending: dict[str, dict[str, set[str]]] = {}  # stream -> group -> set of msg ids
        self._acked: dict[str, dict[str, set[str]]] = {}    # stream -> group -> set of acked ids
        self._delivered: dict[str, dict[str, int]] = {}       # stream -> group -> next delivery index
        self._counter: int = 0                        # auto-increment for message IDs
        logger.info("InMemoryRedis mock initialised.")

    # -- XGROUP CREATE --------------------------------------------------------

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = False,
    ) -> bool:
        """Create a consumer group on stream *name*."""
        if mkstream and name not in self._streams:
            self._streams[name] = []
        if name not in self._streams:
            raise Exception(f"Stream '{name}' does not exist")
        groups = self._groups.setdefault(name, set())
        if groupname in groups:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        groups.add(groupname)
        self._pending.setdefault(name, {}).setdefault(groupname, set())
        self._acked.setdefault(name, {}).setdefault(groupname, set())
        self._delivered.setdefault(name, {}).setdefault(groupname, 0)
        logger.debug(
            "Mock XGROUP CREATE: stream=%s group=%s", name, groupname
        )
        return True

    # -- XADD -----------------------------------------------------------------

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        maxlen: Optional[int] = None,
        approximate: bool = True,
        id: str = "*",
    ) -> str:
        """Add an entry to stream *name*. Returns a synthetic message ID."""
        self._counter += 1
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        msg_id = f"{now_ms}-{self._counter}"

        stream = self._streams.setdefault(name, [])
        stream.append((msg_id, dict(fields)))

        # Trim if maxlen specified.
        if maxlen is not None and len(stream) > maxlen:
            excess = len(stream) - maxlen
            self._streams[name] = stream[excess:]

        logger.debug(
            "Mock XADD: stream=%s id=%s fields=%d",
            name, msg_id, len(fields),
        )
        return msg_id

    # -- XREADGROUP -----------------------------------------------------------

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> list:
        """
        Read new entries from streams for a consumer group.

        Returns a list of ``[stream_name, [(msg_id, fields), ...]]`` for each
        stream that has undelivered messages.
        """
        results = []
        for stream_name, last_id in streams.items():
            entries = self._streams.get(stream_name, [])
            delivered_idx = self._delivered.get(stream_name, {}).get(groupname, 0)
            acked = self._acked.get(stream_name, {}).get(groupname, set())

            # Deliver un-acked messages starting from the delivered index.
            new_entries: list[tuple[str, dict[str, str]]] = []
            start = delivered_idx
            for i in range(start, len(entries)):
                mid, fields = entries[i]
                if mid not in acked:
                    new_entries.append((mid, fields))
                    # Track as pending.
                    self._pending.setdefault(stream_name, {}).setdefault(
                        groupname, set()
                    ).add(mid)
                if count is not None and len(new_entries) >= count:
                    break

            if new_entries:
                # Advance the delivered pointer.
                last_delivered = start + len(new_entries)
                self._delivered.setdefault(stream_name, {})[groupname] = last_delivered
                results.append([stream_name, new_entries])

        return results

    # -- XACK -----------------------------------------------------------------

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        """Acknowledge messages by ID."""
        acked_set = self._acked.setdefault(name, {}).setdefault(groupname, set())
        pending_set = self._pending.get(name, {}).get(groupname, set())
        count = 0
        for mid in ids:
            if mid not in acked_set:
                acked_set.add(mid)
                pending_set.discard(mid)
                count += 1
        logger.debug("Mock XACK: stream=%s group=%s ids=%s", name, groupname, ids)
        return count

    # -- XLEN -----------------------------------------------------------------

    async def xlen(self, name: str) -> int:
        """Return the number of entries in stream *name*."""
        return len(self._streams.get(name, []))

    # -- XTRIM ----------------------------------------------------------------

    async def xtrim(
        self,
        name: str,
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> int:
        """Trim stream *name* to *maxlen* entries."""
        stream = self._streams.get(name, [])
        if maxlen is not None and len(stream) > maxlen:
            excess = len(stream) - maxlen
            self._streams[name] = stream[excess:]
            return excess
        return 0

    # -- Connection lifecycle (no-ops for mock) --------------------------------

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        logger.info("InMemoryRedis mock closed.")

    async def aclose(self) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _serialise_fields(data: dict) -> dict[str, str]:
    """
    Convert a Python dict into Redis Stream field values.

    Scalar types (str, int, float, bool, None) are converted to their string
    representation.  Complex types (list, dict, tuple) are JSON-encoded.
    """
    fields: dict[str, str] = {}
    for key, value in data.items():
        if isinstance(value, str):
            fields[key] = value
        elif isinstance(value, (int, float, bool)):
            fields[key] = str(value)
        elif value is None:
            fields[key] = ""
        else:
            # Lists, nested dicts, etc.
            fields[key] = json.dumps(value, default=str)
    return fields


def _deserialise_fields(fields: dict[bytes | str, bytes | str]) -> dict[str, str]:
    """
    Normalise Redis response fields to ``dict[str, str]``.

    redis-py returns bytes when decode_responses is False.  This function
    handles both bytes and str transparently.
    """
    result: dict[str, str] = {}
    for k, v in fields.items():
        key = k.decode("utf-8") if isinstance(k, bytes) else k
        val = v.decode("utf-8") if isinstance(v, bytes) else v
        result[key] = val
    return result


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

async def get_redis():
    """
    Return the singleton async Redis connection.

    On first call, establishes the connection using REDIS_URL from config.
    If --test mode is active, returns an InMemoryRedis mock instead.

    Implements exponential backoff (1 s, 2 s, 4 s, 8 s) on connection failure.

    Returns:
        redis.asyncio.Redis or InMemoryRedis instance.
    """
    global _redis_instance, _use_mock

    if _redis_instance is not None:
        return _redis_instance

    if _use_mock:
        _redis_instance = InMemoryRedis()
        logger.info("Using InMemoryRedis mock (--test mode).")
        return _redis_instance

    # Late import so --test mode never requires redis package.
    try:
        import redis.asyncio as aioredis
    except ImportError:
        logger.error(
            "redis[asyncio] package is not installed. "
            "Install with: pip install redis"
        )
        sys.exit(1)

    from execution.utils.config import config
    redis_url = config.REDIS_URL
    logger.info("Connecting to Redis: %s", redis_url[:20] + "****")

    backoff = _BACKOFF_BASE
    last_error: Optional[Exception] = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            client = aioredis.from_url(
                redis_url,
                decode_responses=True,
                max_connections=20,
            )
            # Verify the connection is alive.
            await client.ping()
            _redis_instance = client
            logger.info(
                "Redis connection established on attempt %d.", attempt
            )
            return _redis_instance
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Redis connection attempt %d/%d failed: %s. "
                "Retrying in %d s...",
                attempt, _MAX_RETRIES, exc, backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX)

    # All retries exhausted.
    logger.error(
        "Failed to connect to Redis after %d attempts. Last error: %s",
        _MAX_RETRIES, last_error,
    )
    raise ConnectionError(
        f"Could not connect to Redis after {_MAX_RETRIES} attempts: {last_error}"
    )


async def close_redis() -> None:
    """
    Close the singleton Redis connection and reset the module state.

    Safe to call even if no connection has been established.
    """
    global _redis_instance

    if _redis_instance is None:
        logger.debug("close_redis() called but no connection is active.")
        return

    try:
        await _redis_instance.aclose()
        logger.info("Redis connection closed.")
    except Exception as exc:
        logger.warning("Error closing Redis connection: %s", exc)
    finally:
        _redis_instance = None


# ---------------------------------------------------------------------------
# Consumer group management
# ---------------------------------------------------------------------------

async def ensure_consumer_groups() -> None:
    """
    Create all consumer groups defined in _GROUP_STREAM_MAP.

    Uses MKSTREAM to auto-create streams that do not yet exist.
    Silently ignores BUSYGROUP errors (group already exists).

    This function is idempotent and safe to call on every startup.
    """
    r = await get_redis()
    created = 0
    skipped = 0

    for group, streams in _GROUP_STREAM_MAP.items():
        for stream in streams:
            try:
                await r.xgroup_create(
                    name=stream,
                    groupname=group,
                    id="0",
                    mkstream=True,
                )
                logger.info(
                    "Created consumer group '%s' on stream '%s'.",
                    group, stream,
                )
                created += 1
            except Exception as exc:
                if "BUSYGROUP" in str(exc):
                    logger.debug(
                        "Consumer group '%s' already exists on '%s'.",
                        group, stream,
                    )
                    skipped += 1
                else:
                    logger.error(
                        "Failed to create group '%s' on '%s': %s",
                        group, stream, exc,
                    )
                    raise

    logger.info(
        "Consumer groups: %d created, %d already existed.", created, skipped
    )


# ---------------------------------------------------------------------------
# Stream operations
# ---------------------------------------------------------------------------

async def publish(
    stream: str,
    data: dict,
    maxlen: int = 100_000,
) -> str:
    """
    Publish a message to a Redis Stream via XADD.

    Each key in *data* becomes a stream field.  Complex values (lists, dicts)
    are JSON-encoded; scalars are stringified.

    Args:
        stream:  Target stream name.
        data:    Dict of field-value pairs to publish.
        maxlen:  Approximate maximum stream length (default 100 000).

    Returns:
        The message ID assigned by Redis (e.g. "1709123456789-0").
    """
    r = await get_redis()
    fields = _serialise_fields(data)
    msg_id = await r.xadd(
        name=stream,
        fields=fields,
        maxlen=maxlen,
        approximate=True,
    )
    logger.debug("Published to '%s': id=%s", stream, msg_id)
    return msg_id


async def consume(
    stream: str,
    group: str,
    consumer: str,
    count: int = 10,
    block: int = 1000,
) -> list[tuple[str, dict]]:
    """
    Consume messages from a Redis Stream using XREADGROUP.

    Reads up to *count* new (undelivered) messages, blocking for up to
    *block* milliseconds if no messages are available.

    Args:
        stream:    Source stream name.
        group:     Consumer group name.
        consumer:  Consumer name within the group.
        count:     Maximum number of messages to read.
        block:     Block timeout in milliseconds (0 = no block).

    Returns:
        List of ``(message_id, data_dict)`` tuples.  Returns an empty list
        if no messages are available within the block timeout.
    """
    r = await get_redis()

    try:
        response = await r.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block,
        )
    except Exception as exc:
        logger.error(
            "XREADGROUP failed on '%s' group '%s': %s", stream, group, exc
        )
        raise

    if not response:
        return []

    messages: list[tuple[str, dict]] = []
    for _stream_name, entries in response:
        for msg_id, fields in entries:
            data = _deserialise_fields(fields) if isinstance(
                next(iter(fields.keys()), ""), bytes
            ) else dict(fields)
            messages.append((msg_id, data))

    logger.debug(
        "Consumed %d message(s) from '%s' group '%s'.",
        len(messages), stream, group,
    )
    return messages


async def ack(stream: str, group: str, message_id: str) -> None:
    """
    Acknowledge a message so it is removed from the pending entries list.

    Args:
        stream:      Stream name.
        group:       Consumer group name.
        message_id:  The ID of the message to acknowledge.
    """
    r = await get_redis()
    await r.xack(stream, group, message_id)
    logger.debug("Acked message '%s' on '%s' group '%s'.", message_id, stream, group)


async def stream_len(stream: str) -> int:
    """
    Return the number of entries in a Redis Stream.

    Args:
        stream:  Stream name.

    Returns:
        Integer count of entries.
    """
    r = await get_redis()
    length = await r.xlen(stream)
    return length


async def trim_stream(stream: str, maxlen: int) -> None:
    """
    Trim a Redis Stream to approximately *maxlen* entries.

    Uses approximate trimming (``~``) for efficiency.

    Args:
        stream:  Stream name.
        maxlen:  Target maximum length.
    """
    r = await get_redis()
    trimmed = await r.xtrim(name=stream, maxlen=maxlen, approximate=True)
    logger.info(
        "Trimmed stream '%s' to ~%d entries (%d removed).",
        stream, maxlen, trimmed,
    )


# ---------------------------------------------------------------------------
# Demo / self-test
# ---------------------------------------------------------------------------

async def _demo_round_trip() -> None:
    """
    Demonstrate a full publish -> consume -> ack round-trip.

    Used by the CLI entry point to verify that the module works correctly
    against either a live Redis instance or the InMemoryRedis mock.
    """
    print()
    print("=" * 72)
    print("  Redis Streams Client -- Round-Trip Demo")
    print("  Mode: %s" % ("MOCK (in-memory)" if _use_mock else "LIVE"))
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # Step 1: Ensure consumer groups exist.
    logger.info("Step 1: Creating consumer groups...")
    await ensure_consumer_groups()

    # Step 2: Publish sample messages.
    logger.info("Step 2: Publishing sample messages...")
    test_stream = STREAM_PRICES_POLYMARKET
    test_group = GROUP_PRICE_BUS
    test_consumer = "demo-worker-1"

    sample_messages = [
        {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "venue": "polymarket",
            "market_id": "0xabc123def456",
            "title": "Will BTC exceed $100k by March 2026?",
            "yes_price": 0.6500,
            "no_price": 0.3500,
            "spread": 0.0100,
            "volume_24h": 125000.00,
        },
        {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "venue": "polymarket",
            "market_id": "0x789ghi012jkl",
            "title": "Will ETH reach $5k by Q2 2026?",
            "yes_price": 0.4200,
            "no_price": 0.5800,
            "spread": 0.0050,
            "volume_24h": 89000.00,
        },
        {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "venue": "polymarket",
            "market_id": "0xmno345pqr678",
            "title": "US unemployment rate above 4.5% in March 2026?",
            "yes_price": 0.3100,
            "no_price": 0.6900,
            "spread": 0.0020,
            "volume_24h": 45000.00,
        },
    ]

    published_ids = []
    for msg in sample_messages:
        msg_id = await publish(test_stream, msg)
        published_ids.append(msg_id)
        print(f"  Published: {msg_id} -> {msg['title'][:50]}")

    # Step 3: Check stream length.
    length = await stream_len(test_stream)
    print(f"\n  Stream '{test_stream}' length: {length}")

    # Step 4: Consume messages.
    logger.info("Step 3: Consuming messages...")
    consumed = await consume(
        stream=test_stream,
        group=test_group,
        consumer=test_consumer,
        count=10,
        block=500,
    )

    print(f"\n  Consumed {len(consumed)} message(s):")
    for msg_id, data in consumed:
        title = data.get("title", "<no title>")
        yes_p = data.get("yes_price", "?")
        print(f"    {msg_id}: {title[:45]} | yes={yes_p}")

    # Step 5: Acknowledge all consumed messages.
    logger.info("Step 4: Acknowledging messages...")
    for msg_id, _ in consumed:
        await ack(test_stream, test_group, msg_id)
    print(f"\n  Acknowledged {len(consumed)} message(s).")

    # Step 6: Verify no pending messages by consuming again.
    remaining = await consume(
        stream=test_stream,
        group=test_group,
        consumer=test_consumer,
        count=10,
        block=100,
    )
    print(f"  Remaining after ack: {len(remaining)} message(s).")

    # Step 7: Test trim.
    logger.info("Step 5: Testing stream trim...")
    await trim_stream(test_stream, maxlen=2)
    new_length = await stream_len(test_stream)
    print(f"  Stream length after trim(maxlen=2): {new_length}")

    # Cleanup.
    await close_redis()

    print()
    print("=" * 72)
    print("  Round-trip demo complete.  All operations succeeded.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse --test flag and run the round-trip demo.

    In --test mode, uses InMemoryRedis so no running Redis instance is needed.
    In live mode, connects to the Redis instance specified by REDIS_URL.
    """
    global _use_mock

    parser = argparse.ArgumentParser(
        description=(
            "Redis Streams client for polymarket-trader. "
            "Runs a publish/consume round-trip demo."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use in-memory mock Redis (no live Redis connection needed).",
    )
    args = parser.parse_args()

    if args.test:
        _use_mock = True
        logger.info("Running in --test mode with InMemoryRedis mock.")
    else:
        logger.info("Running in live mode against Redis.")

    asyncio.run(_demo_round_trip())


if __name__ == "__main__":
    main()
