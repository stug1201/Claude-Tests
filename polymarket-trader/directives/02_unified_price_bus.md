# Directive: Unified Price Bus

## Goal

Implement a Redis Streams-based message bus that normalises price data from all venue connectors into a unified `MarketPrice` format, enabling all downstream strategies to consume a single, consistent data stream regardless of source venue.

## Input

- `stream:prices:polymarket` — Raw price ticks from Polymarket CLOB WebSocket
- `stream:prices:kalshi` — Raw price ticks from Kalshi WebSocket
- Market metadata from PostgreSQL (titles, close times, categories)

## Tools

- `execution/pipeline/price_bus_publisher.py` — Normalises and publishes unified prices
- `execution/utils/redis_client.py` — Redis Streams helper functions

## Output

- `stream:prices:normalised` — Unified MarketPrice objects consumed by all strategies

## MarketPrice Schema

Every message on `stream:prices:normalised` must conform to this JSON structure:

```json
{
    "timestamp": "2026-02-28T14:30:00.123Z",
    "venue": "polymarket",
    "market_id": "0x1234abcd...",
    "title": "Will BTC exceed $100k by March 2026?",
    "yes_price": 0.6500,
    "no_price": 0.3500,
    "spread": 0.0100,
    "volume_24h": 125000.00,
    "close_time": "2026-03-31T23:59:59Z",
    "matched_market_id": "kalshi-btc-100k-mar26",
    "match_confidence": 0.92
}
```

### Field Definitions

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `timestamp` | ISO 8601 UTC | Connector | Time of price observation |
| `venue` | string | Connector | `"polymarket"` or `"kalshi"` |
| `market_id` | string | Connector | Venue-native market identifier |
| `title` | string | Gamma/Kalshi metadata | Human-readable market title |
| `yes_price` | float | Connector | Price of Yes contract (0.0-1.0) |
| `no_price` | float | Connector | Price of No contract (0.0-1.0) |
| `spread` | float | Computed | `yes_price + no_price - 1.0` (overround) |
| `volume_24h` | float | Connector | 24-hour trading volume in USD |
| `close_time` | ISO 8601 UTC | Metadata | Market resolution deadline |
| `matched_market_id` | string/null | Market Matcher | Cross-venue match if found |
| `match_confidence` | float/null | Market Matcher | Cosine similarity score (0.0-1.0) |

## Processing Logic

1. **Consumer group**: Create a Redis consumer group `price_bus` on both venue streams
2. **Read loop**: `XREADGROUP` with block timeout of 1000ms from both streams
3. **Normalise**: Map venue-specific field names to unified schema
4. **Enrich**: Lookup market metadata (title, close_time) from PostgreSQL cache (refresh every 5 min)
5. **Validate**: Reject records where `yes_price` or `no_price` is outside [0.0, 1.0]
6. **Publish**: `XADD stream:prices:normalised * <fields>`
7. **Persist**: Insert into `market_prices` hypertable for historical queries

## Redis Streams Configuration

- **Max stream length**: 100,000 entries per stream (use `MAXLEN ~100000` on XADD)
- **Consumer group**: `price_bus_group` with auto-acknowledge after processing
- **Pending timeout**: Claim unacknowledged messages after 60s

## Error Handling

- **Missing metadata**: If market title/close_time not found in PostgreSQL cache, publish with null fields and log a warning. Do not block the pipeline.
- **Malformed prices**: If `yes_price` or `no_price` is null, NaN, or outside [0, 1], drop the record and log at WARNING level.
- **Redis connection loss**: Reconnect with exponential backoff (1s, 2s, 4s, max 30s). Buffer up to 1000 messages in memory during reconnection.
- **PostgreSQL cache refresh failure**: Continue with stale cache. Log at ERROR level. Retry on next refresh cycle.

## Performance Requirements

- **Latency**: < 10ms from raw tick to normalised publication under normal load
- **Throughput**: Handle 500 ticks/second sustained (both venues combined)
- **Memory**: In-memory metadata cache should not exceed 100MB

## Testing

- `--test` mode: Read from fixture JSON files instead of Redis Streams, publish to stdout
- Unit tests: Test normalisation logic with known input/output pairs for each venue
- Integration test: Publish fixture ticks to venue streams, verify normalised output schema

## Edge Cases

- **Duplicate ticks**: Deduplicate by `(venue, market_id, timestamp)` tuple within a 1-second window
- **Clock skew**: If a tick timestamp is more than 5 seconds in the future, log a warning and use server time
- **Market not in metadata**: New markets may appear before the metadata cache refreshes. Accept with null metadata, flag for next refresh.
- **Venue-specific quirks**: Polymarket prices are in USDC units (6 decimals). Kalshi prices are in cents (integer). Normalise both to decimal [0.0, 1.0].
