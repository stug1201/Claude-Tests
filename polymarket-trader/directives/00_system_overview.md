# Directive: System Overview

## Goal

Build an automated prediction-market trading system that executes five independent strategies across Polymarket and Kalshi, fed by a unified real-time price bus, with robust risk management and full auditability.

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  Polymarket CLOB ─┐                                             │
│  Polymarket Gamma ─┤  ┌───────────────┐   ┌─────────────────┐   │
│  Polymarket Data  ─┼──│  Connectors   │──▶│  Redis Streams  │   │
│  Kalshi REST/WS   ─┤  │  (Layer 3)    │   │  (Price Bus)    │   │
│  Polygon/Alchemy  ─┤  └───────────────┘   └────────┬────────┘   │
│  Goldsky GraphQL  ─┤                               │            │
│  Glint Telegram   ─┤                               ▼            │
│  Hashdive Scraper ─┘                     ┌─────────────────┐    │
│                                          │    Pipeline      │    │
│                                          │  - Price Bus Pub │    │
│                                          │  - Market Matcher│    │
│                                          │  - Wallet Profiler│   │
│                                          │  - Extremizer    │    │
│                                          └────────┬────────┘    │
│                                                   │             │
│                                                   ▼             │
│                                          ┌─────────────────┐    │
│                                          │   Strategies     │    │
│                                          │  S1: Near-Res    │    │
│                                          │  S2: Insider Det │    │
│                                          │  S3: Glint News  │    │
│                                          │  S4: Intra Arb   │    │
│                                          │  S5: Cross Arb   │    │
│                                          └────────┬────────┘    │
│                                                   │             │
│                                                   ▼             │
│                                          ┌─────────────────┐    │
│                                          │ Execution Engine │    │
│                                          │  - Order Router  │    │
│                                          │  - Position Sizer│    │
│                                          │  - Risk Manager  │    │
│                                          └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Component | Technology | Justification |
|-----------|-----------|---------------|
| Language | Python 3.12 + asyncio | Async-native, rich ecosystem for data/ML |
| Time-series DB | PostgreSQL + TimescaleDB | Hypertables for market price history |
| Message Bus | Redis Streams | Low-latency pub/sub with persistence |
| Order Signing | Rust/Go microservice | Ed25519 signing off the GIL hot path |
| ML | XGBoost, sentence-transformers | Wallet scoring, semantic market matching |
| Scraping | Playwright | Headless browser for Hashdive/Oddpool |
| Deployment | AWS EC2 c6i.xlarge, us-east-1 | Low-latency to Polymarket/Kalshi servers |

## Data Flow

1. **Connectors** pull raw data from all sources → publish to venue-specific Redis Streams
2. **Price Bus Publisher** normalises prices into unified `MarketPrice` objects → publishes to `stream:prices:normalised`
3. **Market Matcher** finds cross-venue pairs → enriches normalised stream
4. **Wallet Profiler** scores wallets hourly → writes to `wallet_scores` table + `stream:insider:alerts`
5. **Strategies** consume normalised prices + signals → emit trade signals to `stream:orders:pending`
6. **Execution Engine** validates signals against risk limits → routes to correct venue connector → logs to `trades` table

## Database Schema

### market_prices (TimescaleDB hypertable)
```sql
CREATE TABLE market_prices (
    time        TIMESTAMPTZ NOT NULL,
    venue       TEXT NOT NULL,
    market_id   TEXT NOT NULL,
    title       TEXT,
    yes_price   NUMERIC(6,4),
    no_price    NUMERIC(6,4),
    spread      NUMERIC(6,4),
    volume_24h  NUMERIC(18,2)
);
SELECT create_hypertable('market_prices', 'time');
```

### wallet_scores
```sql
CREATE TABLE wallet_scores (
    wallet_address  TEXT PRIMARY KEY,
    ips_score       NUMERIC(5,3),
    win_rate_24h    NUMERIC(5,3),
    hashdive_score  INTEGER,
    last_updated    TIMESTAMPTZ
);
```

### trades
```sql
CREATE TABLE trades (
    id              SERIAL PRIMARY KEY,
    time            TIMESTAMPTZ NOT NULL,
    venue           TEXT,
    market_id       TEXT,
    strategy        TEXT,
    side            TEXT,
    price           NUMERIC(6,4),
    size            NUMERIC(18,2),
    status          TEXT,
    pnl             NUMERIC(18,6)
);
```

## Redis Streams Schema

| Stream Key | Producer | Consumer | Payload |
|-----------|----------|----------|---------|
| `stream:prices:polymarket` | polymarket_clob connector | price_bus_publisher | Raw CLOB price ticks |
| `stream:prices:kalshi` | kalshi_ws connector | price_bus_publisher | Raw Kalshi price ticks |
| `stream:prices:normalised` | price_bus_publisher | All strategies | Unified MarketPrice objects |
| `stream:arb:signals` | s4/s5 strategies | execution_engine | Arb opportunity signals |
| `stream:insider:alerts` | wallet_profiler | s2_insider_detection | High-IPS wallet trade events |
| `stream:news:signals` | glint_telegram | s3_glint_news | Parsed Glint alert signals |
| `stream:orders:pending` | All strategies | order_router | Orders queued for execution |
| `stream:orders:filled` | order_router | risk_manager, trades DB | Execution confirmations |

## Operating Principles

1. **Worktree before code** — The directory structure and directives define the plan. No implementation without a directive.
2. **Deterministic execution** — All business logic lives in Python scripts, not in agent conversation context.
3. **Self-annealing** — When scripts fail, fix them, update the directive, test again. The system must get stronger after each failure.
4. **Living directives** — API quirks, rate limits, schema changes discovered during development get written into directives immediately.
5. **`.tmp/` absorbs intermediate state** — Raw API responses, debug output, test fixtures. Never committed.
6. **Phase-gated development** — Foundation → Connectors → Pipeline → Strategies → Execution Engine → Integration. Each phase commits before the next begins.

## Edge Cases and Known Risks

- **Oracle mismatch**: Polymarket (UMA oracle) and Kalshi (CFTC team) can resolve the same event differently. Known incident: March 2025, $7M UMA vote manipulation.
- **Kalshi US-only**: Account only accessible from US IP. VPN required for non-US deployment.
- **Yes-bias on Polymarket**: Retail traders systematically overbuy Yes contracts. S1 must account for this with directional backtesting.
- **Rate limits**: Each API has different rate limits. Document them in connector directives as they are discovered.
- **Ed25519 signing latency**: Python's GIL makes signing a bottleneck under load. Offload to Rust/Go microservice.
