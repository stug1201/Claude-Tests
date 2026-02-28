# Subagent Delegation Plan

## Overview

This document maps every execution component to a subagent role, defines the serialisation order (phases), and provides the exact prompt templates for each subagent. The orchestrator spawns subagents per phase — all within a phase run in parallel, but phases are sequential.

---

## Component-to-Subagent Mapping

| Component | File | Subagent Role | Phase |
|-----------|------|--------------|-------|
| Config loader | `execution/utils/config.py` | foundation-agent | 1 |
| Database helpers | `execution/utils/db.py` | foundation-agent | 1 |
| Redis helpers | `execution/utils/redis_client.py` | foundation-agent | 1 |
| Polymarket CLOB | `execution/connectors/polymarket_clob.py` | api-connector-agent | 2 |
| Polymarket Gamma | `execution/connectors/polymarket_gamma.py` | api-connector-agent | 2 |
| Polymarket Data API | `execution/connectors/polymarket_data_api.py` | api-connector-agent | 2 |
| Kalshi REST | `execution/connectors/kalshi_rest.py` | api-connector-agent | 2 |
| Kalshi WebSocket | `execution/connectors/kalshi_ws.py` | api-connector-agent | 2 |
| Polygon RPC | `execution/connectors/polygon_rpc.py` | api-connector-agent | 2 |
| Goldsky GraphQL | `execution/connectors/goldsky_graphql.py` | api-connector-agent | 2 |
| Glint Telegram | `execution/connectors/glint_telegram.py` | api-connector-agent | 2 |
| Hashdive Scraper | `execution/connectors/hashdive_scraper.py` | scraper-agent | 2 |
| Price Bus Publisher | `execution/pipeline/price_bus_publisher.py` | pipeline-agent | 3 |
| Market Matcher | `execution/pipeline/market_matcher.py` | pipeline-agent | 3 |
| Wallet Profiler | `execution/pipeline/wallet_profiler.py` | pipeline-agent | 3 |
| Extremizer | `execution/pipeline/extremizer.py` | pipeline-agent | 3 |
| S1 Near-Resolution | `execution/strategies/s1_near_resolution.py` | strategy-agent | 4 |
| S2 Insider Detection | `execution/strategies/s2_insider_detection.py` | strategy-agent | 4 |
| S3 Glint News | `execution/strategies/s3_glint_news.py` | strategy-agent | 4 |
| S4 Intra-venue Arb | `execution/strategies/s4_intramarket_arb.py` | strategy-agent | 4 |
| S5 Cross-venue Arb | `execution/strategies/s5_crossvenue_arb.py` | strategy-agent | 4 |
| Order Router | `execution/execution_engine/order_router.py` | execution-agent | 5 |
| Position Sizer | `execution/execution_engine/position_sizer.py` | execution-agent | 5 |
| Risk Manager | `execution/execution_engine/risk_manager.py` | execution-agent | 5 |
| Unit Tests | `tests/unit/` | test-agent | 6 |
| Integration Tests | `tests/integration/` | test-agent | 6 |

---

## Phase Definitions

### Phase 1: Foundation (Zero Dependencies)

**Components**: `config.py`, `db.py`, `redis_client.py`

**Rationale**: Everything else depends on these utilities. They must work before any connector, pipeline, or strategy can be built.

**Parallel subagents**: 3 (one per utility file)

**Acceptance criteria**:
- `config.py` loads `.env`, validates required vars, provides typed access
- `db.py` creates async connection pool, provides insert/query helpers for all 3 tables
- `redis_client.py` creates connection, initialises consumer groups, provides publish/consume helpers
- All three support `--test` mode without external services
- All three are importable as modules from other scripts

---

### Phase 2: Connectors (Depend on Phase 1 utils)

**Components**: All 9 connector files in `execution/connectors/`

**Rationale**: Connectors are the data sources. They can all be built in parallel because they only depend on Phase 1 utils and their own external API.

**Parallel subagents**: 9 (one per connector)

**Acceptance criteria**:
- Each connector imports from `execution.utils.config` and `execution.utils.redis_client`
- Each connector has `--test` mode with fixture data
- Each connector publishes to the appropriate Redis Stream
- Each connector handles auth, rate limits, and reconnection
- Kalshi REST must implement proactive token refresh at 25min mark

---

### Phase 3: Pipeline (Depend on Phase 2 connectors)

**Components**: `price_bus_publisher.py`, `market_matcher.py`, `wallet_profiler.py`, `extremizer.py`

**Rationale**: Pipeline components transform and enrich raw data from connectors. They can be built in parallel.

**Parallel subagents**: 4

**Acceptance criteria**:
- `price_bus_publisher.py` reads from venue streams, normalises, publishes to `stream:prices:normalised`
- `market_matcher.py` computes embeddings, finds cross-venue and intra-venue matches
- `wallet_profiler.py` computes IPS scores, trains XGBoost classifier
- `extremizer.py` implements logit extremizing transform and alpha recalibration
- All import from Phase 1 utils and Phase 2 connectors as needed

---

### Phase 4: Strategies (Depend on Phase 3 pipeline)

**Components**: All 5 strategy files

**Rationale**: Strategies consume enriched pipeline data and emit trade signals. All can be built in parallel.

**Parallel subagents**: 5

**Acceptance criteria**:
- Each strategy reads from `stream:prices:normalised` (and other relevant streams)
- Each strategy emits signals to `stream:orders:pending`
- Each strategy has `--test` and `--backtest` modes
- Position sizing logic matches the directive specification
- Minimum edge thresholds are correctly implemented

---

### Phase 5: Execution Engine (Depend on Phase 4 strategies)

**Components**: `order_router.py`, `position_sizer.py`, `risk_manager.py`

**Rationale**: The execution engine consumes strategy signals and must understand the signal format and risk parameters.

**Parallel subagents**: 3

**Acceptance criteria**:
- `order_router.py` consumes from `stream:orders:pending`, routes to correct connector
- `position_sizer.py` implements Kelly criterion with per-strategy fractional multipliers
- `risk_manager.py` implements all kill switch levels, drawdown monitoring, oracle watchlist
- Risk checks are serialised to prevent race conditions
- All have `--test` and `--dry-run` modes

---

### Phase 6: Integration (Depend on all previous phases)

**Components**: Tests, deployment scripts, operational validation

**Parallel subagents**: 2 (test-agent, devops-agent)

**Acceptance criteria**:
- Unit tests for all utility modules, pipeline components, and position sizer
- Integration test: full pipeline from fixture data through to order signals
- All tests pass in `--test` mode without external services
- Deployment documentation verified against actual infrastructure

---

## Subagent Prompt Templates

### Phase 1: Foundation Agent — config.py

```
You are a foundation-agent building the configuration module for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md to understand the 3-layer architecture
2. Read /home/user/Claude-Tests/polymarket-trader/directives/10_ops_and_deployment.md for infrastructure details
3. Read /home/user/Claude-Tests/polymarket-trader/.env.example for all environment variables
4. Check /home/user/Claude-Tests/polymarket-trader/execution/utils/ for any existing code

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/utils/config.py

REQUIREMENTS:
- Load .env file using python-dotenv
- Validate required variables at import time: POSTGRES_URL, REDIS_URL
- Provide typed access via a Config class with properties
- get_optional() method for non-required vars that returns None if missing
- get_required() method that raises ValueError with clear error message
- Support INITIAL_BANKROLL (float, default 1000) and MAX_DRAWDOWN_PCT (float, default 0.15)
- Follow the crypto-digest script pattern: argparse with --test flag, logging setup, section separators
- --test mode should work without a .env file (use defaults)
- Well-commented, testable code

OUTPUT: A complete, working config.py file
```

### Phase 1: Foundation Agent — db.py

```
You are a foundation-agent building the database module for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md
2. Read /home/user/Claude-Tests/polymarket-trader/directives/00_system_overview.md for the database schema
3. Read /home/user/Claude-Tests/polymarket-trader/directives/10_ops_and_deployment.md for PostgreSQL setup
4. Read /home/user/Claude-Tests/polymarket-trader/execution/utils/config.py (Phase 1 dependency)

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/utils/db.py

REQUIREMENTS:
- Async connection pool using asyncpg
- Import config from execution.utils.config
- Functions:
  - get_pool() -> asyncpg.Pool (singleton, lazy init)
  - close_pool() -> None
  - insert_market_price(time, venue, market_id, title, yes_price, no_price, spread, volume_24h)
  - get_recent_prices(market_id, venue, limit=100) -> list[dict]
  - insert_trade(venue, market_id, strategy, side, price, size, status) -> int
  - update_trade(trade_id, status, pnl) -> None
  - get_trades(strategy=None, status=None, since=None) -> list[dict]
  - upsert_wallet_score(wallet_address, ips_score, win_rate_24h, hashdive_score) -> None
  - get_wallet_score(wallet_address) -> dict | None
  - run_migrations() -> None (executes schema SQL)
- --test mode: use an in-memory mock or skip DB operations, return fixture data
- All functions are async
- Connection pool size: 5-20 connections
- Proper error handling with logging

OUTPUT: A complete, working db.py file
```

### Phase 1: Foundation Agent — redis_client.py

```
You are a foundation-agent building the Redis Streams module for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md
2. Read /home/user/Claude-Tests/polymarket-trader/directives/02_unified_price_bus.md for stream schema
3. Read /home/user/Claude-Tests/polymarket-trader/directives/00_system_overview.md for stream list
4. Read /home/user/Claude-Tests/polymarket-trader/execution/utils/config.py (Phase 1 dependency)

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/utils/redis_client.py

REQUIREMENTS:
- Async Redis client using redis.asyncio
- Import config from execution.utils.config
- Stream constants for all 8 streams (PRICES_POLYMARKET, PRICES_KALSHI, PRICES_NORMALISED, ARB_SIGNALS, INSIDER_ALERTS, NEWS_SIGNALS, ORDERS_PENDING, ORDERS_FILLED)
- Functions:
  - get_redis() -> redis.asyncio.Redis (singleton, lazy init)
  - close_redis() -> None
  - ensure_consumer_groups() -> None (creates all groups, ignores if exist)
  - publish(stream: str, data: dict, maxlen: int = 100000) -> str (message ID)
  - consume(stream: str, group: str, consumer: str, count: int = 10, block: int = 1000) -> list[dict]
  - ack(stream: str, group: str, message_id: str) -> None
- All functions are async
- Use orjson for fast JSON serialization of stream values
- --test mode: use a mock that stores messages in memory
- Reconnection with exponential backoff on connection loss
- Proper logging

OUTPUT: A complete, working redis_client.py file
```

### Phase 2: API Connector Agent (template — customise per connector)

```
You are an api-connector-agent building the {CONNECTOR_NAME} connector for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md
2. Read /home/user/Claude-Tests/polymarket-trader/directives/01_data_infrastructure.md
3. Read /home/user/Claude-Tests/polymarket-trader/execution/utils/config.py
4. Read /home/user/Claude-Tests/polymarket-trader/execution/utils/redis_client.py
5. Read /home/user/Claude-Tests/polymarket-trader/execution/utils/db.py

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/connectors/{CONNECTOR_FILE}

REQUIREMENTS:
- {CONNECTOR_SPECIFIC_REQUIREMENTS}
- Import and use config, redis_client, and db from execution.utils
- Async throughout (asyncio + aiohttp or httpx)
- --test mode with fixture data from .tmp/fixtures/
- Proper logging (ISO 8601 timestamps)
- Error handling: auth failures, rate limits, network timeouts
- Exponential backoff on retries
- Publish data to appropriate Redis Stream
- Follow the execution script pattern from crypto-digest (docstring, argparse, sections)

OUTPUT: A complete, working connector file
```

### Phase 3: Pipeline Agent (template)

```
You are a pipeline-agent building the {PIPELINE_COMPONENT} for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md
2. Read /home/user/Claude-Tests/polymarket-trader/directives/{RELEVANT_DIRECTIVE}
3. Read all files in /home/user/Claude-Tests/polymarket-trader/execution/utils/
4. Read relevant connector files in /home/user/Claude-Tests/polymarket-trader/execution/connectors/

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/pipeline/{PIPELINE_FILE}

REQUIREMENTS:
- {PIPELINE_SPECIFIC_REQUIREMENTS}
- Import from execution.utils (config, db, redis_client)
- Import from execution.connectors as needed
- Async throughout
- --test mode
- Well-documented edge case handling

OUTPUT: A complete, working pipeline component
```

### Phase 4: Strategy Agent (template)

```
You are a strategy-agent building {STRATEGY_NAME} for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md
2. Read /home/user/Claude-Tests/polymarket-trader/directives/{STRATEGY_DIRECTIVE}
3. Read all files in /home/user/Claude-Tests/polymarket-trader/execution/utils/
4. Read relevant pipeline files in /home/user/Claude-Tests/polymarket-trader/execution/pipeline/
5. Read relevant connector files if the strategy uses them directly

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/strategies/{STRATEGY_FILE}

REQUIREMENTS:
- Implement the full strategy logic as specified in the directive
- Consume from stream:prices:normalised (and other streams per directive)
- Emit trade signals to stream:orders:pending with the exact signal schema
- --test mode with fixture data
- --backtest mode for historical replay (where applicable)
- Position sizing per directive specification
- Minimum edge thresholds per directive
- All edge cases from directive must be handled

OUTPUT: A complete, working strategy file
```

### Phase 5: Execution Agent (template)

```
You are an execution-agent building the {COMPONENT_NAME} for the polymarket-trader project.

BEFORE STARTING:
1. Read /home/user/Claude-Tests/agents.md
2. Read /home/user/Claude-Tests/polymarket-trader/directives/08_execution_engine.md
3. Read /home/user/Claude-Tests/polymarket-trader/directives/09_risk_management.md
4. Read all files in /home/user/Claude-Tests/polymarket-trader/execution/utils/
5. Read strategy files to understand the signal format they emit

YOUR TASK:
Implement /home/user/Claude-Tests/polymarket-trader/execution/execution_engine/{COMPONENT_FILE}

REQUIREMENTS:
- {COMPONENT_SPECIFIC_REQUIREMENTS}
- Import from execution.utils (config, db, redis_client)
- Async throughout
- --test and --dry-run modes
- Thread-safe risk checks (asyncio.Lock)

OUTPUT: A complete, working execution engine component
```

---

## Three Highest-Risk Components

These components are most likely to fail on first run and should be assigned to self-annealing subagents with explicit instructions to update their directive on failure.

### 1. `execution/connectors/polymarket_clob.py` — HIGH RISK

**Why**: Ed25519 signing is cryptographically precise. A single byte wrong in the signature computation means every API call fails with auth errors. The `py_clob_client` library may have version-specific quirks. The HMAC signing process is poorly documented.

**Mitigation**: Self-annealing subagent must:
- Test signature generation against known vectors BEFORE making API calls
- If auth fails, inspect the exact error response and update the directive with the error format
- Try both `py_clob_client` and manual signing to find what works
- Update `directives/01_data_infrastructure.md` with discovered auth patterns

### 2. `execution/connectors/kalshi_rest.py` — HIGH RISK

**Why**: RSA-PSS token auth with 30-minute TTL requires precise timing. The login endpoint may have undocumented requirements (CSRF tokens, rate limits on login, IP restrictions). Token refresh at exactly the 25-minute mark is critical — too early wastes tokens, too late causes auth failures mid-operation.

**Mitigation**: Self-annealing subagent must:
- Test the login flow independently before building the full connector
- Log exact response headers and body on auth failure
- Implement and test the proactive refresh mechanism
- Update directive with discovered token behavior (actual TTL, refresh response format)

### 3. `execution/connectors/hashdive_scraper.py` — HIGH RISK

**Why**: Playwright scraping against anti-bot protections is inherently fragile. Page structure may change, XHR endpoints may shift, session cookies may expire quickly. Stealth mode is required but not guaranteed to work.

**Mitigation**: Self-annealing subagent must:
- Save raw HTML to `.tmp/` on every scrape for debugging
- Capture and log XHR request/response pairs
- If scraping fails, update directive with the specific anti-bot mechanism encountered
- Implement graceful degradation: if Hashdive is down, the system should continue without it (S2 uses Hashdive as bootstrap labels only)

---

## Phase Completion Checklist

After each phase completes:

- [ ] All subagent files are written and syntactically valid
- [ ] `python -c "import execution.utils.config"` (or equivalent) succeeds
- [ ] `--test` mode runs without errors for each component
- [ ] No import errors between components within the phase
- [ ] Directives updated with any discoveries
- [ ] Git commit: `feat(phase-N): description`
- [ ] SUBAGENT_PLAN.md updated with lessons learned

---

## Serialisation Diagram

```
Phase 1 (Foundation)          Phase 2 (Connectors)           Phase 3 (Pipeline)
┌──────────┐                  ┌──────────────────┐           ┌──────────────────┐
│ config   │──┐               │ polymarket_clob  │──┐        │ price_bus_pub    │
│ db       │──┼──▶ Phase 2 ──▶│ polymarket_gamma │  │        │ market_matcher   │
│ redis    │──┘               │ polymarket_data  │  │        │ wallet_profiler  │
└──────────┘                  │ kalshi_rest      │  ├──▶ P3 ▶│ extremizer       │
                              │ kalshi_ws        │  │        └────────┬─────────┘
                              │ polygon_rpc      │  │                 │
                              │ goldsky_graphql  │  │                 ▼
                              │ glint_telegram   │  │        Phase 4 (Strategies)
                              │ hashdive_scraper │──┘        ┌──────────────────┐
                              └──────────────────┘           │ s1_near_res      │
                                                             │ s2_insider       │
                                                             │ s3_glint_news    │──▶ P5
                                                             │ s4_intra_arb     │
                                                             │ s5_cross_arb     │
                                                             └──────────────────┘
                                                                      │
                                                                      ▼
                                                             Phase 5 (Execution)
                                                             ┌──────────────────┐
                                                             │ order_router     │
                                                             │ position_sizer   │──▶ P6
                                                             │ risk_manager     │
                                                             └──────────────────┘
                                                                      │
                                                                      ▼
                                                             Phase 6 (Integration)
                                                             ┌──────────────────┐
                                                             │ unit tests       │
                                                             │ integration tests│
                                                             └──────────────────┘
```
