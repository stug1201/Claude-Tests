# Polymarket Trader

Automated prediction market trading system executing five independent strategies across Polymarket and Kalshi, fed by a unified real-time Redis Streams price bus, with risk management and full auditability.

## Architecture

This project follows the 3-layer architecture defined in `agents.md`:

- `directives/` — Markdown SOPs defining each component's contract
- `execution/` — Deterministic Python scripts that do the work
- `.tmp/` — Ephemeral intermediate files (never committed)

### Data Flow

```
Connectors → Redis Streams (Price Bus) → Pipeline → Strategies → Execution Engine
```

1. **Connectors** pull raw data from Polymarket, Kalshi, Goldsky, Glint, Hashdive
2. **Price Bus** normalises into unified `MarketPrice` objects
3. **Pipeline** enriches data (market matching, wallet profiling, extremizing)
4. **Strategies** (S1-S5) consume enriched data and emit trade signals
5. **Execution Engine** validates against risk limits and routes orders to venues

### Strategies

| ID | Name | Description |
|----|------|-------------|
| S1 | Near-Resolution | Markets approaching resolution with high-confidence pricing |
| S2 | Insider Detection | Piggyback on wallets exhibiting insider-like patterns |
| S3 | Glint News | Bayesian updates from Glint Telegram signals + extremizing |
| S4 | Intra-venue Arb | Yes/No mismatches and logical contradictions within Polymarket |
| S5 | Cross-venue Arb | Price discrepancies between Polymarket and Kalshi |

## Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 16 with TimescaleDB extension
- Redis 7.x
- AWS EC2 c6i.xlarge in us-east-1 (for production)

### Installation

```bash
# Clone and enter project
cd polymarket-trader

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (for Hashdive scraper)
playwright install chromium

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your credentials
```

### Database Setup

```bash
# Create database and run migrations
psql -U postgres -c "CREATE DATABASE polymarket_trader;"
psql -U postgres -d polymarket_trader -f schema/001_initial.sql
```

### Running

Each component runs as an independent process. In production, use systemd or supervisord.

```bash
# Start connectors
python execution/connectors/polymarket_clob.py &
python execution/connectors/kalshi_ws.py &

# Start pipeline
python execution/pipeline/price_bus_publisher.py &

# Start strategies
python execution/strategies/s1_near_resolution.py &

# Start execution engine
python execution/execution_engine/order_router.py &
python execution/execution_engine/risk_manager.py &
```

### Testing

```bash
# Run with fixture data (no external dependencies)
python execution/connectors/polymarket_clob.py --test

# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/
```

## File Structure

```
polymarket-trader/
├── directives/           # SOPs for each component
├── execution/
│   ├── connectors/       # API and scraping connectors
│   ├── pipeline/         # Data enrichment layer
│   ├── strategies/       # Trading strategies (S1-S5)
│   ├── execution_engine/ # Order routing and risk management
│   └── utils/            # Shared helpers (db, redis, config)
├── tests/
│   ├── unit/
│   └── integration/
├── .tmp/                 # Intermediate data (not committed)
├── .env.example          # Environment variable template
├── requirements.txt      # Python dependencies
└── LESSONS_LEARNED.md    # Post-mortem insights
```

## Risk Management

- Global kill switch at 15% 24h drawdown
- Per-strategy position limits (max 20% of bankroll)
- Per-position cap (max 5% of bankroll)
- Oracle mismatch watchlist for cross-venue arb
- See `directives/09_risk_management.md` for full details
