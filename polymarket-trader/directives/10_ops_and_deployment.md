# Directive: Operations and Deployment

## Goal

Define the infrastructure setup, deployment procedures, and operational runbooks for running the polymarket-trader system on AWS EC2 with PostgreSQL/TimescaleDB and Redis.

## Infrastructure

### Compute

| Component | Instance | Region | Notes |
|-----------|----------|--------|-------|
| Main trading server | AWS EC2 c6i.xlarge | us-east-1 | 4 vCPU, 8GB RAM. Must be US-based for Kalshi access |
| Database | PostgreSQL 16 + TimescaleDB | Same VPC | Can be RDS or self-hosted on EC2 |
| Redis | Redis 7.x | Same VPC | Can be ElastiCache or self-hosted |

### Why us-east-1

- **Kalshi requirement**: Kalshi API only accessible from US IPs. Non-US deployment requires VPN routing.
- **Latency**: Both Polymarket (Polygon network) and Kalshi servers are in US East.
- **AWS services**: Broadest service availability in us-east-1.

### Network

- VPC with private subnets for database/Redis
- Public subnet for EC2 with elastic IP
- Security groups:
  - EC2: Inbound SSH (22) from admin IPs only. Outbound: all (API access)
  - PostgreSQL: Inbound 5432 from EC2 security group only
  - Redis: Inbound 6379 from EC2 security group only

## Database Setup

### PostgreSQL + TimescaleDB

```bash
# Install TimescaleDB extension on PostgreSQL 16
sudo apt install postgresql-16 postgresql-16-timescaledb

# Enable extension
psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# Create database
psql -U postgres -c "CREATE DATABASE polymarket_trader;"

# Run schema migrations
psql -U postgres -d polymarket_trader -f schema/001_initial.sql
```

### Schema Migration: 001_initial.sql

```sql
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
```

## Redis Setup

```bash
# Install Redis 7.x
sudo apt install redis-server

# Configuration tweaks for streams
# /etc/redis/redis.conf:
maxmemory 2gb
maxmemory-policy allkeys-lru
stream-node-max-bytes 4096
stream-node-max-entries 100

# Start Redis
sudo systemctl enable redis-server
sudo systemctl start redis-server
```

### Stream Initialization

On first startup, the application should create consumer groups:

```python
# Consumer groups (created by redis_client.py on startup)
XGROUP CREATE stream:prices:polymarket price_bus_group $ MKSTREAM
XGROUP CREATE stream:prices:kalshi price_bus_group $ MKSTREAM
XGROUP CREATE stream:prices:normalised strategy_group $ MKSTREAM
XGROUP CREATE stream:orders:pending execution_group $ MKSTREAM
XGROUP CREATE stream:orders:filled audit_group $ MKSTREAM
XGROUP CREATE stream:arb:signals arb_group $ MKSTREAM
XGROUP CREATE stream:insider:alerts insider_group $ MKSTREAM
XGROUP CREATE stream:news:signals news_group $ MKSTREAM
```

## Application Deployment

### Process Management

Use `systemd` or `supervisord` to manage long-running processes:

| Process | Script | Restart Policy |
|---------|--------|---------------|
| Price Bus Publisher | `execution/pipeline/price_bus_publisher.py` | Always restart (5s delay) |
| Polymarket Connector | `execution/connectors/polymarket_clob.py` | Always restart (5s delay) |
| Kalshi WS Connector | `execution/connectors/kalshi_ws.py` | Always restart (10s delay) |
| Glint Telegram | `execution/connectors/glint_telegram.py` | Always restart (10s delay) |
| Wallet Profiler | `execution/pipeline/wallet_profiler.py` | Always restart (60s delay) |
| S1 Strategy | `execution/strategies/s1_near_resolution.py` | Always restart (5s delay) |
| S2 Strategy | `execution/strategies/s2_insider_detection.py` | Always restart (5s delay) |
| S3 Strategy | `execution/strategies/s3_glint_news.py` | Always restart (5s delay) |
| S4 Strategy | `execution/strategies/s4_intramarket_arb.py` | Always restart (5s delay) |
| S5 Strategy | `execution/strategies/s5_crossvenue_arb.py` | Always restart (5s delay) |
| Order Router | `execution/execution_engine/order_router.py` | Always restart (3s delay) |
| Risk Manager | `execution/execution_engine/risk_manager.py` | Always restart (3s delay) |

### Startup Order

1. Redis and PostgreSQL must be running
2. Start connectors (data sources)
3. Start pipeline (price bus, wallet profiler)
4. Start strategies (consume normalised data)
5. Start execution engine (consume trade signals)

### Environment Variables

All secrets in `.env` file, loaded by `python-dotenv`:

```bash
# See .env.example for full list
# NEVER commit .env to git
# Copy .env.example to .env and fill in values
```

## Monitoring

### Health Checks

- Each process should expose a health endpoint or write a heartbeat file to `.tmp/heartbeats/`
- A monitor script checks heartbeat freshness every 60 seconds
- Alert if any process heartbeat is stale (> 2x expected interval)

### Logging

- All processes log to stdout (captured by systemd journal)
- Log format: `%(asctime)s [%(levelname)s] %(name)s: %(message)s`
- Log rotation: systemd journal handles this
- Critical events also logged to PostgreSQL `system_events` table (future enhancement)

### Metrics to Track

- API response times per venue (p50, p95, p99)
- Messages processed per stream per minute
- Trading PnL (realized + unrealized) per strategy
- Redis memory usage
- PostgreSQL connection pool utilization
- Disk space in `.tmp/`

## Backup and Recovery

### Database Backups

- Daily `pg_dump` to S3: `pg_dump polymarket_trader | gzip | aws s3 cp - s3://backups/daily/$(date +%Y%m%d).sql.gz`
- TimescaleDB continuous aggregates for fast recovery of summary data
- Point-in-time recovery via WAL archiving (if using RDS, this is automatic)

### Redis Persistence

- RDB snapshots every 5 minutes
- AOF logging for Redis Streams (ensures no message loss on restart)
- On catastrophic Redis failure: strategies can cold-start from PostgreSQL historical data

### Disaster Recovery

1. Launch new EC2 instance from AMI
2. Restore PostgreSQL from latest S3 backup
3. Redis will rebuild streams from connectors on restart
4. Verify `.env` is configured
5. Start processes in order

## Security

- **Secrets management**: `.env` file with strict permissions (`chmod 600`)
- **SSH access**: Key-based only, no password auth
- **API keys**: Rotate quarterly. Never log API keys.
- **Network**: All internal communication over private VPC. No public access to DB/Redis.
- **Ed25519 private key**: Most sensitive credential. Consider AWS Secrets Manager for production.

## Testing

- `--test` mode for each component: No external dependencies, fixture data
- Integration test: Full pipeline with fixture data, all components running locally
- Staging environment: Separate EC2 instance, separate database, paper-trading mode (orders validated but not submitted)

## Edge Cases

- **EC2 instance restart**: systemd services auto-restart. Strategies resume from last Redis consumer position.
- **Network partition**: If EC2 loses internet, all connectors will fail. Kill switches activate automatically (venue API unreachable).
- **Disk full**: `.tmp/` can grow unbounded. Add cron job to clean files older than 24h.
- **Clock drift**: Use NTP. Install `chrony` and verify synchronization on startup.
- **AWS region outage**: us-east-1 outages are rare but impactful. No automated failover — manual migration to us-east-2 if needed.
