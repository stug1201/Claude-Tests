# Directive: Risk Management

## Goal

Define the comprehensive risk framework that protects the trading system from catastrophic losses, ensures position limits are enforced, monitors oracle mismatch risk, and provides a clear escalation path when things go wrong.

## Scope

Risk management is embedded at multiple layers:
1. **Position Sizer** — Prevents oversized individual trades (Kelly criterion bounds)
2. **Risk Manager** — Real-time monitoring of portfolio exposure, drawdown, kill switches
3. **Strategy-level guards** — Each strategy has its own minimum edge thresholds and filters
4. **Operational risk** — Infrastructure monitoring, API health checks, data staleness

This directive covers the **system-wide risk framework**. Strategy-specific risk is in each strategy directive (03-07).

## Risk Parameters

### Portfolio-Level Limits

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max drawdown (24h rolling) | 15% | Prevent catastrophic loss spiral |
| Max total deployed capital | 80% of bankroll | Maintain cash reserve |
| Max single position | 5% of bankroll | Diversification |
| Max per-strategy exposure | 20% of bankroll | Strategy concentration |
| Max per-market exposure | 10% of bankroll | Event concentration |
| Min position size | $10 | Below this, fees dominate |

### Strategy-Specific Limits

| Strategy | Max Exposure | Min Edge | Position Size Method |
|----------|-------------|----------|---------------------|
| S1 (Near-Res) | 20% | 3c after fees | Quarter Kelly |
| S2 (Insider) | 15% | 5c estimated | Eighth Kelly |
| S3 (Glint News) | 15% | 5c after fees | Quarter Kelly |
| S4 (Intra Arb) | 15% | 3c after fees | Half Kelly |
| S5 (Cross Arb) | $500/pair | 2.5% net ROI | Half Kelly, hard cap |

### Kill Switch Hierarchy

```
Level 0: INFORMATIONAL
  - Log event, continue trading
  - Examples: single trade loss < 1%, minor API latency spike

Level 1: STRATEGY HALT
  - Halt the specific strategy that triggered the event
  - Auto-resume after cooldown period
  - Examples: 3 consecutive losses, single trade loss 1-3%

Level 2: VENUE HALT
  - Halt all trading on a specific venue
  - Auto-resume when condition clears
  - Examples: venue API down, venue-specific error rate > 10%

Level 3: GLOBAL HALT
  - Halt ALL trading across all strategies and venues
  - MANUAL restart required
  - Examples: 24h drawdown > 15%, bankroll < $100, 5+ consecutive losses across all strategies
```

## Oracle Mismatch Risk

### Background

Polymarket and Kalshi use fundamentally different resolution mechanisms:
- **Polymarket**: UMA oracle — decentralized token vote. Can be manipulated by whale token holders.
- **Kalshi**: CFTC-regulated team — internal determination following regulatory procedures.

These can disagree on the same real-world event, destroying cross-venue arb positions.

### Known Incident

**March 2025**: A whale accumulated UMA tokens and manipulated a $7M vote on Polymarket, causing it to resolve opposite to Kalshi on the same event. Cross-venue arb positions were wiped out.

### Mitigation Protocol

1. **Market classification**:
   - **LOW oracle risk**: Sports scores, economic data releases (CPI/GDP/jobs), certified election results, measurable numerical thresholds
   - **MEDIUM oracle risk**: Court rulings, regulatory decisions (definitive but interpretable)
   - **HIGH oracle risk**: Diplomatic events, deals, agreements, announcements, any subjective or ambiguous criteria

2. **Keyword filter**: Auto-flag markets containing: "deal", "agreement", "announcement", "negotiate", "propose", "consider", "likely", "expected"

3. **Divergence watchlist**:
   - Monitor: price difference between matched cross-venue pairs
   - Threshold: > 8c divergence sustained for > 30 minutes
   - Action: Quarantine pair, reject new S5 signals, alert operator
   - Recovery: Remove from quarantine when divergence < 3c for > 1 hour

4. **Position cap**: Maximum $500 per cross-venue matched pair until oracle agreement rate is empirically validated over 50+ resolutions

## Drawdown Monitoring

### Rolling 24h Drawdown

```python
def calculate_drawdown(trades: list, bankroll: float) -> float:
    """Calculate rolling 24h max drawdown as percentage of bankroll."""
    now = datetime.utcnow()
    recent_trades = [t for t in trades if t.time > now - timedelta(hours=24)]

    if not recent_trades:
        return 0.0

    cumulative_pnl = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0

    for trade in sorted(recent_trades, key=lambda t: t.time):
        cumulative_pnl += trade.pnl
        peak_pnl = max(peak_pnl, cumulative_pnl)
        drawdown = peak_pnl - cumulative_pnl
        max_drawdown = max(max_drawdown, drawdown)

    return max_drawdown / bankroll
```

### Drawdown Response Curve

| Drawdown | Response |
|----------|----------|
| 0-5% | Normal operation |
| 5-10% | Reduce all position sizes to 50% of computed Kelly |
| 10-15% | Reduce to 25% of Kelly, halt S2 (highest uncertainty) |
| > 15% | GLOBAL HALT (Level 3 kill switch) |

## Operational Risk

### API Health Monitoring

- Ping each venue API every 60 seconds with a lightweight health check
- Track response latency (p50, p95, p99)
- If p99 > 5 seconds for any venue, reduce order frequency on that venue
- If API returns 5xx errors > 3 times in 5 minutes, trigger Level 2 (VENUE HALT)

### Data Staleness

- Track timestamp of last received price tick per venue
- If no ticks received for > 2 minutes, mark venue data as STALE
- Strategies must not trade on stale data — check freshness before emitting signals
- Log at WARNING after 2 minutes, ERROR after 5 minutes

### Infrastructure Health

- Redis: Ping every 30 seconds. If unreachable, buffer in memory (max 1000 messages), trigger Level 2
- PostgreSQL: Connection pool health check every 60 seconds. If pool exhausted, queue writes (max 100), trigger Level 2
- Disk space: Alert if < 1GB free (`.tmp/` can grow unbounded)

## Reporting

### Real-Time Metrics (logged every 5 minutes)

- Total bankroll and current deployment %
- Per-strategy PnL (realized + unrealized)
- Number of open positions per strategy
- 24h drawdown %
- Kill switch status (any active?)
- API latency per venue

### Daily Summary (logged at midnight UTC)

- Total PnL for the day
- Strategy performance ranking
- Markets traded
- Risk events triggered
- Oracle divergence events

## Testing

- `--test` mode: Simulate portfolio states and verify kill switch triggers at exact thresholds
- Stress test: Feed a sequence of losing trades and verify drawdown response curve activates correctly
- Oracle test: Simulate price divergence between venues and verify quarantine logic
- Edge case test: Verify behavior when bankroll exactly equals threshold values

## Edge Cases

- **Multiple kill switches simultaneously**: Process in priority order (Level 3 > Level 2 > Level 1). The highest level takes precedence.
- **Kill switch during open orders**: Cancel all pending orders immediately when a kill switch triggers. Do not wait for fills.
- **Bankroll calculation during settlement**: Some trades may be in `pending_settlement` state. Include them in exposure calculation but not in realized PnL.
- **Time zone issues**: All risk calculations use UTC. Never use local time. Ensure NTP synchronization.
- **Concurrent risk checks**: Use asyncio.Lock() to serialize risk checks and prevent race conditions between strategies.
