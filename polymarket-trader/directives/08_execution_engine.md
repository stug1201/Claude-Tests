# Directive: Execution Engine

## Goal

Provide the final layer between strategy signals and live order placement. The execution engine validates signals against risk limits, sizes positions using Kelly criterion, routes orders to the correct venue connector, and logs all activity for audit and PnL tracking.

## Components

| Component | Tool | Responsibility |
|-----------|------|---------------|
| Order Router | `execution/execution_engine/order_router.py` | Routes validated signals to venue connectors |
| Position Sizer | `execution/execution_engine/position_sizer.py` | Kelly criterion position sizing |
| Risk Manager | `execution/execution_engine/risk_manager.py` | Kill switches, position limits, drawdown monitoring |

## Input

- `stream:orders:pending` — Trade signals from all five strategies
- PostgreSQL `trades` — Historical trades for PnL tracking
- PostgreSQL `market_prices` — Current prices for position valuation
- Configuration: bankroll, per-strategy limits, risk thresholds

## Output

- Executed orders via venue connectors
- `stream:orders:filled` — Execution confirmations
- PostgreSQL `trades` — Logged with status, fill price, PnL

## Order Router (`order_router.py`)

### Processing Loop

1. **Consume** from `stream:orders:pending` via consumer group `execution_group`
2. **Validate** signal schema (required fields: strategy, venue, market_id, side, target_price, size_usd)
3. **Risk check**: Pass signal through risk_manager for approval (see below)
4. **Size**: Pass through position_sizer for final size calculation
5. **Route**: Call appropriate venue connector:
   - `venue == "polymarket"` → `polymarket_clob.place_order()`
   - `venue == "kalshi"` → `kalshi_rest.place_order()`
6. **Log**: Write to `trades` table with status `pending`
7. **Confirm**: Wait for fill confirmation (max 30 seconds)
8. **Update**: Update `trades` table with fill price, actual size, status `filled` or `failed`
9. **Publish**: Emit to `stream:orders:filled`

### Signal Schema

```json
{
    "strategy": "s1",
    "venue": "polymarket",
    "market_id": "0x1234...",
    "side": "yes",
    "target_price": 0.92,
    "size_usd": 50.00,
    "edge_estimate": 0.06,
    "confidence": "high",
    "reasoning": "..."
}
```

### Routing Logic

- **Single-venue signals** (S1, S2, S3, S4): Route to the specified venue
- **Cross-venue signals** (S5): Route both legs simultaneously. Use asyncio.gather() to place both orders concurrently. If one leg fails, handle per S5 error protocol.

## Position Sizer (`position_sizer.py`)

### Kelly Criterion

```
f* = (p * b - q) / b
```

Where:
- `p` = estimated true probability (from strategy's edge_estimate + market_price)
- `q` = 1 - p
- `b` = net odds after fees (payout ratio)
- `f*` = fraction of bankroll to risk

### Modified Kelly

Apply fractional Kelly for safety:
- **Quarter Kelly** (0.25x) for S1, S3 (model-based edges, higher uncertainty)
- **Half Kelly** (0.50x) for S4, S5 (structural arb, lower uncertainty)
- **Eighth Kelly** (0.125x) for S2 (insider detection, highest uncertainty)

### Position Size Constraints

| Constraint | Value | Rationale |
|-----------|-------|-----------|
| Max per-position | 5% of bankroll | Diversification |
| Max per-strategy | 20% of bankroll | Strategy concentration limit |
| Max per-market | 10% of bankroll | Single-event risk limit |
| Max total deployed | 80% of bankroll | Cash reserve |
| Min position | $10 | Below this, fees eat the edge |
| S5 max cross-venue | $500 per pair | Oracle risk mitigation |

### Calculation Flow

1. Receive signal with `edge_estimate` and `size_usd` (strategy's suggestion)
2. Compute Kelly fraction using strategy's estimated probability
3. Apply fractional Kelly multiplier for the strategy type
4. Apply all constraints (min, max, per-strategy, per-market)
5. Return final `approved_size_usd`

## Risk Manager (`risk_manager.py`)

### Kill Switches

| Trigger | Action | Recovery |
|---------|--------|----------|
| 24h drawdown > 15% | Halt ALL trading | Manual review + restart required |
| Single trade loss > 3% of bankroll | Halt the responsible strategy | Auto-resume after 1 hour cooldown |
| 5 consecutive losses on any strategy | Halt that strategy | Manual review |
| Venue API unreachable > 5 min | Halt trading on that venue | Auto-resume when API recovers |
| Redis connection lost | Halt all strategies | Auto-resume when Redis recovers |
| Database connection lost | Halt new trades only | Auto-resume when DB recovers |

### Position Limit Enforcement

Before approving any signal:

1. **Check per-strategy exposure**: Sum all open positions for this strategy. Reject if adding this trade would exceed 20% of bankroll.
2. **Check per-market exposure**: Sum all positions on this market_id across all strategies. Reject if exceeding 10% of bankroll.
3. **Check total exposure**: Sum all open positions across all strategies. Reject if exceeding 80% of bankroll.
4. **Check S5 cross-venue limits**: For cross-venue arb, check that total cross-venue exposure < $500 per matched pair.

### Oracle Mismatch Watchlist

- Continuously monitor matched cross-venue pairs
- If Polymarket and Kalshi prices diverge by > 8c on the same event for > 30 minutes:
  - Add pair to quarantine list
  - Reject any new S5 signals on quarantined pairs
  - Alert via log at WARNING level
  - Remove from quarantine when prices converge to within 3c

### Drawdown Calculation

```python
# Rolling 24h PnL
pnl_24h = sum(trades WHERE time > now - 24h AND status = 'filled')
drawdown_pct = abs(min(0, pnl_24h)) / bankroll
if drawdown_pct > 0.15:
    trigger_kill_switch("24h_drawdown")
```

### Bankroll Tracking

- **Starting bankroll**: Set via config (`INITIAL_BANKROLL`)
- **Current bankroll**: `INITIAL_BANKROLL + sum(all realized PnL)`
- **Refresh**: Recalculate on every trade fill
- **Separate by venue**: Track Polymarket and Kalshi balances independently

## Error Handling

- **Connector timeout**: If venue connector doesn't respond within 30 seconds, mark order as `timeout` and DO NOT retry automatically. Log at ERROR.
- **Partial fill**: Some venues support partial fills. Log the filled amount, update position tracking, and optionally place a new order for the remainder if the edge still exists.
- **Duplicate signals**: Deduplicate by `(strategy, market_id, side)` within a 60-second window. Same strategy shouldn't signal the same trade twice in a minute.
- **Database write failure**: If trade logging fails, the order MUST NOT be placed. Logging is required for risk management. No trade without audit trail.

## Testing

- `--test` mode: Process fixture signals, validate against risk checks, but do NOT call venue connectors. Log what would have been placed.
- `--dry-run` mode: Full pipeline including connector calls, but with `dry_run=True` flag that each connector must respect (validate auth, check market exists, but don't place orders)
- Risk manager tests: Verify kill switch triggers at exact thresholds, position limit enforcement, oracle quarantine logic

## Edge Cases

- **Race condition between strategies**: Two strategies may signal trades on the same market simultaneously. The risk manager must serialize risk checks to prevent over-allocation.
- **Market closes during execution**: If a market closes between signal and execution, the connector will return an error. Handle gracefully — do not retry on a closed market.
- **Bankroll goes negative**: If realized PnL takes bankroll below $0 (shouldn't happen with proper risk management), halt everything and alert. This indicates a bug in position sizing or risk management.
- **Clock drift**: Use NTP-synchronized server time for all timestamp comparisons. A 5-second drift can cause incorrect drawdown calculations.
