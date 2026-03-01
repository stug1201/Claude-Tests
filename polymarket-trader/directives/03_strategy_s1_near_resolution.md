# Directive: Strategy S1 — Near-Resolution Scanner

## Goal

Identify prediction markets approaching resolution where the current price implies high probability but may still be systematically mispriced. Capture the final convergence to $1.00 on correctly-priced high-confidence markets, while avoiding the Yes-bias trap on Polymarket.

## Input

- `stream:prices:normalised` — Unified MarketPrice objects with `close_time` and `yes_price`
- PostgreSQL `market_prices` — Historical price data for backtesting
- PostgreSQL `trades` — Past S1 trades for performance tracking

## Tool

`execution/strategies/s1_near_resolution.py`

## Output

- Trade signals → `stream:orders:pending`
- Signal format:
  ```json
  {
      "strategy": "s1",
      "venue": "polymarket",
      "market_id": "0x1234...",
      "side": "yes",
      "target_price": 0.92,
      "size_usd": 50.00,
      "edge_estimate": 0.06,
      "confidence": "medium",
      "reasoning": "7-day market at 92c, no Yes-bias flag, modified Kelly suggests 3.2% allocation"
  }
  ```

## Strategy Logic

### Tier 1: Short-Horizon (< 7 days to resolution)

1. **Scan**: Filter all normalised prices where `close_time < now + 7 days` AND `yes_price > 0.88`
2. **Yes-bias check**:
   - CRITICAL: Polymarket has a documented Yes-bias where retail traders systematically overbuy Yes contracts
   - Do NOT assume a 95c market is underpriced without evidence
   - Require: the market has been above 90c for at least 48 hours (not a recent spike)
   - Require: volume_24h > $10,000 (liquid market, not a thin book being pushed)
3. **Directional filter**: Before taking a position, check if historical data shows this category of market tends to resolve Yes at the current price level. Skip if win rate < 70% for this price bucket.
4. **Signal emission**: If all filters pass, emit trade signal

### Tier 2: Medium-Horizon (2-4 weeks to resolution)

1. **Scan**: Filter markets where `close_time` is 14-28 days away AND `yes_price` is 0.85-0.90
2. **Thesis**: These markets are systematically underpriced due to opportunity cost — traders prefer shorter-term bets (Page & Clemen 2013)
3. **Filter**: volume_24h > $5,000, market has existed for > 7 days
4. **Signal emission**: If filters pass, emit with lower confidence

### Position Sizing

- Use modified Kelly criterion: `f* = (p * b - q) / b` where:
  - `p` = estimated true probability (from price + bias adjustment)
  - `q` = 1 - p
  - `b` = net odds after fees
- Apply Kelly fraction: use 0.25x Kelly (quarter Kelly) for safety
- **Hard cap**: Maximum 5% of bankroll per position
- **Minimum edge**: `(estimated_p - market_price) > 0.03` after Polymarket fee (~2%)

### Scan Frequency

- Run every 5 minutes for Tier 1 (< 7 days)
- Run every 30 minutes for Tier 2 (2-4 weeks)

## Error Handling

- **No markets match filters**: Log at INFO level, sleep until next scan cycle. This is normal — not every scan produces signals.
- **Metadata missing close_time**: Skip market, log at WARNING. Cannot evaluate without resolution date.
- **Price data stale (> 5 min old)**: Skip market, log at WARNING. Do not trade on stale prices.
- **Database connection failure**: Log at ERROR, retry once, then skip this scan cycle.

## Testing

- `--test` mode: Load fixture markets from `.tmp/fixtures/s1_markets.json`
- Backtest mode: `--backtest --start-date 2025-01-01 --end-date 2025-12-31` — run strategy on historical data from `market_prices` table, report win rate, PnL, Sharpe ratio
- Edge case tests: market at exactly 0.88, market with null close_time, market with 0 volume

## Edge Cases

- **Market resolution mid-scan**: If a market resolves while being evaluated, the price will jump to 1.00 or 0.00. Detect this (price change > 0.10 in < 1 minute) and skip.
- **Illiquid markets**: If order book depth < $100, do not trade. The spread will eat the edge.
- **Multiple markets on same event**: Avoid doubling exposure. Group by event slug/category and limit total exposure per event to 5% of bankroll.
- **Weekend/holiday markets**: Some markets resolve on specific dates that may not have active trading. Reduce position size by 50% if volume drops > 80% from 7-day average.

## Research Notes

- Page & Clemen (2013): "Do prediction markets produce well-calibrated probability forecasts?" — evidence for medium-horizon mispricing
- Polymarket Yes-bias: Documented in multiple research threads. Retail traders anchor on "will this happen?" and overbuy Yes. This is most pronounced in political and speculative markets, less so in sports/objective markets.
