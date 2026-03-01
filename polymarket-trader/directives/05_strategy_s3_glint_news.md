# Directive: Strategy S3 — Glint News Pipeline

## Goal

Parse structured alert messages from Glint's Telegram channel, compute Bayesian posteriors by combining Glint impact scores with current market prices, apply Satopaa extremizing calibration, and emit trade signals for markets where the extremized probability diverges meaningfully from the market price.

## Input

- `stream:news:signals` — Parsed Glint alert messages from `glint_telegram.py`
- `stream:prices:normalised` — Current market prices for matched contracts
- PostgreSQL `market_prices` — Historical prices for calibration

## Tools

- `execution/strategies/s3_glint_news.py` — Strategy logic
- `execution/connectors/glint_telegram.py` — Telegram feed parser
- `execution/pipeline/extremizer.py` — Satopaa logit extremizing calibration

## Output

- Trade signals → `stream:orders:pending`
- Signal format:
  ```json
  {
      "strategy": "s3",
      "venue": "polymarket",
      "market_id": "0x1234...",
      "side": "yes",
      "target_price": 0.68,
      "size_usd": 40.00,
      "edge_estimate": 0.07,
      "confidence": "medium",
      "reasoning": "Glint HIGH impact signal. Prior=0.62, Posterior=0.71, Extremized=0.75. Edge=0.07 after fees.",
      "glint_signal_type": "news_break",
      "glint_impact": "HIGH",
      "prior_price": 0.62,
      "posterior": 0.71,
      "extremized_p": 0.75
  }
  ```

## Glint Signal Schema

Each parsed Glint message must contain:

```json
{
    "timestamp": "2026-02-28T14:30:00Z",
    "signal_type": "news_break",
    "matched_contracts": ["0x1234...", "0x5678..."],
    "impact_level": "HIGH",
    "relevance_score": 0.85,
    "headline": "Fed announces emergency rate cut",
    "raw_text": "..."
}
```

### Signal Types
- `news_break` — Breaking news event
- `data_release` — Economic data (CPI, GDP, jobs)
- `political` — Political event or announcement
- `legal` — Court ruling, regulatory action
- `market_move` — Significant market movement in related assets

### Impact Levels
- `HIGH` — Likely to move matched markets > 5c
- `MEDIUM` — Likely to move 2-5c
- `LOW` — Informational, < 2c expected impact

## Bayesian Update Logic

1. **Prior**: Current market price `p_market` (from `stream:prices:normalised`)
2. **Likelihood ratio**: Derived from Glint impact level and relevance score:
   ```
   LR = base_lr[impact_level] * relevance_score

   base_lr = {
       "HIGH": 3.0,
       "MEDIUM": 1.5,
       "LOW": 1.1
   }
   ```
3. **Posterior (raw)**:
   ```
   posterior = (p_market * LR) / (p_market * LR + (1 - p_market))
   ```

## Extremizing Calibration (Satopaa et al. 2014)

Raw posteriors from Bayesian updates tend to be underconfident. Apply the extremizing transform:

```
extremized_p = logistic(alpha * logit(posterior))
```

Where:
- `logit(p) = ln(p / (1 - p))`
- `logistic(x) = 1 / (1 + exp(-x))`
- `alpha ≈ 1.73` (calibrated against resolved Polymarket markets)

### Alpha Calibration

- **Initial value**: 1.73 (from Satopaa et al. 2014 for prediction markets)
- **Recalibration**: After accumulating 100+ resolved trades with Glint signals, recalibrate alpha by minimizing Brier score on the out-of-sample set
- **Tool**: `execution/pipeline/extremizer.py` handles both the transform and recalibration

## Trade Signal Emission

1. Compute `edge = extremized_p - market_price`
2. Subtract estimated fees: Polymarket ~2%, Kalshi varies
3. **Minimum edge threshold**: `net_edge > 0.05` (5 cents after fees)
4. If edge passes threshold, compute position size using modified Kelly
5. Emit signal to `stream:orders:pending`

### Timing

- **Latency matters**: Glint signals are time-sensitive. Process within 30 seconds of receipt.
- **Decay**: Edge decays as market absorbs the news. If market price has already moved > 2c toward the posterior since the signal timestamp, reduce edge estimate by the movement.
- **Staleness**: Discard signals older than 10 minutes.

## Error Handling

- **Glint channel offline**: If no messages received for > 1 hour during market hours, log at WARNING. This is expected outside market hours.
- **Contract match failure**: If Glint references a contract ID not found in our market data, log at WARNING and skip. Do not create phantom markets.
- **Extremizer produces p > 0.99 or p < 0.01**: Cap at [0.01, 0.99]. Extreme probabilities outside this range are likely calibration errors.
- **Multiple signals for same market within 5 minutes**: Use only the most recent signal. Do not stack Bayesian updates — this double-counts evidence.

## Testing

- `--test` mode: Load fixture Glint signals from `.tmp/fixtures/s3_glint_signals.json`, compute posteriors against fixture market prices
- `--backtest` mode: Replay historical Glint signals against historical prices, report Brier score, calibration curve, PnL
- Extremizer unit test: Verify `extremize(0.5, 1.73) ≈ 0.5` (symmetric), `extremize(0.6, 1.73) ≈ 0.667`

## Edge Cases

- **Signal contradicts market**: If Glint says HIGH impact bullish but market is at 95c (already high), the posterior barely moves. This is correct behavior — don't override.
- **Multiple contracts matched**: If Glint matches multiple contracts, evaluate each independently. They may be related (same event, different outcomes) — do not take opposing positions.
- **Glint format change**: If parsing fails on > 20% of messages in a batch, halt processing and log at ERROR. The message format may have changed. Update `glint_telegram.py` parser.
- **Weekend signals**: Lower liquidity on weekends. Increase minimum edge threshold to 0.07 on Saturdays and Sundays.

## Research Notes

- Satopaa, Baron, Foster, Mellers, Tetlock, Ungar (2014): "Combining multiple probability predictions using a simple logit model" — source for extremizing calibration
- Alpha of 1.73 found optimal for aggregating forecasts from informational signals in prediction markets
- Brier score is the preferred calibration metric: `BS = (1/N) * Σ(forecast_i - outcome_i)²`
