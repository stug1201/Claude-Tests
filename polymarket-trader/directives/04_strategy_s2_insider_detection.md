# Directive: Strategy S2 — Insider Wallet Detection

## Goal

Identify wallets exhibiting insider-like trading patterns on Polymarket, compute an Insider Probability Score (IPS) for each wallet, and piggyback on high-IPS wallet trades in real-time — adjusting position size based on whether the market has already absorbed the information.

## Input

- Goldsky GraphQL: Last 90 days of Polymarket order-filled events
- Polymarket Data API: Wallet positions, activity history
- Hashdive: Smart Scores as bootstrap labels for classifier training
- `stream:prices:normalised` — Real-time prices for timing analysis
- PostgreSQL `wallet_scores` — Persisted IPS scores

## Tool

`execution/strategies/s2_insider_detection.py`

## Supporting Tools

- `execution/connectors/goldsky_graphql.py` — Pulls trade history
- `execution/connectors/hashdive_scraper.py` — Pulls Smart Scores
- `execution/pipeline/wallet_profiler.py` — Computes IPS scores

## Output

- Updated `wallet_scores` table in PostgreSQL (hourly batch)
- Real-time alerts → `stream:insider:alerts`
- Trade signals → `stream:orders:pending`
- Signal format:
  ```json
  {
      "strategy": "s2",
      "venue": "polymarket",
      "market_id": "0x1234...",
      "side": "yes",
      "target_price": 0.55,
      "size_usd": 75.00,
      "edge_estimate": 0.08,
      "confidence": "high",
      "reasoning": "Wallet 0xABCD (IPS=0.87) bought Yes at $0.52. Price flat after 5min = info not incorporated.",
      "trigger_wallet": "0xABCD...",
      "trigger_ips": 0.87,
      "price_response": "flat"
  }
  ```

## IPS Scoring Model

### Features (per wallet, computed over 90-day window)

| Feature | Computation | Weight Hint |
|---------|------------|-------------|
| `win_rate_24h` | Win rate on markets that resolved within 24h of wallet's trade | High |
| `avg_entry_timing` | Average hours between wallet trade and major price move (>5c) | High |
| `cluster_id` | Wallets that trade the same market within 5 minutes of each other | Medium |
| `cluster_size` | Number of wallets in the cluster | Medium |
| `avg_position_size` | Average trade size in USD | Low |
| `market_diversity` | Number of distinct markets traded | Low |
| `hashdive_score` | Hashdive Smart Score (0-100) as external validation | Bootstrap |

### Training Pipeline

1. **Label generation**: Use Hashdive Smart Scores > 80 as positive labels, < 30 as negative labels
2. **Feature extraction**: Compute all features from Goldsky trade history
3. **Model**: XGBoost classifier (`xgboost.XGBClassifier`)
4. **Validation**: 5-fold cross-validation on time-ordered splits (no future leakage)
5. **Output**: Trained model saved to `.tmp/models/ips_model.json`
6. **Retraining**: Weekly, or when Hashdive labels change by > 10%

### Cluster Detection

1. Pull all trades from Goldsky for the last 90 days
2. Group by `(market_id, 5-minute time bucket)`
3. Within each group, identify wallets that appear together in > 3 distinct markets
4. Assign cluster IDs using connected-component analysis
5. Wallets in large clusters (> 5 members) with high win rates are strong insider signals

## Real-Time Trading Logic

When a high-IPS wallet (IPS > 0.70) places a trade:

1. **Detect**: Monitor `stream:insider:alerts` for new wallet activity
2. **Price response check**: Wait N minutes (configurable, default 5 min), then:
   - **Price moved > 3c in their direction**: Information partially incorporated. Reduce position size by 50%. The edge is shrinking.
   - **Price flat (< 1c movement)**: Information NOT yet propagated. Maximum conviction entry. The market hasn't reacted.
   - **Price moved against them**: Skip. Either the insider was wrong, or this is a false positive.
3. **Signal emission**: Emit trade signal with appropriate size and reasoning

## Error Handling

- **Goldsky rate limits**: Unknown at design time. Implement exponential backoff. If rate-limited during batch processing, resume from last processed wallet.
- **Hashdive scraping fails**: Degrade gracefully — train classifier without Hashdive labels (using win_rate_24h > 0.75 as proxy positive label). Log at WARNING.
- **Insufficient training data**: Need minimum 50 positive and 50 negative labels. If below threshold, log at ERROR and skip model retraining. Use previous model.
- **Wallet not in database**: New wallets with no history get IPS = 0.50 (uninformative prior). Only flag after 10+ trades.

## Testing

- `--test` mode: Load fixture wallet trades from `.tmp/fixtures/s2_wallets.json`, compute IPS scores without external API calls
- `--backtest` mode: Replay historical wallet activity, simulate trading on detected insiders, report PnL
- Model validation: `--validate` mode runs cross-validation and prints metrics (AUC, precision, recall, F1)

## Edge Cases

- **Wash trading**: Wallets trading with themselves to inflate volume. Detect by checking if the same wallet appears on both sides of a trade.
- **Sybil wallets**: One entity controlling many wallets. Cluster detection should catch this. Treat the cluster as a single entity for exposure limits.
- **Smart Score changes**: Hashdive scores are not static. If a wallet's score drops > 20 points, trigger immediate IPS recalculation.
- **Market already at 95c+**: Even if insider buys, the remaining edge is < 5c minus fees. Skip these.
- **Low liquidity reaction**: In thin markets, a single insider trade may move the price > 3c mechanically (not informationally). Use volume-weighted price impact to distinguish mechanical vs informational moves.

## Research Notes

- IPS methodology inspired by academic literature on informed trading detection (Easley & O'Hara PIN model, adapted for prediction markets)
- Cluster analysis: connected-component approach more robust than hierarchical clustering for wallet networks
- XGBoost chosen over neural nets for interpretability — feature importances help debug false positives
