# Directive: Strategy S5 — Cross-venue Direct Arbitrage (Polymarket ↔ Kalshi)

## Goal

Detect and exploit price discrepancies between Polymarket and Kalshi on the same event by matching markets semantically across venues, calculating net ROI after fees, and executing simultaneous opposing positions — while carefully managing oracle mismatch risk.

## Input

- `stream:prices:normalised` — Unified MarketPrice objects from both venues (with `matched_market_id` and `match_confidence` from market matcher)
- PostgreSQL `market_prices` — Historical cross-venue price data
- Market metadata from Gamma (Polymarket) and Kalshi REST APIs

## Tools

- `execution/strategies/s5_crossvenue_arb.py` — Strategy logic
- `execution/pipeline/market_matcher.py` — Cross-venue semantic matching

## Output

- Arb signals → `stream:arb:signals`
- Trade signals → `stream:orders:pending` (two legs: one per venue)
- Signal format:
  ```json
  {
      "strategy": "s5",
      "arb_type": "cross_venue",
      "leg_poly": {
          "venue": "polymarket",
          "market_id": "0x1234...",
          "side": "yes",
          "price": 0.62
      },
      "leg_kalshi": {
          "venue": "kalshi",
          "market_id": "TICKER-ABC",
          "side": "no",
          "price": 0.32
      },
      "gross_spread": 0.06,
      "net_roi": 0.032,
      "oracle_risk": "LOW",
      "size_usd": 100.00,
      "confidence": "high"
  }
  ```

## Strategy Logic

### Market Matching

1. Use `market_matcher.py` to find cross-venue pairs:
   - Embed titles from both venues using `all-MiniLM-L6-v2`
   - Pairs with `cosine > 0.80` = same event (high confidence)
   - Pairs with `cosine 0.70-0.80` = probable match (flag for manual review)
   - Below 0.70 = different events, skip
2. Persist matched pairs in PostgreSQL for reuse (don't re-match every cycle)
3. Re-run matching when new markets appear on either venue

### Spread Calculation

For a matched pair (Polymarket market P, Kalshi market K):

```
# If Polymarket Yes is cheaper → Buy Poly Yes + Buy Kalshi No
gross_spread = (1.0 - poly_yes) + (1.0 - kalshi_no) - 1.0
             = kalshi_yes - poly_yes  (simplified)

# Fees
poly_fee ≈ 0.02  (2% on winnings)
kalshi_fee = varies by market (check API)

net_roi = gross_spread - poly_fee - kalshi_fee
```

### Execution Criteria

1. **Minimum net ROI**: 2.5% (accounts for execution slippage and oracle risk)
2. **Match confidence**: cosine > 0.80 required for automated execution
3. **Oracle risk check**: MUST pass oracle risk filter (see below)
4. **Liquidity check**: Both legs must have order book depth > 2x intended position size
5. **Pre-positioned capital**: NEVER attempt cross-venue arb without funds already on both venues. Settlement is not instant.

### Oracle Mismatch Risk Management

**This is the highest-risk component of S5.**

Polymarket resolves via UMA oracle (decentralized token vote). Kalshi resolves via CFTC-regulated internal team. These can disagree.

**Known incident**: March 2025 — A whale manipulated a $7M UMA vote, causing Polymarket to resolve opposite to Kalshi on the same event. Arb positions got destroyed.

**Mitigation rules**:

1. **SKIP high-risk markets**: Flag and exclude any market whose title contains:
   - "deal", "agreement", "announcement" — subjective resolution criteria
   - "by end of", "before" — ambiguous timing boundaries
   - Any political negotiation or diplomatic event

2. **ONLY arb low-risk markets**: Markets with unambiguous, independently verifiable criteria:
   - Sports scores (NFL, NBA, MLB final scores)
   - Economic data releases (CPI, GDP, jobs report — published by government agencies)
   - Election results (certified by election authorities)
   - Measurable thresholds (stock price, temperature, specific numerical values)

3. **Oracle divergence watchlist**:
   - If Polymarket and Kalshi prices on a matched pair diverge by > 8c for > 30 minutes, quarantine the pair
   - This suggests potential resolution dispute brewing
   - Remove from active arb pool until prices converge or resolution occurs

4. **Maximum cross-venue position**: $500 per matched pair until oracle divergence rate is empirically measured over 50+ resolutions

### Position Sizing

- Position size = min(kelly_fraction * bankroll, $500, 0.5 * min_book_depth)
- Kelly fraction based on historical arb completion rate (start with 0.1x Kelly until 30+ arb completions)
- Both legs must be placed within 5 seconds of each other

### Scan Frequency

- Every 30 seconds for actively matched pairs
- Every 5 minutes for new market matching

## Error Handling

- **One leg fills, other doesn't**: This is the worst case. If only one leg executes:
  - Immediately attempt the second leg at market price (wider spread acceptable)
  - If second leg fails completely, you have a directional position — manage as a regular trade with a stop-loss
  - Log at CRITICAL level. This should be rare but devastating if unmanaged.
- **Kalshi token expired mid-arb**: If Kalshi returns 401, the order will fail. Refresh token and retry within 10 seconds. If still failing, cancel the Polymarket leg if possible.
- **Price moved during execution**: If the spread has shrunk below minimum ROI by the time the order is placed, abort both legs.
- **Venue downtime**: If either venue API is unreachable, suspend all S5 activity. Do not attempt one-legged arbs.

## Testing

- `--test` mode: Load fixture matched pairs with known spreads from `.tmp/fixtures/s5_arb_pairs.json`
- `--backtest` mode: Replay historical cross-venue prices, simulate arb execution with realistic latency (500ms per leg), report PnL and arb hit rate
- Oracle risk test: Verify that markets with "deal" in title are correctly flagged as HIGH risk

## Edge Cases

- **Different settlement times**: Polymarket may resolve days after Kalshi for the same event. Capital is locked during this period. Factor settlement delay into ROI calculation.
- **Kalshi US-only access**: All Kalshi API calls must originate from US IP. Non-US deployment requires VPN routing for Kalshi traffic only.
- **Fee changes**: Both venues may change fee structures. Parameterize fees in config, check API for current fee schedule on startup.
- **Market close timing**: Kalshi markets may close before Polymarket. If one venue closes trading before the other, exit the position on the still-open venue.
- **Currency mismatch**: Polymarket settles in USDC, Kalshi in USD. Exchange rate risk is negligible (USDC ≈ $1.00) but monitor for depegging events.

## Research Notes

- Cross-venue prediction market arbitrage literature is thin. Most work focuses on sports betting arbitrage.
- UMA oracle design: disputes require staking tokens, resolution by token holder vote. Susceptible to whale manipulation.
- CFTC-regulated resolution: Kalshi must follow regulatory procedures. More predictable but slower.
- Empirical oracle agreement rate: Unknown at project start. Track and update this directive with observed rate after first 50 matched resolutions.
