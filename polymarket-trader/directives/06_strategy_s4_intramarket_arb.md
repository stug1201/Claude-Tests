# Directive: Strategy S4 — Intra-venue Combinatorial Arbitrage

## Goal

Detect arbitrage opportunities within Polymarket by (1) finding Yes+No price mismatches on individual contracts, and (2) finding logical contradictions between semantically related markets using sentence-transformer embeddings.

## Input

- `stream:prices:normalised` — Unified MarketPrice objects (filtered to venue=polymarket)
- PostgreSQL `market_prices` — Historical prices and market metadata
- Polymarket Gamma API — Market titles, categories, and resolution criteria

## Tools

- `execution/strategies/s4_intramarket_arb.py` — Strategy logic
- `execution/pipeline/market_matcher.py` — Sentence-transformer semantic matching

## Output

- Arb signals → `stream:arb:signals`
- Trade signals → `stream:orders:pending`
- Signal format:
  ```json
  {
      "strategy": "s4",
      "venue": "polymarket",
      "arb_type": "logical_contradiction",
      "leg_a": {
          "market_id": "0x1234...",
          "title": "Trump wins 2024",
          "side": "yes",
          "price": 0.62
      },
      "leg_b": {
          "market_id": "0x5678...",
          "title": "Republican wins 2024",
          "side": "yes",
          "price": 0.58
      },
      "contradiction": "A implies B, but P(A) > P(B)",
      "edge_estimate": 0.04,
      "size_usd": 30.00,
      "confidence": "high"
  }
  ```

## Strategy Logic

### Type 1: Simple Yes+No Mismatch

1. **Scan**: Every 60 seconds, check all active markets
2. **Test**: `yes_price + no_price` should equal ~1.00 (minus Polymarket fee of ~2%)
3. **Threshold**: If `|yes_price + no_price - 1.00| > 0.03` (3c beyond fee), it's an opportunity
4. **Trade**: Buy the underpriced side (whichever makes yes+no sum closer to 1.00)
5. **Size**: Limited by order book depth — never exceed 50% of visible depth on the cheaper side

### Type 2: Logical Contradiction Detection

This is the more valuable and complex arbitrage type.

1. **Semantic matching**:
   - Embed all active market titles using sentence-transformers (`all-MiniLM-L6-v2`)
   - Compute pairwise cosine similarity
   - Filter pairs with similarity > 0.60 (potential relations)
   - Cache embeddings — only recompute for new markets

2. **Logical implication analysis**:
   For each semantically related pair (A, B), check for logical violations:
   - **A implies B**: If event A happening necessarily means event B happens, then `P(A) <= P(B)` must hold. Violation: `P(A) > P(B)`
     - Example: "Trump wins 2024" (62c) implies "Republican wins 2024" (58c) — contradiction
   - **A and B mutually exclusive**: If A and B cannot both be true, then `P(A) + P(B) <= 1.00`
     - Example: "Biden wins" (40c) + "Trump wins" (62c) = 1.02 — marginal violation
   - **A is subset of B**: Same as implication. P(subset) <= P(superset)

3. **Implication detection heuristics**:
   - Parse market titles for entity relationships (named entities, verbs, outcomes)
   - Use keyword rules: "X wins [election]" implies "[X's party] wins [election]"
   - Use category metadata from Gamma API to group related markets
   - Flag pairs for manual review if semantic similarity > 0.80 but no clear logical relationship

4. **Edge calculation**: The contradiction magnitude = `P(A) - P(B)` for implication violations
5. **Minimum edge**: 3c after fees (Polymarket ~2% per side)
6. **Position sizing**: 50% of minimum order book depth across both legs

### Scan Frequency

- Type 1: Every 60 seconds (cheap computation)
- Type 2: Every 5 minutes (embedding computation is heavier)
- Re-embed new markets: On first appearance in normalised stream

## Market Matching Details

The `market_matcher.py` pipeline component handles semantic matching:

1. **Model**: `sentence-transformers/all-MiniLM-L6-v2` (384-dim embeddings, ~22M params, fast)
2. **Embedding cache**: Store in Redis hash `cache:market_embeddings` keyed by market_id
3. **Similarity index**: Use FAISS for fast approximate nearest-neighbor search when market count > 1000
4. **Threshold tuning**:
   - `cosine > 0.80`: Almost certainly the same event — flag for cross-venue arb (S5) too
   - `cosine 0.60-0.80`: Potentially related — check for logical implications
   - `cosine < 0.60`: Unrelated — skip

## Error Handling

- **Embedding model load failure**: Log at ERROR. Cannot run Type 2 without the model. Fall back to Type 1 only.
- **Order book depth unavailable**: If CLOB orderbook fetch fails, use conservative fixed size ($20 per leg).
- **Stale prices**: If price data is > 2 minutes old for either leg, skip the pair. Arb opportunities are time-sensitive.
- **Self-referential match**: Filter out pairs where market_id_A == market_id_B (same market matched to itself).

## Testing

- `--test` mode: Load fixture markets with known contradictions from `.tmp/fixtures/s4_markets.json`
- Type 1 test: Create a market with yes=0.55, no=0.48 → should detect arb of 0.03
- Type 2 test: Create "X wins election" at 0.62 and "X's party wins election" at 0.58 → should detect implication violation
- Embedding test: Verify that "Will Bitcoin reach $100k?" and "BTC to hit $100,000" have cosine > 0.80

## Edge Cases

- **Multi-outcome markets**: Polymarket has markets with multiple outcomes (not just Yes/No). All outcome prices should sum to ~1.00. Detect violations in N-outcome markets.
- **Time-zone confusion**: Some markets reference dates in specific time zones. "Before midnight ET" vs "before midnight UTC" — these are different markets even if titles are similar.
- **Resolution ambiguity**: If two markets are semantically similar but have different resolution sources or criteria, the logical implication may not hold. Check resolution details from Gamma metadata.
- **Flash arbs**: Some arbs last < 10 seconds (fat-finger trades, bot rebalancing). By the time we detect and execute, they may be gone. Track hit rate and discard arb types with < 30% execution success.
- **Book depth changes**: Between detection and execution, book depth may thin. Always re-check depth before placing orders.

## Research Notes

- Saguillo et al. (2025): Methodology for sizing arb positions relative to order book depth
- Sentence-transformers: Reimers & Gurevych (2019), `all-MiniLM-L6-v2` offers best speed/quality tradeoff for this use case
- FAISS: Facebook AI Similarity Search — necessary when market count exceeds ~1000 for real-time matching
