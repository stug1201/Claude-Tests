# TWAP Order Detection

## Goal
Detect Time-Weighted Average Price (TWAP) algorithmic orders on cryptocurrency exchanges by analyzing trade flow patterns using Fourier Transform analysis.

## Background

### What is a TWAP?
TWAP orders split a large order into smaller chunks executed at regular time intervals. For example, selling 100 BTC over 2 hours might execute 0.83 BTC every minute. This creates a **periodic signal** in trade data.

### Detection Method: Fourier Transform
The FFT converts time-domain trade data into frequency-domain, revealing periodic patterns:
- Regular interval execution → frequency spike at 1/interval Hz
- Consistent order sizes → amplitude indicates size
- Multiple TWAPs → multiple frequency peaks

## Supported Markets

### Exchange
- **Binance**: USDT-M Perpetual Futures only

### Data Source
| Exchange | Market Type | WebSocket Endpoint | Trade Channel |
|----------|-------------|-------------------|---------------|
| Binance | Perpetual | wss://fstream.binance.com/ws | `<symbol>@aggTrade` |

## Data Requirements

### Collection
- **Granularity**: Individual trades (not candles)
- **Fields**: timestamp (ms), price, size, side (buy/sell)
- **Storage**: In-memory rolling buffer + optional disk persistence

### Lookback Period
- **Minimum**: 15 minutes (detects TWAPs with intervals up to ~90 seconds)
- **Recommended**: 30-60 minutes (detects longer-interval TWAPs)
- **Maximum useful**: 4 hours (beyond this, market conditions change too much)

### Sampling for FFT
- Aggregate trades into time buckets (e.g., 1-second bins)
- Separate buy and sell volumes
- FFT requires evenly-spaced samples

## Detection Algorithm

### Step 1: Data Preparation
```
1. Collect trades for lookback period
2. Bucket into 1-second intervals
3. Calculate: buy_volume[t], sell_volume[t], net_flow[t]
4. Apply window function (Hann) to reduce spectral leakage
```

### Step 2: FFT Analysis
```
1. Compute FFT on buy_volume, sell_volume, and net_flow separately
2. Calculate power spectrum: |FFT|^2
3. Identify peaks above noise floor (e.g., 3 standard deviations)
4. Convert peak frequencies to intervals: interval = 1/frequency
```

### Step 3: TWAP Validation
A valid TWAP detection requires:
- Frequency peak significantly above noise (SNR > 3)
- Interval makes sense (5 seconds to 5 minutes typical)
- Consistent amplitude over time (not just a few coincidental trades)
- Pattern persists across multiple analysis windows

### Step 4: Size Estimation
- **Per-execution size**: Amplitude of frequency component
- **Total size estimate**: (amplitude) × (duration / interval)
- Confidence decreases for longer extrapolations

## Classification

### By Estimated Total Size
| Category | Size Range | Description |
|----------|-----------|-------------|
| Small | < $50K | Retail or small fund |
| Medium | $50K - $500K | Institutional |
| Large | $500K - $5M | Major fund |
| Whale | > $5M | Market-moving |

### By Execution Interval
| Category | Interval | Urgency |
|----------|----------|---------|
| Aggressive | < 10s | High urgency, willing to impact |
| Normal | 10-60s | Balanced execution |
| Passive | > 60s | Low urgency, minimizing impact |

### By Side
- **Buy TWAP**: Accumulation pattern
- **Sell TWAP**: Distribution pattern

## Outputs

### Real-time Display
```
[2024-01-15 14:32:15] TWAP DETECTED on BTCUSDT (Binance Perp)
  Side: SELL
  Interval: 30.2 seconds (Normal)
  Est. per-execution: 0.45 BTC (~$19,350)
  Running for: ~12 minutes
  Est. total so far: ~$465K (Medium)
  Confidence: HIGH (SNR: 8.2)
```

### Summary Report
After analysis completes, categorize all detected TWAPs and display summary.

## Edge Cases

### False Positives
- Market maker rebate farming (very small, very fast)
- Coincidental retail patterns
- News-driven synchronized trading

### Mitigation
- Minimum size threshold
- Persistence requirement (pattern must last > 5 minutes)
- Cross-validate with orderbook if available

## Scripts

| Script | Purpose |
|--------|---------|
| `execution/twap_data_collector.py` | WebSocket connections, trade buffering |
| `execution/twap_fourier_analyzer.py` | FFT analysis, peak detection |
| `execution/twap_classifier.py` | Categorization logic |
| `execution/twap_detector.py` | Local CLI interface |
| `execution/twap_telegram_alerts.py` | Multi-ticker cloud service with Telegram |

## Usage

```bash
# Run the local detector
python execution/twap_detector.py

# Select token from menu
# Monitor for TWAPs in real-time

# Run the Telegram alerts service
python execution/twap_telegram_alerts.py
```

## Environment Variables

```
# Optional - for authenticated endpoints (higher rate limits)
BINANCE_API_KEY=
BINANCE_API_SECRET=
```

## Learnings & Updates

<!-- Add notes here as you discover edge cases, API limits, or improvements -->
