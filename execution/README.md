# TWAP Detection System

Real-time detection of Time-Weighted Average Price (TWAP) algorithmic orders on Binance Perpetual Futures using Fourier Transform analysis.

## Table of Contents

1. [Overview](#overview)
2. [How TWAP Detection Works](#how-twap-detection-works)
3. [System Architecture](#system-architecture)
4. [Running Locally](#running-locally)
5. [Telegram Alerts Setup](#telegram-alerts-setup)
6. [AWS Deployment](#aws-deployment)
7. [Admin Commands](#admin-commands)
8. [Configuration](#configuration)
9. [Troubleshooting](#troubleshooting)

---

## Overview

TWAP (Time-Weighted Average Price) is an algorithmic execution strategy that splits large orders into smaller chunks executed at regular time intervals. This creates a **periodic pattern** in trade flow that can be detected using signal processing techniques.

### What This System Does

- Connects to Binance Perpetual Futures WebSocket feeds
- Collects real-time trade data
- Analyzes trade flow using Fast Fourier Transform (FFT)
- Detects periodic execution patterns indicative of TWAP orders
- Classifies detections by size, urgency, and confidence
- Optionally sends alerts to Telegram

---

## How TWAP Detection Works

### The Mathematics

#### 1. Data Collection & Aggregation

Raw trades are collected and aggregated into time buckets (default: 1 second):

```
trades[t] → bucket[t] = {buy_volume, sell_volume}
```

For example, 10 minutes of data produces 600 buckets.

#### 2. Signal Extraction

We extract separate signals for buy and sell activity:

```
buy_signal[t]  = total buy volume in bucket t
sell_signal[t] = total sell volume in bucket t
```

#### 3. Windowing (Hann Window)

Before FFT, we apply a Hann window to reduce **spectral leakage**:

```python
window[n] = 0.5 * (1 - cos(2π * n / N))
windowed_signal = signal * window
```

Spectral leakage occurs when a signal doesn't perfectly fit the FFT window, causing energy to "leak" into adjacent frequency bins. The Hann window tapers the signal at edges to minimize this.

#### 4. Fast Fourier Transform (FFT)

The FFT converts the time-domain signal to frequency-domain:

```python
fft_result = numpy.fft.rfft(windowed_signal)
power_spectrum = |fft_result|²
frequencies = numpy.fft.rfftfreq(N, d=1/sample_rate)
```

The power spectrum shows how much "energy" exists at each frequency. A TWAP executing every 30 seconds creates a spike at **f = 1/30 = 0.033 Hz**.

#### 5. Peak Detection

We find peaks in the power spectrum that:
- Are above the noise floor (median power)
- Have Signal-to-Noise Ratio (SNR) above threshold
- Correspond to reasonable TWAP intervals (5s - 300s)

```python
SNR = (peak_power - noise_floor) / noise_std
```

#### 6. Confidence Calculation

Confidence is determined by:
- **SNR**: Higher = more distinct from noise
- **Cycles**: More cycles observed = more reliable

| Confidence | SNR | Cycles |
|------------|-----|--------|
| HIGH | ≥ 4.5 | ≥ 10 |
| MEDIUM | ≥ 3.5 | ≥ 6 |
| LOW | < 3.5 | < 6 |

### Why This Works

TWAP orders have three detectable characteristics:

1. **Periodicity**: Fixed time intervals → frequency spike in FFT
2. **Consistency**: Similar order sizes → stable amplitude
3. **Persistence**: Runs for extended periods → multiple observable cycles

Random market noise is aperiodic and creates a relatively flat power spectrum. TWAP orders create distinct peaks that stand out.

### Limitations

- **Harmonics**: A 30s TWAP also shows peaks at 15s, 10s, 7.5s (harmonics)
- **Noise**: High-volume markets may obscure smaller TWAPs
- **Overlap**: Multiple TWAPs can interfere with each other
- **Jitter**: Execution timing variance reduces signal clarity

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TWAP Detection System                        │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐     ┌──────────────────────┐
│  twap_data_collector │     │   Binance WebSocket  │
│  ──────────────────  │◄────│  (Perpetual Futures) │
│  - TradeCollector    │     └──────────────────────┘
│  - TradeBuffer       │
│  - Trade dataclass   │
└──────────┬───────────┘
           │ trades[]
           ▼
┌──────────────────────┐
│ twap_fourier_analyzer│
│ ──────────────────── │
│ - Bucket aggregation │
│ - Hann windowing     │
│ - FFT computation    │
│ - Peak detection     │
│ - SNR calculation    │
└──────────┬───────────┘
           │ TWAPDetection[]
           ▼
┌──────────────────────┐
│   twap_classifier    │
│   ────────────────   │
│ - Size categories    │
│ - Urgency levels     │
│ - Risk scoring       │
│ - Description gen    │
└──────────┬───────────┘
           │ ClassifiedTWAP[]
           ▼
┌──────────────────────┐     ┌──────────────────────┐
│   twap_detector.py   │     │ twap_telegram_alerts │
│   (CLI Interface)    │     │ (Cloud Service)      │
│   ────────────────   │     │ ──────────────────── │
│ - Interactive menus  │     │ - Multi-ticker       │
│ - Single ticker      │     │ - Telegram bot       │
│ - Local use          │     │ - Admin commands     │
└──────────────────────┘     └──────────────────────┘
```

### File Descriptions

| File | Purpose |
|------|---------|
| `twap_data_collector.py` | WebSocket connection, trade buffering |
| `twap_fourier_analyzer.py` | FFT analysis, peak detection, TWAP detection logic |
| `twap_classifier.py` | Classification by size/urgency, risk scoring, descriptions |
| `twap_detector.py` | Interactive CLI for single-ticker local monitoring |
| `twap_telegram_alerts.py` | Multi-ticker cloud service with Telegram integration |
| `test_twap_synthetic.py` | Test suite using synthetic data |

---

## Running Locally

### Prerequisites

- Python 3.9+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USER/Claude-Tests.git
cd Claude-Tests

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Run Interactive Detector

```bash
python execution/twap_detector.py
```

You'll see a menu to select a Binance Perpetual trading pair.

The detector will:
- Collect trade data (needs ~2 min buffer before first analysis)
- Run FFT analysis every 30 seconds
- Display detected TWAPs with names (e.g., BTC-B1, ETH-S2)
- Show updates when existing TWAPs are re-detected

Press **Ctrl+C** to stop and see session summary.

### Run Tests

```bash
python execution/test_twap_synthetic.py
```

This verifies all components work correctly using synthetic data with a known TWAP pattern.

---

## Telegram Alerts Setup

### 1. Create Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot`
3. Choose a name and username
4. Copy the **bot token** (looks like `1234567890:ABCdefGHI...`)

### 2. Create Alerts Channel

1. Create a new Telegram channel (can be private)
2. Add your bot as an administrator
3. Send any message to the channel
4. Visit: `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
5. Find the `"chat":{"id":-100...}` - this is your **channel ID**

### 3. Get Your Admin ID

1. Message [@userinfobot](https://t.me/userinfobot) on Telegram
2. It will reply with your **user ID** (a number like `123456789`)

### 4. Configure

Create `config.json` in the execution folder:

```json
{
  "telegram_bot_token": "YOUR_BOT_TOKEN",
  "telegram_channel_id": "-100YOUR_CHANNEL_ID",
  "telegram_admin_id": YOUR_USER_ID,
  "tickers": [
    {"symbol": "BTCUSDT", "enabled": true},
    {"symbol": "ETHUSDT", "enabled": true}
  ],
  "min_confidence": "LOW",
  "alert_on_updates": false
}
```

### 5. Run

```bash
python execution/twap_telegram_alerts.py
```

---

## AWS Deployment

### Recommended: EC2 Free Tier

**Instance**: t2.micro (750 hours/month free for 12 months)
**OS**: Amazon Linux 2023 or Ubuntu 22.04
**Cost**: $0 (within free tier)

### Setup Steps

1. **Launch EC2 Instance**
   - Go to AWS Console → EC2 → Launch Instance
   - Select Amazon Linux 2023 or Ubuntu 22.04
   - Instance type: t2.micro
   - Create or select a key pair
   - Security group: Allow SSH (port 22)
   - Launch

2. **Connect via SSH**
   ```bash
   ssh -i your-key.pem ec2-user@YOUR_EC2_IP
   # For Ubuntu: ssh -i your-key.pem ubuntu@YOUR_EC2_IP
   ```

3. **Clone and Setup**
   ```bash
   git clone https://github.com/YOUR_USER/Claude-Tests.git
   cd Claude-Tests
   chmod +x deploy/setup.sh
   ./deploy/setup.sh
   ```

4. **Configure**
   ```bash
   nano /opt/twap-alerts/config.json
   # Add your Telegram credentials and tickers
   ```

5. **Start Service**
   ```bash
   sudo systemctl start twap-alerts
   sudo systemctl status twap-alerts
   ```

### Service Management

```bash
# Start/stop/restart
sudo systemctl start twap-alerts
sudo systemctl stop twap-alerts
sudo systemctl restart twap-alerts

# View status
sudo systemctl status twap-alerts

# View logs
tail -f /var/log/twap-alerts.log

# Edit config (restart required after)
nano /opt/twap-alerts/config.json
sudo systemctl restart twap-alerts
```

---

## Admin Commands

Send these commands to your bot via **direct message** (not in the channel):

### Monitoring Commands

| Command | Description |
|---------|-------------|
| `/help` | Show all commands |
| `/status` | Show monitoring status and active TWAPs |
| `/pause` | Pause all monitoring |
| `/resume` | Resume monitoring |

### Ticker Management

| Command | Description |
|---------|-------------|
| `/list` | List all monitored tickers with thresholds |
| `/add SYMBOL` | Add ticker (e.g., `/add LINKUSDT`) |
| `/remove SYMBOL` | Remove ticker |

### Threshold Commands

| Command | Description |
|---------|-------------|
| `/config` | Show current configuration |
| `/setmajor VALUE` | Set BTC/ETH/SOL min USD (e.g., `/setmajor 80000`) |
| `/setother VALUE` | Set other tickers min USD (e.g., `/setother 40000`) |
| `/setconf LEVEL` | Set min confidence: LOW, MEDIUM, HIGH |

### Examples

```
/add LINKUSDT
/remove ADAUSDT
/status
/setmajor 100000
/setother 50000
```

**Note**: Only messages from the configured `telegram_admin_id` are processed. Service start/stop is only via systemctl.

---

## Configuration

### config.json Reference

| Field | Type | Description |
|-------|------|-------------|
| `telegram_bot_token` | string | Bot token from @BotFather |
| `telegram_channel_id` | string | Channel ID (usually starts with -100) |
| `telegram_admin_id` | integer | Your Telegram user ID |
| `tickers` | array | List of tickers to monitor |
| `analysis_interval_sec` | integer | How often to run FFT analysis (default: 30) |
| `min_buffer_sec` | integer | Minimum data before first analysis (default: 120) |
| `buffer_minutes` | integer | Trade buffer size (default: 30) |
| `min_confidence` | string | Minimum confidence to alert: LOW, MEDIUM, HIGH |
| `min_value_major` | integer | Min USD for BTC/ETH/SOL (default: 80000) |
| `min_value_other` | integer | Min USD for other tickers (default: 40000) |
| `alert_on_updates` | boolean | Alert when existing TWAP is re-detected |

### Threshold Logic

| Ticker Type | Default Threshold | Change Command |
|-------------|-------------------|----------------|
| BTC, ETH, SOL | $80,000 | `/setmajor VALUE` |
| All others | $40,000 | `/setother VALUE` |

### Ticker Configuration

```json
{
  "symbol": "BTCUSDT",
  "enabled": true
}
```

### Available Pairs

All Binance Perpetual Futures pairs are supported. Common examples:
- BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT
- DOGEUSDT, ADAUSDT, AVAXUSDT, DOTUSDT, MATICUSDT

---

## Troubleshooting

### "Connection refused" or "403" errors

Binance blocks connections from some cloud provider IPs. Solutions:
- Try a different AWS region
- Use a residential VPN/proxy (advanced)

### No TWAPs detected

- Wait for buffer to fill (2+ minutes minimum)
- TWAPs aren't always present - this is normal
- Lower `min_confidence` to `"LOW"`
- Check a high-volume pair like BTCUSDT

### Telegram bot not responding to commands

- Ensure you're messaging the bot directly (DM), not in the channel
- Verify your `telegram_admin_id` matches your actual Telegram user ID
- Check logs: `tail -f /var/log/twap-alerts.log`

### High CPU usage

- Reduce number of monitored tickers
- Increase `analysis_interval_sec` (e.g., 60)
- t2.micro should handle 10 tickers comfortably

### Service keeps restarting

Check logs for the actual error:
```bash
sudo journalctl -u twap-alerts -n 50
tail -f /var/log/twap-alerts.log
```

---

## Understanding the Output

### Detection Alert Example

```
🎯 NEW TWAP: BTC-S1

Ticker:        BTCUSDT
Side:          SELL
Category:      Large (Normal)
Interval:      30.2s
Per-exec:      0.450000 (~$19,350)
Est. Total:    ~$465,000
Confidence:    Medium (SNR: 4.8)
Risk:          52/100

A major fund is steadily selling, executing ~$19,350 every 30 seconds.
Signal is moderate (SNR: 4.8); pattern is likely real but monitor for
confirmation. Risk score 52/100 indicates moderate market influence.
```

### TWAP Naming Convention

Names follow the format: `TICKER-DIRECTION#`
- **TICKER**: 3-4 letter code (BTC, ETH, SOL, etc.)
- **DIRECTION**: B = Buy, S = Sell
- **#**: Order of appearance for that ticker/direction

Examples:
- `BTC-B1` = First BTC buy TWAP detected
- `ETH-S2` = Second ETH sell TWAP detected
- `SOL-B1` = First SOL buy TWAP detected

Each ticker has independent numbering, so BTC and ETH counters don't interfere.

### Field Explanations

| Field | Meaning |
|-------|---------|
| **Name** | Unique identifier for tracking (e.g., BTC-S1, ETH-B2) |
| **Side** | BUY (accumulation) or SELL (distribution) |
| **Category** | Size (Small/Medium/Large/Whale) + Urgency (Aggressive/Normal/Passive) |
| **Interval** | Time between executions (detected from FFT frequency) |
| **Per-exec** | Estimated size of each execution |
| **Est. Total** | Estimated total order value (extrapolated from amplitude and duration) |
| **Confidence** | Detection reliability based on SNR and observed cycles |
| **Risk** | Market impact score (0-100) based on size, urgency, and confidence |

### Size Categories

| Category | Estimated Total Value |
|----------|----------------------|
| Small | < $50,000 |
| Medium | $50,000 - $500,000 |
| Large | $500,000 - $5,000,000 |
| Whale | > $5,000,000 |

### Urgency Categories

| Category | Execution Interval |
|----------|-------------------|
| Aggressive | < 10 seconds |
| Normal | 10 - 60 seconds |
| Passive | > 60 seconds |

---

## AWS Maintenance

### After Code Updates

When code changes are pushed to the repository, follow these steps on your EC2 instance:

```bash
# 1. SSH into your instance
ssh -i your-key.pem ec2-user@YOUR_EC2_IP

# 2. Navigate to the project directory
cd ~/Claude-Tests

# 3. Pull the latest changes
git pull origin claude/instantiate-agents-from-docs-OQfoA

# 4. Restart the service to apply changes
sudo systemctl restart twap-alerts

# 5. Verify it's running
sudo systemctl status twap-alerts

# 6. Watch the logs for any errors
sudo journalctl -u twap-alerts -f
```

### Quick Restart Commands

```bash
# Restart service
sudo systemctl restart twap-alerts

# View status
sudo systemctl status twap-alerts

# View live logs
sudo journalctl -u twap-alerts -f
```

### If Something Goes Wrong

```bash
# Stop the service
sudo systemctl stop twap-alerts

# Check recent logs for errors
sudo journalctl -u twap-alerts -n 100

# Test manually to see error output
cd ~/Claude-Tests/execution
source ../venv/bin/activate
python twap_telegram_alerts.py

# Once fixed, start the service again
sudo systemctl start twap-alerts
```

### Updating Config via Telegram

You can change settings without SSH using Telegram commands:
- `/setsize MEDIUM` - Change minimum TWAP size filter
- `/setconf LOW` - Change minimum confidence filter
- `/add SYMBOL` - Add new ticker
- `/remove SYMBOL` - Remove ticker

Config changes via Telegram are saved automatically and persist across restarts.
