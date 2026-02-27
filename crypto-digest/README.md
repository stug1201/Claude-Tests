# Crypto News Digest

A daily crypto market digest system that scrapes public Telegram channels, summarises content using Claude AI, and delivers a professional morning brief.

## How It Works

1. **Scrape** — Pulls the last 24 hours of messages from 4 configured Telegram channels
2. **Process** — Extracts and summarises articles, tweets, images, and text using Claude (model routed by content type)
3. **Compile** — Synthesises all summaries into a single professional brief using Claude Opus
4. **Send** — Delivers the brief to a Telegram output chat

Runs daily at 10:00 AM IST via GitHub Actions.

## Architecture

This project follows a 3-layer architecture (see `agents.md`):

- `directives/` — Markdown SOPs defining what each step does
- `execution/` — Deterministic Python scripts that do the work
- `.tmp/` — Ephemeral intermediate files (auto-cleaned after each run)

## Setup

### 1. Get Telegram API Credentials

1. Go to [my.telegram.org](https://my.telegram.org)
2. Create an application to get your `API_ID` and `API_HASH`

### 2. Create a Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Create a new bot with `/newbot`
3. Copy the bot token
4. Add the bot to your output channel/group as an admin

### 3. Generate a Telethon Session String

```bash
pip install telethon
python execution/generate_session.py
```

This will prompt for your API ID, API hash, and phone number. It outputs a session string that allows headless Telegram access without interactive login.

### 4. Configure Environment

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

Required variables:
| Variable | Description |
|---|---|
| `TELEGRAM_API_ID` | Telegram API ID from my.telegram.org |
| `TELEGRAM_API_HASH` | Telegram API hash |
| `TELEGRAM_SESSION_STRING` | Generated session string |
| `TELEGRAM_BOT_TOKEN` | Bot token from BotFather |
| `TELEGRAM_OUTPUT_CHAT_ID` | Chat ID of output channel |
| `TELEGRAM_SOURCE_CHANNELS` | Comma-separated channel usernames (without @) |
| `ANTHROPIC_API_KEY` | Anthropic API key |

### 5. Install Dependencies

```bash
pip install -r requirements.txt
```

### 6. Test Locally

Run in test mode (uses fixture data, no live API calls):

```bash
python execution/run_digest.py --test
```

Run individual scripts in test mode:

```bash
python execution/scrape_telegram.py --test
python execution/process_content.py --test
python execution/compile_brief.py --test
python execution/send_brief.py --test
```

Run the full pipeline with live data:

```bash
python execution/run_digest.py
```

### 7. Deploy to GitHub Actions

Add these secrets to your GitHub repository (Settings > Secrets and variables > Actions):

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_SESSION_STRING`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_OUTPUT_CHAT_ID`
- `TELEGRAM_SOURCE_CHANNELS`
- `ANTHROPIC_API_KEY`

The workflow runs automatically at 10:00 AM IST (04:30 UTC) daily. You can also trigger it manually from the Actions tab.

## Model Routing

| Content Type | Model |
|---|---|
| Long articles (>1000 words) | claude-opus-4-5 |
| Short articles (<=1000 words) | claude-sonnet-4-6 |
| Tweets | claude-sonnet-4-6 (via nitter scraping) |
| Images | claude-sonnet-4-6 (vision) |
| Plain text | claude-sonnet-4-6 |
| Brief compilation | claude-opus-4-6 |

## File Structure

```
.
├── agents.md                          # 3-layer architecture docs
├── directives/
│   ├── scrape_telegram.md             # Scraping SOP
│   ├── process_content.md             # Content processing SOP
│   ├── compile_brief.md               # Brief compilation SOP
│   └── send_brief.md                  # Sending SOP
├── execution/
│   ├── scrape_telegram.py             # Telegram channel scraper
│   ├── process_content.py             # Content processor & summariser
│   ├── compile_brief.py               # Brief compiler
│   ├── send_brief.py                  # Telegram sender
│   ├── run_digest.py                  # Main orchestrator
│   └── generate_session.py            # One-time session string helper
├── .github/workflows/
│   └── daily_digest.yml               # GitHub Actions cron workflow
├── requirements.txt
├── .env.example
└── .gitignore
```
