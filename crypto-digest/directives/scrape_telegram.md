# Directive: Scrape Telegram Channels

## Goal
Pull all messages from the last 24 hours from each source Telegram channel.

## Input
- List of channel usernames from `TELEGRAM_SOURCE_CHANNELS` env var (comma-separated)
- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_STRING` for Telethon auth

## Tool
`execution/scrape_telegram.py`

## Output
In-memory list of message objects written to `.tmp/scraped.json`, each with:
- `channel` — source channel username
- `timestamp` — message datetime (ISO 8601 UTC)
- `text` — message text content (may be empty for image-only messages)
- `media_type` — one of: `text`, `image`, `url`
- `url` — extracted URL if present, else null
- `image_path` — local path to downloaded image if present, else null

## Edge Cases
- **Channels with no new messages**: skip gracefully, log that zero messages were found, continue to next channel.
- **Stickers and GIFs**: skip entirely. Only process photo media types (standalone images, charts, screenshots). Check `message.photo` — ignore `message.sticker`, `message.gif`, `message.document` (unless document is a photo).
- **URL deduplication**: maintain a `seen_urls` set across all channels during the scrape phase. If a URL has already been seen, skip the duplicate message (or strip the duplicate URL from it).
- **Session string auth**: use `StringSession` from Telethon for headless operation — no interactive login prompts. This is critical for GitHub Actions.
- **Network errors**: if a channel fails to fetch, log the error and continue with remaining channels. Do not abort the entire scrape.
- **Rate limiting**: Telegram may rate-limit rapid channel reads. Add a 1-second pause between channel iterations.

## Testing
Pass `--test` flag to use a small fixture of fake messages instead of live Telegram data.
