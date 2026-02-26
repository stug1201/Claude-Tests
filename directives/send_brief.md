# Directive: Send Brief to Telegram

## Goal
Send the compiled daily brief to the designated Telegram output chat.

## Input
- Formatted brief string from `.tmp/brief.txt` (output of compile_brief.py)

## Tool
`execution/send_brief.py`

## Configuration
- Use `python-telegram-bot` library with `TELEGRAM_BOT_TOKEN` env var
- Send to chat ID specified in `TELEGRAM_OUTPUT_CHAT_ID` env var
- Parse mode: Markdown

## Message Splitting
Telegram has a 4096 character limit per message. If the brief exceeds this:
1. Split at paragraph boundaries (double newline) where possible
2. If a single paragraph exceeds 4096 chars, split at sentence boundaries
3. Send each part sequentially with a 0.5s delay between sends
4. Ensure no split breaks mid-markdown formatting (bold, italic)

## Cleanup
After successful send, delete the entire `.tmp/` directory and its contents. This keeps the environment clean for the next run.

## Output
Log confirmation of successful send, including:
- Number of messages sent
- Chat ID sent to
- Timestamp of send

## Edge Cases
- **Empty brief**: if `.tmp/brief.txt` is empty or missing, log an error and exit without sending.
- **Bot permissions**: if the bot lacks permission to post in the chat, log a clear error message indicating the bot needs to be added to the chat as an admin.
- **Network failure**: if the send fails, log the error. Do not retry automatically (the orchestrator handles retries if needed).

## Testing
Pass `--test` flag to print the brief to stdout instead of sending to Telegram.
