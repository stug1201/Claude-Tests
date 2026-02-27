# Directive: Compile Daily Brief

## Goal
Compile all summaries into a single daily brief suitable for Telegram delivery.

## Input
- List of summary objects from `.tmp/summaries.json` (output of process_content.py)

## Tool
`execution/compile_brief.py`

## Model
`claude-opus-4-6` — hardcoded, always use the latest Opus model for this step. The brief is the final deliverable and requires the highest-quality synthesis.

## Brief Format Instructions (passed to the model)

### Tone
Professional financial analytics — precise, neutral, and authoritative. Write as though this is an internal morning brief distributed at a macro hedge fund. No casual language, no hype, no speculative framing beyond what the source material explicitly supports. Treat price movements, protocol events, and regulatory developments with the same measured register you would apply to equities or fixed income commentary.

### Formatting Rules
- **No emojis.** None. Not in headers, not inline, not as bullets.
- **No exclamation marks.**
- Use Telegram-compatible Markdown: bold with `*`, italic with `_` for headers and key terms only. Do not over-format.

### Structure
1. **Market Overview** (opening): 3-4 sentence paragraph synthesising the dominant themes of the past 24 hours at a high level, written in prose.
2. **Key Developments**: Flag 1-2 highest signal items at the top — items with meaningful market implications, not just interesting news.
3. **Thematic Sections** (use only those that have material):
   - Macro & Risk
   - Digital Assets
   - DeFi & Protocols
   - Regulation & Policy
   - Notable Reads
4. Each item: 2-3 sentences, factual and direct, followed by source link on a new line: `Source: [url]`
5. Numbers and percentages stated precisely where available (write "BTC declined 4.2% over the session" not "Bitcoin saw a notable drop").
6. Omit any section for which there is no material that day.

### Length
The brief should be digestible in under 3 minutes. Density over length.

## Output
Formatted brief string written to `.tmp/brief.txt`, ready to send to Telegram.

## Edge Cases
- If summaries list is empty, generate a brief noting that no significant content was found in the monitored channels over the last 24 hours.
- If the model response is somehow empty or malformed, log the error and write a fallback message to brief.txt.

## Testing
Pass `--test` flag to use fixture summary data instead of reading from .tmp/summaries.json.
