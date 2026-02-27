# Directive: Process and Summarise Content

## Goal
Summarise each piece of scraped content using the appropriate Claude model based on content type and length.

## Input
- List of message objects from `.tmp/scraped.json` (output of scrape_telegram.py)

## Tool
`execution/process_content.py`

## Model Routing Logic (hardcoded, not configurable)

| Content Type | Condition | Model |
|---|---|---|
| Long article | URL fetched, clean text > 1000 words | `claude-opus-4-5` |
| Short article | URL fetched, clean text <= 1000 words | `claude-sonnet-4-6` |
| Tweet URL | URL contains `x.com/`, `twitter.com/`, or `t.co/` | Attempt nitter scrape first, then `claude-sonnet-4-6` |
| Image | Photo media from Telegram | `claude-sonnet-4-6` with vision (base64 input) |
| Plain text | Telegram message text with no URL or image | `claude-sonnet-4-6` |
| Unfetchable URL | Paywalled, blocked, or network error on fetch | `claude-sonnet-4-6` on preview text only |

### Word Counting
Always count words on trafilatura clean-extracted text, **never** raw HTML. Use `len(clean_text.split())` after `trafilatura.extract()`.

### Article Extraction
Use `trafilatura.fetch_url()` + `trafilatura.extract()` for all non-tweet URLs. This handles most article sites cleanly.

### Tweet Handling
1. Detect tweet URLs: check if URL contains `x.com/`, `twitter.com/`, or `t.co/`
2. Try nitter instances in order: `nitter.net`, `nitter.privacydev.net`
3. Replace the twitter/x.com domain with each nitter instance, fetch with requests
4. If nitter succeeds, extract text with trafilatura or BeautifulSoup
5. If all nitter instances fail, pass whatever metadata is available (URL, any preview text from Telegram message) to claude-sonnet-4-6

### Image Handling
1. Read image from `.tmp/images/<filename>`
2. Encode as base64
3. Pass to claude-sonnet-4-6 with vision prompt: "Extract and describe any text, charts, price data, or market information visible"
4. Skip stickers, GIFs, profile images — only process photo media

### Unfetchable URLs
If trafilatura returns None or fetch fails (timeout, paywall, 403, etc.), summarise using only the preview text from the Telegram message itself with claude-sonnet-4-6.

## Concurrency
Use `ThreadPoolExecutor(max_workers=8)` for concurrent processing of all items. Add 0.5s sleep between Anthropic API calls to respect rate limits (use a threading lock around API calls if needed).

## Output
List of summary objects written to `.tmp/summaries.json`, each with:
- `source_channel` — which channel the content came from
- `original_url` — the URL if any, else null
- `summary_text` — the generated summary
- `content_type` — one of: `long_article`, `short_article`, `tweet`, `image`, `plain_text`, `unfetchable_url`

## Edge Cases
- If a single item fails to process, log the error and skip it. Do not abort the entire batch.
- Empty or whitespace-only text: skip, do not send to Claude.
- Very long articles (>10000 words): truncate to first 10000 words before sending to Claude to stay within context limits.

## Testing
Pass `--test` flag to use fixture data from a local JSON file instead of live scraping and API calls.
