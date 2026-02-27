#!/usr/bin/env python3
"""
process_content.py - Process and summarise scraped Telegram content.

Uses a two-phase pipeline designed for low API rate limits (5 req/min):

  Phase 1 — Pre-filter (zero API calls):
      Scores each scraped item using text heuristics (length, URL presence,
      channel diversity) and keeps only the top N highest-signal items.

  Phase 2 — Batched processing:
      Groups selected items into batched API calls: text items batched
      5 per prompt, images batched 3 per vision call. Calls are made
      sequentially with rate-limit-safe delays between them.

Model routing:
    - All item processing:  claude-sonnet-4-6
    - Tweet URLs:           nitter scrape first, then included in text batch
    - Images:               batched vision calls (3 per call)
    - Articles:             fetched via trafilatura, then included in text batch

Usage:
    python execution/process_content.py          # Process live data
    python execution/process_content.py --test   # Use fixture data, no API calls
"""

import argparse
import base64
import json
import logging
import mimetypes
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Resolve paths relative to the project root (parent of execution/).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
SCRAPED_PATH = TMP_DIR / "scraped.json"
SUMMARIES_PATH = TMP_DIR / "summaries.json"
IMAGES_DIR = TMP_DIR / "images"
ENV_PATH = PROJECT_ROOT / ".env"

# Model — all item processing goes through Sonnet.
MODEL_SONNET = "claude-sonnet-4-6"

# Pre-filter: keep only the top N items after scoring.
MAX_ITEMS_TO_PROCESS = 20

# Batching: how many items per single API call.
TEXT_BATCH_SIZE = 5
IMAGE_BATCH_SIZE = 3

# Rate limiting: seconds to wait between API calls.
# At 5 req/min the minimum safe interval is 12s; we add 1s buffer.
RATE_LIMIT_DELAY = 13.0

# Word count limits for articles included in batch prompts.
MAX_ARTICLE_WORDS = 2000

# Nitter instances to try, in order.
NITTER_INSTANCES = ["nitter.net", "nitter.privacydev.net"]

# Content type labels for output.
CONTENT_TYPE_LONG_ARTICLE = "long_article"
CONTENT_TYPE_SHORT_ARTICLE = "short_article"
CONTENT_TYPE_TWEET = "tweet"
CONTENT_TYPE_IMAGE = "image"
CONTENT_TYPE_PLAIN_TEXT = "plain_text"
CONTENT_TYPE_UNFETCHABLE = "unfetchable_url"


# ---------------------------------------------------------------------------
# URL classification helpers
# ---------------------------------------------------------------------------

def is_tweet_url(url: str) -> bool:
    """Check whether *url* points to a tweet on X/Twitter."""
    if not url:
        return False
    return "x.com/" in url or "twitter.com/" in url or "t.co/" in url


def _make_nitter_url(original_url: str, nitter_domain: str) -> str:
    """Replace the twitter/x.com domain with *nitter_domain*."""
    parsed = urlparse(original_url)
    return urlunparse(parsed._replace(netloc=nitter_domain))


# ---------------------------------------------------------------------------
# Content fetching (web scraping only — no Anthropic API calls)
# ---------------------------------------------------------------------------

def fetch_article_text(url: str) -> Optional[str]:
    """Fetch the page at *url* and extract clean article text via trafilatura."""
    try:
        import trafilatura

        downloaded = trafilatura.fetch_url(url)
        if downloaded is None:
            logger.warning("trafilatura.fetch_url returned None for %s", url)
            return None

        clean_text = trafilatura.extract(downloaded)
        if clean_text is None or not clean_text.strip():
            logger.warning("trafilatura.extract returned empty text for %s", url)
            return None

        return clean_text
    except Exception as exc:
        logger.error("Failed to fetch/extract article from %s: %s", url, exc)
        return None


def fetch_tweet_text(url: str) -> Optional[str]:
    """Attempt to fetch tweet content via nitter proxy instances."""
    import requests
    import trafilatura

    for nitter_domain in NITTER_INSTANCES:
        nitter_url = _make_nitter_url(url, nitter_domain)
        logger.info("Trying nitter instance: %s", nitter_url)

        try:
            resp = requests.get(nitter_url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (compatible; TelegramBrief/1.0)"
            })
            resp.raise_for_status()

            clean_text = trafilatura.extract(resp.text)
            if clean_text and clean_text.strip():
                logger.info("Successfully fetched tweet via %s", nitter_domain)
                return clean_text

            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                tweet_div = soup.find("div", class_="tweet-content")
                if tweet_div and tweet_div.get_text(strip=True):
                    logger.info("Fetched tweet via %s (BeautifulSoup)", nitter_domain)
                    return tweet_div.get_text(strip=True)
            except ImportError:
                pass

        except Exception as exc:
            logger.warning("Nitter instance %s failed for %s: %s", nitter_domain, url, exc)
            continue

    logger.warning("All nitter instances failed for tweet: %s", url)
    return None


def read_image_as_base64(image_path: str) -> Optional[tuple]:
    """Read an image file and return (base64_data, media_type)."""
    path = Path(image_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        logger.error("Image file not found: %s", path)
        return None

    mime_type, _ = mimetypes.guess_type(str(path))
    if mime_type is None:
        mime_type = "image/jpeg"

    try:
        raw_bytes = path.read_bytes()
        b64_data = base64.b64encode(raw_bytes).decode("utf-8")
        return b64_data, mime_type
    except Exception as exc:
        logger.error("Failed to read image %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Phase 1: Pre-filter — score and rank items (zero API calls)
# ---------------------------------------------------------------------------

def score_item(item: dict) -> float:
    """
    Score an item by likely informational value. Higher = more important.
    Uses only local heuristics — no API calls.
    """
    score = 0.0
    text = (item.get("text") or "").strip()
    channel = item.get("channel", "")
    media_type = item.get("media_type", "text")
    url = item.get("url")

    # --- Text quality signals ---
    if len(text) > 200:
        score += 4
    elif len(text) > 100:
        score += 3
    elif len(text) > 20:
        score += 1

    # --- Has a URL (articles/tweets carry more structured info) ---
    if url:
        score += 2
        if is_tweet_url(url):
            score += 1  # Tweets often contain breaking news

    # --- Channel diversity ---
    # Channels with fewer messages per day tend to be higher signal per post.
    # ahboyashreads dominates with ~50 image posts; cap its representation.
    if channel != "ahboyashreads":
        score += 3

    # --- Content type efficiency ---
    # Plain text with content is cheapest to process.
    if media_type == "text" and text:
        score += 1

    # Image with caption text is higher value than bare image.
    if media_type == "image" and len(text) > 10:
        score += 1

    return score


def pre_filter_items(
    items: list[dict], max_items: int = MAX_ITEMS_TO_PROCESS
) -> list[dict]:
    """
    Score all items, sort by score descending, return the top *max_items*.
    """
    if len(items) <= max_items:
        logger.info(
            "Pre-filter: all %d items kept (at or below threshold of %d)",
            len(items), max_items,
        )
        return items

    scored = [(score_item(item), i, item) for i, item in enumerate(items)]
    # Sort by score descending, then by original order for ties.
    scored.sort(key=lambda x: (-x[0], x[1]))
    selected = [item for _, _, item in scored[:max_items]]

    # Log channel distribution.
    kept_channels: dict[str, int] = {}
    for _, _, item in scored[:max_items]:
        ch = item.get("channel", "?")
        kept_channels[ch] = kept_channels.get(ch, 0) + 1

    logger.info(
        "Pre-filter: %d/%d items selected. Per-channel: %s",
        max_items, len(items),
        ", ".join(f"{ch}={n}" for ch, n in sorted(kept_channels.items())),
    )
    return selected


# ---------------------------------------------------------------------------
# Phase 2: Batched API calls
# ---------------------------------------------------------------------------

def _parse_numbered_response(text: str, expected_count: int) -> list[str]:
    """
    Parse a numbered response (1. xxx  2. xxx ...) into a list of strings.
    Returns a list of length *expected_count*; missing entries are empty strings.
    """
    sections = [""] * expected_count
    current_idx = -1
    current_lines: list[str] = []

    for line in text.split("\n"):
        # Match: "1.", "1)", "**1.**", "**1:**", "Image 1:", "Message 1:", etc.
        m = re.match(
            r'^\s*(?:\*\*)?(?:Image\s+|Message\s+)?(\d+)[.):\s]',
            line,
            re.IGNORECASE,
        )
        if m:
            num = int(m.group(1))
            # Flush previous section.
            if 0 <= current_idx < expected_count:
                sections[current_idx] = "\n".join(current_lines).strip()
            current_idx = num - 1  # 1-indexed → 0-indexed
            # Strip the number prefix from this line.
            rest = re.sub(
                r'^\s*(?:\*\*)?(?:Image\s+|Message\s+)?(\d+)[.):\s]+(?:\*\*)?\s*',
                '', line, flags=re.IGNORECASE,
            )
            current_lines = [rest] if rest.strip() else []
        elif current_idx >= 0:
            current_lines.append(line)

    # Flush the final section.
    if 0 <= current_idx < expected_count:
        sections[current_idx] = "\n".join(current_lines).strip()

    return sections


def _process_text_batch(client, items: list[dict]) -> list[dict]:
    """
    Process a batch of text-based items in a single API call.
    Returns a list of summary dicts.
    """
    if not items:
        return []

    # Build the batched prompt.
    parts = [
        "Analyze the following messages and provide a concise summary "
        "(2-4 sentences) for each one. Focus on key facts, figures, and "
        "market implications. Number your summaries to match the input numbers.\n"
    ]

    for i, item in enumerate(items, 1):
        channel = item.get("channel", "unknown")
        text = (item.get("text") or "").strip()
        url = item.get("url", "")
        fetched = item.get("_fetched_text", "")

        parts.append(f"\n--- Message {i} (from {channel}) ---")
        if url:
            parts.append(f"URL: {url}")
        if fetched:
            words = fetched.split()
            if len(words) > MAX_ARTICLE_WORDS:
                fetched = " ".join(words[:MAX_ARTICLE_WORDS]) + " [truncated]"
            parts.append(f"Article content:\n{fetched}")
        elif text:
            parts.append(f"Content: {text}")
        else:
            parts.append("Content: (no text available)")

    prompt = "\n".join(parts)
    logger.info("Text batch: %d items, prompt %d chars", len(items), len(prompt))

    try:
        response = client.messages.create(
            model=MODEL_SONNET,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        combined = response.content[0].text
        parsed = _parse_numbered_response(combined, len(items))

        summaries = []
        for i, item in enumerate(items):
            if parsed[i]:
                url = item.get("url")
                if url and is_tweet_url(url):
                    ctype = CONTENT_TYPE_TWEET
                elif url and item.get("media_type") == "url":
                    fetched_text = item.get("_fetched_text", "")
                    if fetched_text and len(fetched_text.split()) > 1000:
                        ctype = CONTENT_TYPE_LONG_ARTICLE
                    elif fetched_text:
                        ctype = CONTENT_TYPE_SHORT_ARTICLE
                    else:
                        ctype = CONTENT_TYPE_UNFETCHABLE
                elif url:
                    ctype = CONTENT_TYPE_UNFETCHABLE
                else:
                    ctype = CONTENT_TYPE_PLAIN_TEXT

                summaries.append({
                    "source_channel": item.get("channel", "unknown"),
                    "original_url": url,
                    "summary_text": parsed[i],
                    "content_type": ctype,
                })
            else:
                logger.warning("No summary parsed for text batch item %d", i + 1)

        logger.info("Text batch produced %d/%d summaries", len(summaries), len(items))
        return summaries

    except Exception as exc:
        logger.error("Text batch API call failed: %s", exc)
        return []


def _process_image_batch(client, items: list[dict]) -> list[dict]:
    """
    Process a batch of images in a single vision API call.
    Returns a list of summary dicts.
    """
    if not items:
        return []

    content: list[dict] = []
    valid_items: list[dict] = []

    for item in items:
        img = read_image_as_base64(item.get("image_path", ""))
        if img is None:
            logger.warning("Skipping unreadable image: %s", item.get("image_path"))
            continue
        b64, mime = img
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": mime, "data": b64},
        })
        valid_items.append(item)

    if not valid_items:
        return []

    # Add the instruction text.
    if len(valid_items) == 1:
        content.append({
            "type": "text",
            "text": (
                "Extract and describe any text, charts, price data, or "
                "market information visible in this image."
            ),
        })
    else:
        content.append({
            "type": "text",
            "text": (
                f"There are {len(valid_items)} images above. For each image "
                f"(numbered 1 to {len(valid_items)} in order), extract and "
                f"describe any text, charts, price data, or market information "
                f"visible. Number your descriptions to match the image order."
            ),
        })

    logger.info("Image batch: %d images", len(valid_items))

    try:
        response = client.messages.create(
            model=MODEL_SONNET,
            max_tokens=2048,
            messages=[{"role": "user", "content": content}],
        )
        combined = response.content[0].text

        # Single image — no numbered parsing needed.
        if len(valid_items) == 1:
            return [{
                "source_channel": valid_items[0].get("channel", "unknown"),
                "original_url": valid_items[0].get("url"),
                "summary_text": combined.strip(),
                "content_type": CONTENT_TYPE_IMAGE,
            }]

        # Multi-image — parse numbered descriptions.
        parsed = _parse_numbered_response(combined, len(valid_items))
        summaries = []
        for i, item in enumerate(valid_items):
            if parsed[i]:
                summaries.append({
                    "source_channel": item.get("channel", "unknown"),
                    "original_url": item.get("url"),
                    "summary_text": parsed[i],
                    "content_type": CONTENT_TYPE_IMAGE,
                })
            else:
                logger.warning("No summary parsed for image batch item %d", i + 1)

        logger.info("Image batch produced %d/%d summaries", len(summaries), len(valid_items))
        return summaries

    except Exception as exc:
        logger.error("Image batch API call failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Main processing pipeline
# ---------------------------------------------------------------------------

def process_all(items: list[dict]) -> list[dict]:
    """
    Two-phase pipeline:
      1. Pre-filter items by heuristic score (no API calls)
      2. Pre-fetch article/tweet text via web scraping (no API calls)
      3. Batch items and process via Anthropic API with rate-safe delays
    """
    import anthropic

    client = anthropic.Anthropic()

    # Phase 1: Pre-filter.
    selected = pre_filter_items(items)

    # Phase 1.5: Pre-fetch URLs (web scraping only, no Anthropic API calls).
    for item in selected:
        url = item.get("url")
        if url and item.get("media_type") == "url":
            logger.info("Pre-fetching URL: %s", url)
            if is_tweet_url(url):
                item["_fetched_text"] = fetch_tweet_text(url) or ""
            else:
                fetched = fetch_article_text(url) or ""
                item["_fetched_text"] = fetched
                if fetched:
                    logger.info(
                        "Fetched %d words from %s",
                        len(fetched.split()), url,
                    )

    # Separate into image items and text items.
    image_items = [
        i for i in selected
        if i.get("media_type") == "image" and i.get("image_path")
    ]
    text_items = [i for i in selected if i not in image_items]

    logger.info(
        "Processing %d text items + %d image items = %d total",
        len(text_items), len(image_items), len(text_items) + len(image_items),
    )

    summaries: list[dict] = []
    api_call_count = 0

    # Phase 2a: Process text batches.
    for batch_start in range(0, len(text_items), TEXT_BATCH_SIZE):
        batch = text_items[batch_start:batch_start + TEXT_BATCH_SIZE]

        if api_call_count > 0:
            logger.info(
                "Rate limit pause: %.0fs before next API call...",
                RATE_LIMIT_DELAY,
            )
            time.sleep(RATE_LIMIT_DELAY)

        results = _process_text_batch(client, batch)
        summaries.extend(results)
        api_call_count += 1

    # Phase 2b: Process image batches.
    for batch_start in range(0, len(image_items), IMAGE_BATCH_SIZE):
        batch = image_items[batch_start:batch_start + IMAGE_BATCH_SIZE]

        if api_call_count > 0:
            logger.info(
                "Rate limit pause: %.0fs before next API call...",
                RATE_LIMIT_DELAY,
            )
            time.sleep(RATE_LIMIT_DELAY)

        results = _process_image_batch(client, batch)
        summaries.extend(results)
        api_call_count += 1

    logger.info(
        "Processing complete: %d/%d selected items produced summaries (%d API calls)",
        len(summaries), len(selected), api_call_count,
    )
    return summaries


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_scraped_data() -> list[dict]:
    """Load scraped message objects from .tmp/scraped.json."""
    if not SCRAPED_PATH.exists():
        logger.error("Scraped data file not found at %s", SCRAPED_PATH)
        sys.exit(1)

    try:
        data = json.loads(SCRAPED_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in %s: %s", SCRAPED_PATH, exc)
        sys.exit(1)

    if not isinstance(data, list):
        logger.error(
            "Expected a JSON array in %s, got %s",
            SCRAPED_PATH, type(data).__name__,
        )
        sys.exit(1)

    logger.info("Loaded %d items from %s", len(data), SCRAPED_PATH)
    return data


def write_summaries(summaries: list[dict]) -> None:
    """Write the list of summary objects to .tmp/summaries.json."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARIES_PATH.write_text(
        json.dumps(summaries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Wrote %d summaries to %s", len(summaries), SUMMARIES_PATH)


# ---------------------------------------------------------------------------
# Test / fixture mode
# ---------------------------------------------------------------------------

def run_test_mode() -> None:
    """--test mode: generate fixture summaries without making any API calls."""
    logger.info("Running in test mode — using fixture data, no API calls")

    fixture_summaries = [
        {
            "source_channel": "test_channel_alpha",
            "original_url": "https://example.com/long-article-about-markets",
            "summary_text": (
                "[TEST] This is a fixture summary for a long article. "
                "The article discussed macroeconomic trends, central bank "
                "policy shifts, and their impact on emerging markets over "
                "the coming quarter."
            ),
            "content_type": CONTENT_TYPE_LONG_ARTICLE,
        },
        {
            "source_channel": "test_channel_alpha",
            "original_url": "https://example.com/short-news",
            "summary_text": (
                "[TEST] This is a fixture summary for a short article. "
                "Bitcoin briefly topped $70k before pulling back to $68k "
                "amid profit-taking."
            ),
            "content_type": CONTENT_TYPE_SHORT_ARTICLE,
        },
        {
            "source_channel": "test_channel_beta",
            "original_url": "https://x.com/elonmusk/status/123456789",
            "summary_text": (
                "[TEST] This is a fixture summary for a tweet. "
                "The author commented on recent regulatory developments "
                "in the cryptocurrency space."
            ),
            "content_type": CONTENT_TYPE_TWEET,
        },
        {
            "source_channel": "test_channel_beta",
            "original_url": None,
            "summary_text": (
                "[TEST] This is a fixture summary for an image. "
                "The chart shows BTC/USD 4-hour candles with a descending "
                "wedge pattern and RSI divergence near the 30 level."
            ),
            "content_type": CONTENT_TYPE_IMAGE,
        },
        {
            "source_channel": "test_channel_gamma",
            "original_url": None,
            "summary_text": (
                "[TEST] This is a fixture summary for plain text. "
                "The message contained a brief market commentary noting "
                "increased volume on ETH pairs."
            ),
            "content_type": CONTENT_TYPE_PLAIN_TEXT,
        },
        {
            "source_channel": "test_channel_gamma",
            "original_url": "https://paywalled-site.example.com/article",
            "summary_text": (
                "[TEST] This is a fixture summary for an unfetchable URL. "
                "Based on the preview text, the article appears to cover "
                "new SEC enforcement actions against DeFi protocols."
            ),
            "content_type": CONTENT_TYPE_UNFETCHABLE,
        },
    ]

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    write_summaries(fixture_summaries)

    print("=" * 60)
    print(f"TEST MODE: Wrote {len(fixture_summaries)} fixture summaries")
    print(f"Output: {SUMMARIES_PATH}")
    print("=" * 60)
    for i, s in enumerate(fixture_summaries, start=1):
        print(f"\n  [{i}] ({s['content_type']}) {s['source_channel']}")
        print(f"      URL: {s['original_url'] or '(none)'}")
        print(f"      Summary: {s['summary_text'][:80]}...")
    print()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_production() -> None:
    """
    Production path: load .env, read scraped data, run the two-phase
    pipeline, and write summaries to .tmp/summaries.json.
    """
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error(
            "ANTHROPIC_API_KEY is not set. Add it to your .env file at %s",
            ENV_PATH,
        )
        sys.exit(1)

    items = load_scraped_data()

    if not items:
        logger.warning("No items to process. Writing empty summaries file.")
        write_summaries([])
        return

    summaries = process_all(items)
    write_summaries(summaries)
    logger.info("Done. %d summaries written to %s", len(summaries), SUMMARIES_PATH)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process and summarise scraped Telegram content.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use fixture data instead of live scraping and API calls.",
    )
    args = parser.parse_args()

    if args.test:
        run_test_mode()
    else:
        run_production()


if __name__ == "__main__":
    main()
