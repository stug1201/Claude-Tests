#!/usr/bin/env python3
"""
process_content.py - Process and summarise scraped Telegram content.

Reads message objects from .tmp/scraped.json (output of scrape_telegram.py),
classifies each item by content type, fetches full article text where possible,
and generates summaries using the appropriate Claude model.

Model routing (hardcoded):
    - Long articles (>1000 words):  claude-opus-4-5
    - Short articles (<=1000 words): claude-sonnet-4-6
    - Tweet URLs:                    nitter scrape first, then claude-sonnet-4-6
    - Images:                        claude-sonnet-4-6 (vision)
    - Plain text:                    claude-sonnet-4-6
    - Unfetchable URLs:              claude-sonnet-4-6 on preview text only

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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Model identifiers (hardcoded, not configurable).
MODEL_OPUS = "claude-opus-4-5"
MODEL_SONNET = "claude-sonnet-4-6"

# Word count thresholds.
LONG_ARTICLE_THRESHOLD = 1000   # Words above this -> opus
MAX_ARTICLE_WORDS = 10000       # Truncate articles longer than this

# Concurrency settings.
MAX_WORKERS = 8
API_RATE_LIMIT_SLEEP = 0.5  # Seconds to sleep between Anthropic API calls

# Nitter instances to try, in order.
NITTER_INSTANCES = ["nitter.net", "nitter.privacydev.net"]

# Content type labels for output.
CONTENT_TYPE_LONG_ARTICLE = "long_article"
CONTENT_TYPE_SHORT_ARTICLE = "short_article"
CONTENT_TYPE_TWEET = "tweet"
CONTENT_TYPE_IMAGE = "image"
CONTENT_TYPE_PLAIN_TEXT = "plain_text"
CONTENT_TYPE_UNFETCHABLE = "unfetchable_url"

# Threading lock for Anthropic API calls (rate limiting).
_api_lock = threading.Lock()


# ---------------------------------------------------------------------------
# URL classification helpers
# ---------------------------------------------------------------------------

def is_tweet_url(url: str) -> bool:
    """
    Check whether *url* points to a tweet on X/Twitter.

    Matches URLs containing 'x.com/', 'twitter.com/', or 't.co/'.
    """
    if not url:
        return False
    return bool(
        "x.com/" in url
        or "twitter.com/" in url
        or "t.co/" in url
    )


def _make_nitter_url(original_url: str, nitter_domain: str) -> str:
    """
    Replace the twitter/x.com domain in *original_url* with *nitter_domain*.

    Handles x.com, twitter.com, and t.co domains.
    """
    parsed = urlparse(original_url)
    # Replace just the netloc (domain) portion.
    new_parsed = parsed._replace(netloc=nitter_domain)
    return urlunparse(new_parsed)


# ---------------------------------------------------------------------------
# Content fetching
# ---------------------------------------------------------------------------

def fetch_article_text(url: str) -> Optional[str]:
    """
    Fetch the page at *url* and extract clean article text using trafilatura.

    Returns the extracted text, or None if fetching or extraction fails.
    """
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
    """
    Attempt to fetch tweet content via nitter proxy instances.

    Tries each nitter instance in NITTER_INSTANCES. On the first success,
    extracts text using trafilatura. Returns None if all instances fail.
    """
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

            # Try trafilatura first for clean extraction.
            clean_text = trafilatura.extract(resp.text)
            if clean_text and clean_text.strip():
                logger.info("Successfully fetched tweet via %s", nitter_domain)
                return clean_text

            # Fallback: try BeautifulSoup to grab the tweet content.
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                # Nitter wraps tweet content in .tweet-content class.
                tweet_div = soup.find("div", class_="tweet-content")
                if tweet_div and tweet_div.get_text(strip=True):
                    logger.info(
                        "Successfully fetched tweet via %s (BeautifulSoup)",
                        nitter_domain,
                    )
                    return tweet_div.get_text(strip=True)
            except ImportError:
                logger.debug("BeautifulSoup not available for fallback parsing")

        except Exception as exc:
            logger.warning(
                "Nitter instance %s failed for %s: %s",
                nitter_domain, url, exc,
            )
            continue

    # All nitter instances failed.
    logger.warning("All nitter instances failed for tweet: %s", url)
    return None


def read_image_as_base64(image_path: str) -> Optional[tuple]:
    """
    Read an image file and return (base64_data, media_type).

    Returns None if the file does not exist or cannot be read.
    """
    path = Path(image_path)

    # If the path is relative, resolve it against the project root.
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.exists():
        logger.error("Image file not found: %s", path)
        return None

    # Determine MIME type from extension.
    mime_type, _ = mimetypes.guess_type(str(path))
    if mime_type is None:
        # Default to JPEG for unknown image types.
        mime_type = "image/jpeg"

    try:
        raw_bytes = path.read_bytes()
        b64_data = base64.b64encode(raw_bytes).decode("utf-8")
        return b64_data, mime_type
    except Exception as exc:
        logger.error("Failed to read image %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Anthropic API helpers
# ---------------------------------------------------------------------------

def _call_anthropic_text(client, model: str, prompt: str) -> str:
    """
    Send a text prompt to the Anthropic API with rate limiting.

    Acquires the global API lock and sleeps for API_RATE_LIMIT_SLEEP seconds
    to avoid hitting rate limits, then makes the API call.
    """
    with _api_lock:
        time.sleep(API_RATE_LIMIT_SLEEP)

    response = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def _call_anthropic_vision(client, model: str, base64_data: str, media_type: str) -> str:
    """
    Send an image to the Anthropic API for vision analysis with rate limiting.

    Acquires the global API lock and sleeps for API_RATE_LIMIT_SLEEP seconds,
    then sends the image with a description prompt.
    """
    with _api_lock:
        time.sleep(API_RATE_LIMIT_SLEEP)

    response = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": base64_data,
                    },
                },
                {
                    "type": "text",
                    "text": (
                        "Extract and describe any text, charts, price data, "
                        "or market information visible"
                    ),
                },
            ],
        }],
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Per-item processing
# ---------------------------------------------------------------------------

def process_item(client, item: dict) -> Optional[dict]:
    """
    Process a single scraped message item and return a summary object.

    Determines the content type, fetches full text if needed, selects the
    appropriate model, and calls the Anthropic API for summarisation.

    Returns a summary dict, or None if the item should be skipped.
    """
    channel = item.get("channel", "unknown")
    text = item.get("text", "")
    media_type = item.get("media_type", "text")
    url = item.get("url")
    image_path = item.get("image_path")

    # --- Image handling ---
    if media_type == "image" and image_path:
        logger.info("Processing image from %s: %s", channel, image_path)

        img_result = read_image_as_base64(image_path)
        if img_result is None:
            logger.error("Skipping unreadable image: %s", image_path)
            return None

        b64_data, mime = img_result
        try:
            summary = _call_anthropic_vision(client, MODEL_SONNET, b64_data, mime)
            return {
                "source_channel": channel,
                "original_url": url,
                "summary_text": summary,
                "content_type": CONTENT_TYPE_IMAGE,
            }
        except Exception as exc:
            logger.error("API error processing image %s: %s", image_path, exc)
            return None

    # --- Tweet URL handling ---
    if url and is_tweet_url(url):
        logger.info("Processing tweet from %s: %s", channel, url)

        # Try nitter first.
        tweet_text = fetch_tweet_text(url)
        if tweet_text and tweet_text.strip():
            # Successfully fetched tweet content via nitter.
            prompt = f"Summarise this tweet:\n\n{tweet_text}"
        else:
            # Nitter failed — use URL and any available preview text.
            preview = text.strip() if text and text.strip() else ""
            if preview:
                prompt = (
                    f"Summarise this tweet based on the available information.\n"
                    f"Tweet URL: {url}\n"
                    f"Preview text: {preview}"
                )
            else:
                prompt = (
                    f"Summarise this tweet based on the URL alone.\n"
                    f"Tweet URL: {url}"
                )

        try:
            summary = _call_anthropic_text(client, MODEL_SONNET, prompt)
            return {
                "source_channel": channel,
                "original_url": url,
                "summary_text": summary,
                "content_type": CONTENT_TYPE_TWEET,
            }
        except Exception as exc:
            logger.error("API error processing tweet %s: %s", url, exc)
            return None

    # --- URL (article) handling ---
    if url and media_type == "url":
        logger.info("Processing URL from %s: %s", channel, url)

        clean_text = fetch_article_text(url)

        if clean_text is None or not clean_text.strip():
            # Unfetchable URL — fall back to preview text from Telegram message.
            logger.info("URL unfetchable, using preview text: %s", url)
            preview = text.strip() if text and text.strip() else ""
            if not preview:
                logger.warning(
                    "Skipping unfetchable URL with no preview text: %s", url
                )
                return None

            prompt = (
                f"Summarise the following content preview for an article that "
                f"could not be fully fetched.\n"
                f"URL: {url}\n"
                f"Preview: {preview}"
            )
            try:
                summary = _call_anthropic_text(client, MODEL_SONNET, prompt)
                return {
                    "source_channel": channel,
                    "original_url": url,
                    "summary_text": summary,
                    "content_type": CONTENT_TYPE_UNFETCHABLE,
                }
            except Exception as exc:
                logger.error(
                    "API error processing unfetchable URL %s: %s", url, exc
                )
                return None

        # Article successfully fetched. Count words on clean extracted text.
        word_count = len(clean_text.split())
        logger.info(
            "Fetched article from %s: %d words", url, word_count,
        )

        # Truncate very long articles to stay within context limits.
        if word_count > MAX_ARTICLE_WORDS:
            logger.info(
                "Truncating article from %d to %d words", word_count, MAX_ARTICLE_WORDS
            )
            words = clean_text.split()
            clean_text = " ".join(words[:MAX_ARTICLE_WORDS])
            word_count = MAX_ARTICLE_WORDS

        # Select model based on word count.
        if word_count > LONG_ARTICLE_THRESHOLD:
            model = MODEL_OPUS
            content_type = CONTENT_TYPE_LONG_ARTICLE
        else:
            model = MODEL_SONNET
            content_type = CONTENT_TYPE_SHORT_ARTICLE

        prompt = f"Summarise the following article:\n\n{clean_text}"
        try:
            summary = _call_anthropic_text(client, model, prompt)
            return {
                "source_channel": channel,
                "original_url": url,
                "summary_text": summary,
                "content_type": content_type,
            }
        except Exception as exc:
            logger.error("API error processing article %s: %s", url, exc)
            return None

    # --- Plain text handling ---
    if text and text.strip():
        logger.info("Processing plain text from %s", channel)

        prompt = f"Summarise the following message:\n\n{text.strip()}"
        try:
            summary = _call_anthropic_text(client, MODEL_SONNET, prompt)
            return {
                "source_channel": channel,
                "original_url": None,
                "summary_text": summary,
                "content_type": CONTENT_TYPE_PLAIN_TEXT,
            }
        except Exception as exc:
            logger.error("API error processing plain text from %s: %s", channel, exc)
            return None

    # Nothing to process (empty text, no URL, no image).
    logger.warning("Skipping empty item from %s", channel)
    return None


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_all(items: list[dict]) -> list[dict]:
    """
    Process all scraped items concurrently using a thread pool.

    Initialises the Anthropic client, dispatches items to worker threads,
    collects results, and returns a list of summary dicts.

    Items that fail are logged and skipped (the batch is never aborted).
    """
    import anthropic

    client = anthropic.Anthropic()  # Reads ANTHROPIC_API_KEY from env.

    summaries: list[dict] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all items for concurrent processing.
        future_to_index = {
            executor.submit(process_item, client, item): i
            for i, item in enumerate(items)
        }

        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                result = future.result()
                if result is not None:
                    summaries.append(result)
                    logger.info(
                        "Item %d processed successfully (%s)",
                        idx, result["content_type"],
                    )
                else:
                    logger.info("Item %d skipped (no result)", idx)
            except Exception as exc:
                # Catch any unexpected exceptions from the thread.
                logger.error("Item %d raised an unexpected error: %s", idx, exc)

    logger.info(
        "Processing complete: %d/%d items produced summaries",
        len(summaries), len(items),
    )
    return summaries


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def load_scraped_data() -> list[dict]:
    """
    Load scraped message objects from .tmp/scraped.json.

    Returns the list of message dicts, or exits with an error if the file
    is missing or malformed.
    """
    if not SCRAPED_PATH.exists():
        logger.error("Scraped data file not found at %s", SCRAPED_PATH)
        sys.exit(1)

    try:
        data = json.loads(SCRAPED_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in %s: %s", SCRAPED_PATH, exc)
        sys.exit(1)

    if not isinstance(data, list):
        logger.error("Expected a JSON array in %s, got %s", SCRAPED_PATH, type(data).__name__)
        sys.exit(1)

    logger.info("Loaded %d items from %s", len(data), SCRAPED_PATH)
    return data


def write_summaries(summaries: list[dict]) -> None:
    """
    Write the list of summary objects to .tmp/summaries.json.

    Creates the .tmp/ directory if it does not exist.
    """
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
    """
    --test mode: generate fixture summaries without making any API calls.

    Writes a small set of fake summary objects to .tmp/summaries.json so that
    downstream scripts can be tested independently.
    """
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

    # Ensure .tmp/ directory exists.
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
    Production path: load .env, read scraped data, process all items
    concurrently, and write summaries to .tmp/summaries.json.
    """
    # Load environment variables from .env file.
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH)

    # Verify the API key is available.
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error(
            "ANTHROPIC_API_KEY is not set. Add it to your .env file at %s",
            ENV_PATH,
        )
        sys.exit(1)

    # Load scraped messages.
    items = load_scraped_data()

    if not items:
        logger.warning("No items to process. Writing empty summaries file.")
        write_summaries([])
        return

    # Process all items concurrently.
    summaries = process_all(items)

    # Write results.
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
