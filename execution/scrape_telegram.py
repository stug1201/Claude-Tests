#!/usr/bin/env python3
"""
scrape_telegram.py — Scrape recent messages from Telegram channels.

Pulls all messages from the last 24 hours from each configured source
Telegram channel, downloads photo attachments, deduplicates URLs, and
writes structured JSON output to .tmp/scraped.json.

Usage:
    python execution/scrape_telegram.py          # Live scrape
    python execution/scrape_telegram.py --test   # Generate fixture data

Environment variables (loaded from .env via python-dotenv):
    TELEGRAM_API_ID            — Telegram API application ID
    TELEGRAM_API_HASH          — Telegram API application hash
    TELEGRAM_SESSION_STRING    — Telethon StringSession token for headless auth
    TELEGRAM_SOURCE_CHANNELS   — Comma-separated list of channel usernames
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
# Resolve paths relative to the repository root (parent of execution/)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
IMAGES_DIR = TMP_DIR / "images"
OUTPUT_FILE = TMP_DIR / "scraped.json"


def ensure_directories() -> None:
    """Create .tmp/ and .tmp/images/ directories if they do not exist."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Output directories verified: %s, %s", TMP_DIR, IMAGES_DIR)


# ---------------------------------------------------------------------------
# URL extraction helper
# ---------------------------------------------------------------------------
# Simple but effective regex for extracting URLs from message text.
URL_PATTERN = re.compile(
    r"https?://[^\s<>\"'`,;)}\]]+",
    re.IGNORECASE,
)


def extract_first_url(text: str | None) -> str | None:
    """Return the first HTTP(S) URL found in *text*, or None."""
    if not text:
        return None
    match = URL_PATTERN.search(text)
    return match.group(0) if match else None


# ---------------------------------------------------------------------------
# Test / fixture mode
# ---------------------------------------------------------------------------

def generate_fixture_data() -> None:
    """
    Generate a small set of fake messages (5-6 items across 2 channels)
    and write them to .tmp/scraped.json, mimicking the live scraper output.
    Also creates a tiny placeholder image file for image-type fixtures.
    """
    ensure_directories()

    now = datetime.now(timezone.utc)
    fixture_messages: list[dict] = []

    # --- Channel 1: @crypto_signals -------------------------------------------
    channel_1 = "crypto_signals"

    fixture_messages.append({
        "channel": channel_1,
        "timestamp": (now - timedelta(hours=2)).isoformat(),
        "text": "BTC just broke $72k resistance. Watching for retest.",
        "media_type": "text",
        "url": None,
        "image_path": None,
    })

    fixture_messages.append({
        "channel": channel_1,
        "timestamp": (now - timedelta(hours=5)).isoformat(),
        "text": "Chart analysis for ETH/USDT 4H",
        "media_type": "image",
        "url": None,
        "image_path": str(IMAGES_DIR / "crypto_signals_1001.jpg"),
    })

    fixture_messages.append({
        "channel": channel_1,
        "timestamp": (now - timedelta(hours=8)).isoformat(),
        "text": "Deep dive on L2 scaling: https://example.com/l2-report",
        "media_type": "url",
        "url": "https://example.com/l2-report",
        "image_path": None,
    })

    # --- Channel 2: @tech_news ------------------------------------------------
    channel_2 = "tech_news"

    fixture_messages.append({
        "channel": channel_2,
        "timestamp": (now - timedelta(hours=1)).isoformat(),
        "text": "OpenAI announces GPT-5 preview: https://example.com/gpt5",
        "media_type": "url",
        "url": "https://example.com/gpt5",
        "image_path": None,
    })

    fixture_messages.append({
        "channel": channel_2,
        "timestamp": (now - timedelta(hours=3)).isoformat(),
        "text": "",
        "media_type": "image",
        "url": None,
        "image_path": str(IMAGES_DIR / "tech_news_2001.jpg"),
    })

    fixture_messages.append({
        "channel": channel_2,
        "timestamp": (now - timedelta(hours=6)).isoformat(),
        "text": "Rust 1.80 is out with major async improvements.",
        "media_type": "text",
        "url": None,
        "image_path": None,
    })

    # Create small placeholder image files for the image fixtures.
    for msg in fixture_messages:
        if msg["image_path"]:
            placeholder_path = Path(msg["image_path"])
            # Write a minimal "image" placeholder (a few bytes).
            placeholder_path.write_bytes(
                b"\x89PNG\r\n\x1a\n"  # PNG magic bytes (not a valid image, but enough for a fixture)
                b"\x00" * 32
            )
            logger.info("Created placeholder image: %s", placeholder_path)

    # Write the fixture JSON output.
    OUTPUT_FILE.write_text(
        json.dumps(fixture_messages, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        "Fixture data written: %d messages -> %s",
        len(fixture_messages),
        OUTPUT_FILE,
    )


# ---------------------------------------------------------------------------
# Live scraping logic
# ---------------------------------------------------------------------------

async def scrape_channels() -> None:
    """
    Connect to Telegram via Telethon, iterate over configured source channels,
    pull messages from the last 24 hours, download photos, and write the
    collected data to .tmp/scraped.json.
    """
    # Late imports so that the --test path never requires Telethon installed.
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:
        logger.error(
            "Telethon is not installed. "
            "Install it with: pip install telethon"
        )
        sys.exit(1)

    # Load environment variables from .env (if present).
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
        logger.info("Loaded .env from %s", PROJECT_ROOT / ".env")
    except ImportError:
        logger.warning(
            "python-dotenv not installed; reading env vars from environment only."
        )

    # ----- Read and validate required env vars --------------------------------
    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_string = os.getenv("TELEGRAM_SESSION_STRING")
    channels_raw = os.getenv("TELEGRAM_SOURCE_CHANNELS")

    missing: list[str] = []
    if not api_id_raw:
        missing.append("TELEGRAM_API_ID")
    if not api_hash:
        missing.append("TELEGRAM_API_HASH")
    if not session_string:
        missing.append("TELEGRAM_SESSION_STRING")
    if not channels_raw:
        missing.append("TELEGRAM_SOURCE_CHANNELS")

    if missing:
        logger.error(
            "Missing required environment variables: %s",
            ", ".join(missing),
        )
        sys.exit(1)

    # TELEGRAM_API_ID must be an integer.
    try:
        api_id = int(api_id_raw)  # type: ignore[arg-type]
    except ValueError:
        logger.error("TELEGRAM_API_ID must be an integer, got: %s", api_id_raw)
        sys.exit(1)

    # Parse comma-separated channel list; strip whitespace, ignore empties.
    channels: list[str] = [
        ch.strip() for ch in channels_raw.split(",") if ch.strip()  # type: ignore[union-attr]
    ]
    if not channels:
        logger.error("TELEGRAM_SOURCE_CHANNELS is empty after parsing.")
        sys.exit(1)

    logger.info("Channels to scrape: %s", channels)

    ensure_directories()

    # ----- Connect to Telegram ------------------------------------------------
    client = TelegramClient(
        StringSession(session_string),
        api_id,
        api_hash,  # type: ignore[arg-type]
    )

    await client.connect()

    if not await client.is_user_authorized():
        logger.error(
            "Session string is invalid or expired. "
            "Please generate a new TELEGRAM_SESSION_STRING."
        )
        await client.disconnect()
        sys.exit(1)

    logger.info("Authenticated with Telegram successfully.")

    # ----- Scrape each channel ------------------------------------------------
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    all_messages: list[dict] = []
    seen_urls: set[str] = set()  # Cross-channel URL deduplication

    for idx, channel_name in enumerate(channels):
        logger.info(
            "Scraping channel %d/%d: %s",
            idx + 1,
            len(channels),
            channel_name,
        )

        try:
            # Resolve the channel entity. Supports @username or full URL.
            entity = await client.get_entity(channel_name)
        except Exception as exc:
            logger.error(
                "Failed to resolve channel '%s': %s — skipping.",
                channel_name,
                exc,
            )
            # Pause before next channel even on error to respect rate limits.
            if idx < len(channels) - 1:
                await asyncio.sleep(1)
            continue

        channel_msg_count = 0

        try:
            # iter_messages with offset_date returns messages *older* than the
            # given date, so we iterate and break once we pass the cutoff.
            async for message in client.iter_messages(entity):
                # Stop once we've gone past the 24-hour window.
                if message.date.replace(tzinfo=timezone.utc) < cutoff:
                    break

                # ----- Skip non-photo media (stickers, GIFs, documents) ------
                if message.sticker or message.gif:
                    logger.debug(
                        "Skipping sticker/GIF in %s msg %d",
                        channel_name,
                        message.id,
                    )
                    continue

                # If there is a document that is NOT a photo, skip it.
                if message.document and not message.photo:
                    logger.debug(
                        "Skipping non-photo document in %s msg %d",
                        channel_name,
                        message.id,
                    )
                    continue

                # ----- Determine media type and extract data ------------------
                text = message.text or ""
                url = extract_first_url(text)
                image_path: str | None = None
                media_type = "text"  # default

                # Check for URL deduplication.
                if url:
                    if url in seen_urls:
                        # Strip the duplicate URL — keep the message but
                        # treat it as text-only (no url field).
                        logger.info(
                            "Duplicate URL skipped in %s msg %d: %s",
                            channel_name,
                            message.id,
                            url,
                        )
                        url = None
                    else:
                        seen_urls.add(url)
                        media_type = "url"

                # Download photo if present.
                if message.photo:
                    filename = f"{channel_name}_{message.id}.jpg"
                    download_path = IMAGES_DIR / filename
                    try:
                        await client.download_media(
                            message,
                            file=str(download_path),
                        )
                        image_path = str(download_path)
                        media_type = "image"
                        logger.info(
                            "Downloaded image: %s", download_path
                        )
                    except Exception as dl_exc:
                        logger.warning(
                            "Failed to download image for %s msg %d: %s",
                            channel_name,
                            message.id,
                            dl_exc,
                        )
                        # Fall through — message is still recorded, just
                        # without the image.

                # Build the message object.
                msg_obj = {
                    "channel": channel_name,
                    "timestamp": message.date.replace(
                        tzinfo=timezone.utc
                    ).isoformat(),
                    "text": text,
                    "media_type": media_type,
                    "url": url,
                    "image_path": image_path,
                }
                all_messages.append(msg_obj)
                channel_msg_count += 1

        except Exception as exc:
            logger.error(
                "Network/API error while fetching messages from '%s': %s — "
                "continuing with next channel.",
                channel_name,
                exc,
            )

        if channel_msg_count == 0:
            logger.info(
                "No messages found in the last 24h for channel: %s",
                channel_name,
            )
        else:
            logger.info(
                "Collected %d messages from %s",
                channel_msg_count,
                channel_name,
            )

        # Rate-limit pause between channel iterations (skip after the last one).
        if idx < len(channels) - 1:
            logger.info("Pausing 1 second before next channel...")
            await asyncio.sleep(1)

    # ----- Disconnect and write output ----------------------------------------
    await client.disconnect()
    logger.info("Disconnected from Telegram.")

    OUTPUT_FILE.write_text(
        json.dumps(all_messages, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(
        "Scrape complete: %d total messages written to %s",
        len(all_messages),
        OUTPUT_FILE,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Telegram channels for messages from the last 24 hours.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Generate fixture data instead of performing a live scrape.",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode: generating fixture data.")
        generate_fixture_data()
    else:
        logger.info("Running live Telegram scrape.")
        asyncio.run(scrape_channels())


if __name__ == "__main__":
    main()
