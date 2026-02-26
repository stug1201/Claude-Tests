#!/usr/bin/env python3
"""
send_brief.py - Send the compiled daily brief to Telegram.

Reads the formatted brief from .tmp/brief.txt (output of compile_brief.py),
then sends it to the configured Telegram chat using python-telegram-bot.

Handles message splitting for Telegram's 4096 character limit, cleanup of
the .tmp/ directory after a successful send, and various edge cases.

Usage:
    python execution/send_brief.py          # Send brief to Telegram
    python execution/send_brief.py --test   # Print brief to stdout instead
"""

import argparse
import asyncio
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
# Telegram's hard limit per message.
MAX_MESSAGE_LENGTH = 4096

# Delay in seconds between consecutive message sends.
SEND_DELAY = 0.5

# Resolve paths relative to the project root (parent of execution/).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRIEF_PATH = PROJECT_ROOT / ".tmp" / "brief.txt"
TMP_DIR = PROJECT_ROOT / ".tmp"
ENV_PATH = PROJECT_ROOT / ".env"

# Sample brief used in --test mode when .tmp/brief.txt does not exist.
SAMPLE_BRIEF = (
    "*Daily Brief - Test Mode*\n\n"
    "This is a sample brief used for --test mode because "
    "`.tmp/brief.txt` was not found.\n\n"
    "Run `compile_brief.py` first to generate a real brief."
)


# ---------------------------------------------------------------------------
# Message splitting helpers
# ---------------------------------------------------------------------------

def _count_open_formatting(text: str) -> dict:
    """
    Count unmatched (open) Markdown formatting markers in *text*.

    Tracks bold (**), italic (* but not **), and inline code (`).
    Returns a dict mapping each marker to its open/closed state (True = open).
    """
    # We track whether we are currently "inside" each formatting marker.
    state = {"**": False, "*": False, "`": False}

    # Bold first (** before *), so we consume ** tokens before single *.
    bold_count = text.count("**")
    state["**"] = bold_count % 2 == 1

    # For italic we need to count single * that are NOT part of **.
    # Remove all ** occurrences, then count remaining *.
    stripped = text.replace("**", "")
    italic_count = stripped.count("*")
    state["*"] = italic_count % 2 == 1

    code_count = text.count("`")
    state["`"] = code_count % 2 == 1

    return state


def _close_open_formatting(text: str) -> str:
    """
    Append closing markers for any markdown formatting left open in *text*.
    This prevents Telegram from rejecting a chunk that has unmatched markers.
    """
    state = _count_open_formatting(text)
    suffix = ""
    # Close in reverse nesting order: code, then italic, then bold.
    if state["`"]:
        suffix += "`"
    if state["*"]:
        suffix += "*"
    if state["**"]:
        suffix += "**"
    return text + suffix


def _reopen_formatting(text: str) -> str:
    """
    Prepend reopening markers that were closed at the end of the previous
    chunk so that formatting continues seamlessly.
    """
    state = _count_open_formatting(text)
    prefix = ""
    # Open in nesting order: bold, italic, code.
    if state["**"]:
        prefix += "**"
    if state["*"]:
        prefix += "*"
    if state["`"]:
        prefix += "`"
    return prefix


def _split_paragraph_at_sentences(paragraph: str, limit: int) -> list[str]:
    """
    Split a single paragraph that exceeds *limit* characters at sentence
    boundaries. Sentences are delimited by '. ', '! ', '? ', or newline.

    If a single sentence still exceeds the limit it is hard-split at *limit*
    characters as a last resort.
    """
    # Split on sentence-ending punctuation followed by a space (keep delimiter).
    sentences = re.split(r'(?<=[.!?])\s+', paragraph)

    chunks: list[str] = []
    current = ""

    for sentence in sentences:
        # If adding this sentence would exceed the limit, flush current chunk.
        if current and len(current) + 1 + len(sentence) > limit:
            chunks.append(current)
            current = ""

        # If the sentence itself exceeds the limit, hard-split it.
        if len(sentence) > limit:
            # Flush anything in current first.
            if current:
                chunks.append(current)
                current = ""
            # Hard-split the oversized sentence.
            for i in range(0, len(sentence), limit):
                chunks.append(sentence[i:i + limit])
        else:
            current = f"{current} {sentence}".strip() if current else sentence

    if current:
        chunks.append(current)

    return chunks


def split_message(text: str) -> list[str]:
    """
    Split *text* into chunks that each fit within Telegram's 4096-char limit.

    Strategy:
      1. Split on paragraph boundaries (double newline).
      2. Accumulate paragraphs into a chunk until the limit would be exceeded.
      3. If a single paragraph exceeds the limit, split at sentence boundaries.
      4. After splitting, close any open markdown formatting at the end of each
         chunk and reopen it at the start of the next chunk.
    """
    # If the entire message fits, return it as-is.
    if len(text) <= MAX_MESSAGE_LENGTH:
        return [text]

    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current = ""

    for para in paragraphs:
        candidate = f"{current}\n\n{para}" if current else para

        if len(candidate) <= MAX_MESSAGE_LENGTH:
            # Still fits — keep accumulating.
            current = candidate
        else:
            # Flush the current chunk (if non-empty).
            if current:
                chunks.append(current)
                current = ""

            # Check if this single paragraph exceeds the limit on its own.
            if len(para) > MAX_MESSAGE_LENGTH:
                sentence_chunks = _split_paragraph_at_sentences(
                    para, MAX_MESSAGE_LENGTH
                )
                # All but the last sentence-chunk go directly to chunks;
                # the last one becomes 'current' so more paragraphs can attach.
                chunks.extend(sentence_chunks[:-1])
                current = sentence_chunks[-1]
            else:
                current = para

    # Don't forget the last accumulated chunk.
    if current:
        chunks.append(current)

    # ---- Fix markdown formatting across chunk boundaries ----
    # We walk through the chunks and ensure every chunk has balanced markers.
    # If a chunk closes formatting that was open, the next chunk reopens it.
    fixed: list[str] = []
    carry_prefix = ""  # Formatting markers to prepend to the next chunk.

    for chunk in chunks:
        # Prepend any formatting that was left open from the previous chunk.
        chunk = carry_prefix + chunk

        # Determine what formatting is still open at the end of this chunk.
        state = _count_open_formatting(chunk)
        any_open = state["**"] or state["*"] or state["`"]

        if any_open:
            # Close the open formatting for this chunk and carry it forward.
            closed_chunk = _close_open_formatting(chunk)
            carry_prefix = _reopen_formatting(chunk)
            fixed.append(closed_chunk)
        else:
            carry_prefix = ""
            fixed.append(chunk)

    return fixed


# ---------------------------------------------------------------------------
# Telegram sending
# ---------------------------------------------------------------------------

async def send_to_telegram(bot_token: str, chat_id: str, text: str) -> int:
    """
    Send *text* to the Telegram chat identified by *chat_id*.

    Returns the number of messages sent.

    Raises on permission or network errors — callers should handle them.
    """
    # Import here so --test mode works without python-telegram-bot installed.
    from telegram import Bot
    from telegram.error import Forbidden, NetworkError, TelegramError

    bot = Bot(token=bot_token)
    chunks = split_message(text)
    sent_count = 0

    for i, chunk in enumerate(chunks):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode="Markdown",
            )
            sent_count += 1

            # Delay between consecutive messages to respect rate limits.
            if i < len(chunks) - 1:
                await asyncio.sleep(SEND_DELAY)

        except Forbidden as exc:
            # Bot lacks permission to post in this chat.
            logger.error(
                "Bot does not have permission to send messages to chat %s. "
                "Ensure the bot is added to the chat/group and has admin "
                "privileges to post messages. Telegram error: %s",
                chat_id,
                exc,
            )
            raise

        except NetworkError as exc:
            # Transient network failure — log and let the orchestrator retry.
            logger.error(
                "Network error while sending message %d/%d to chat %s: %s",
                i + 1,
                len(chunks),
                chat_id,
                exc,
            )
            raise

        except TelegramError as exc:
            # Catch-all for any other Telegram API errors.
            logger.error(
                "Telegram API error while sending message %d/%d to chat %s: %s",
                i + 1,
                len(chunks),
                chat_id,
                exc,
            )
            raise

    return sent_count


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def read_brief() -> Optional[str]:
    """
    Read the brief from .tmp/brief.txt.

    Returns the brief text, or None if the file is missing or empty.
    """
    if not BRIEF_PATH.exists():
        logger.error("Brief file not found at %s", BRIEF_PATH)
        return None

    content = BRIEF_PATH.read_text(encoding="utf-8").strip()
    if not content:
        logger.error("Brief file at %s is empty", BRIEF_PATH)
        return None

    return content


def cleanup_tmp() -> None:
    """Remove the .tmp/ directory and all its contents."""
    if TMP_DIR.exists():
        shutil.rmtree(TMP_DIR)
        logger.info("Cleaned up temporary directory: %s", TMP_DIR)


def run_test_mode() -> None:
    """
    --test mode: print the brief to stdout instead of sending to Telegram.
    Does NOT delete .tmp/.
    """
    brief = read_brief()
    if brief is None:
        # No brief file available — use the built-in sample.
        logger.info(
            "No brief file found; using sample brief for test output."
        )
        brief = SAMPLE_BRIEF

    # Show splitting information so the user can verify chunking logic.
    chunks = split_message(brief)
    print("=" * 60)
    print(f"BRIEF PREVIEW  ({len(brief)} chars, {len(chunks)} message(s))")
    print("=" * 60)

    for i, chunk in enumerate(chunks, start=1):
        if len(chunks) > 1:
            print(f"\n--- Message {i}/{len(chunks)} ({len(chunk)} chars) ---\n")
        print(chunk)

    print("\n" + "=" * 60)
    print("[Test mode] No message sent. .tmp/ directory preserved.")
    print("=" * 60)


async def run_send() -> None:
    """
    Production path: load env vars, read the brief, send to Telegram,
    clean up .tmp/, and log a confirmation summary.
    """
    # Load environment variables from .env file.
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH)

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_OUTPUT_CHAT_ID")

    if not bot_token:
        logger.error(
            "TELEGRAM_BOT_TOKEN is not set. "
            "Add it to your .env file at %s",
            ENV_PATH,
        )
        sys.exit(1)

    if not chat_id:
        logger.error(
            "TELEGRAM_OUTPUT_CHAT_ID is not set. "
            "Add it to your .env file at %s",
            ENV_PATH,
        )
        sys.exit(1)

    # Read the compiled brief.
    brief = read_brief()
    if brief is None:
        # read_brief() already logged the specific error.
        sys.exit(1)

    # Send to Telegram.
    try:
        sent_count = await send_to_telegram(bot_token, chat_id, brief)
    except Exception:
        # Error already logged inside send_to_telegram. Exit without retry —
        # the orchestrator is responsible for retry logic.
        sys.exit(1)

    # Clean up .tmp/ directory after a successful send.
    cleanup_tmp()

    # Log confirmation with details.
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(
        "Brief sent successfully: %d message(s) to chat %s at %s",
        sent_count,
        chat_id,
        timestamp,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send the compiled daily brief to Telegram.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Print the brief to stdout instead of sending to Telegram.",
    )
    args = parser.parse_args()

    if args.test:
        run_test_mode()
    else:
        asyncio.run(run_send())


if __name__ == "__main__":
    main()
