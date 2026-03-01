#!/usr/bin/env python3
"""
glint_telegram.py -- Glint Telegram feed parser for polymarket-trader.

Listens to the Glint alert Telegram channel via the python-telegram-bot
library, parses structured alert messages into normalised signal dicts,
and publishes each parsed signal to the stream:news:signals Redis Stream.

Glint alert messages follow this format:

    ALERT [SIGNAL_TYPE] IMPACT_LEVEL
    Headline text describing the event
    Contracts: 0xabc123..., 0xdef456..., Market Title Here
    Relevance: 0.85

    Optional body text with additional context...

Parsed signal schema (published to Redis):

    {
        "timestamp":         ISO-8601 datetime string,
        "signal_type":       "news_break" | "data_release" | "political" |
                             "legal" | "market_move" | "rumor",
        "matched_contracts": ["0x...", "Market Title"],
        "impact_level":      "HIGH" | "MEDIUM" | "LOW",
        "relevance_score":   float 0.0-1.0,
        "headline":          str,
        "raw_text":          str (full message text)
    }

Auth:
    TELEGRAM_BOT_TOKEN   -- Bot token from @BotFather
    TELEGRAM_CHANNEL_ID  -- Numeric channel ID for the Glint alert channel

Usage:
    python execution/connectors/glint_telegram.py          # Live listener
    python execution/connectors/glint_telegram.py --test   # Process fixtures

Environment variables (via execution.utils.config):
    TELEGRAM_BOT_TOKEN   -- Telegram bot token
    TELEGRAM_CHANNEL_ID  -- Glint alert channel ID

See: directives/01_data_infrastructure.md
     directives/05_strategy_s3_glint_news.md
"""

import argparse
import asyncio
import json
import logging
import re
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
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"

# Ensure project root is on sys.path so `execution.utils.*` imports work
# when this script is invoked directly (python execution/connectors/...).
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "glint_alerts.json"

# ---------------------------------------------------------------------------
# Redis stream constants
# ---------------------------------------------------------------------------
STREAM_NEWS_SIGNALS = "stream:news:signals"

# ---------------------------------------------------------------------------
# Reconnection configuration
# ---------------------------------------------------------------------------
_RECONNECT_BACKOFF_BASE = 1       # seconds
_RECONNECT_BACKOFF_MAX = 60       # seconds
_RECONNECT_MAX_RETRIES = 10       # per reconnection cycle

# ---------------------------------------------------------------------------
# Regex patterns for parsing Glint alert messages
# ---------------------------------------------------------------------------
# Main header line: ALERT [SIGNAL_TYPE] IMPACT_LEVEL
_RE_ALERT_HEADER = re.compile(
    r"^ALERT\s+\[([A-Z_]+)\]\s+(HIGH|MEDIUM|LOW)\s*$",
    re.MULTILINE,
)

# Contracts line: Contracts: 0xabc..., 0xdef..., Market Title Here
_RE_CONTRACTS = re.compile(
    r"^Contracts:\s*(.+)$",
    re.MULTILINE,
)

# Relevance line: Relevance: 0.85
_RE_RELEVANCE = re.compile(
    r"^Relevance:\s*([\d.]+)\s*$",
    re.MULTILINE,
)

# ---------------------------------------------------------------------------
# Signal type normalisation map
# ---------------------------------------------------------------------------
# Glint uses uppercase identifiers; we normalise to the schema defined in
# directive 05_strategy_s3_glint_news.md.
_SIGNAL_TYPE_MAP: dict[str, str] = {
    "NEWS_BREAK":    "news_break",
    "NEWS":          "news_break",
    "DATA_RELEASE":  "data_release",
    "DATA":          "data_release",
    "POLITICAL":     "political",
    "LEGAL":         "legal",
    "MARKET_MOVE":   "market_move",
    "RUMOR":         "rumor",
    "RUMOUR":        "rumor",
}

# Valid impact levels (already uppercase in the regex).
_VALID_IMPACT_LEVELS = {"HIGH", "MEDIUM", "LOW"}


# ---------------------------------------------------------------------------
# GlintTelegramParser class
# ---------------------------------------------------------------------------

class GlintTelegramParser:
    """
    Parses structured alert messages from Glint's Telegram channel and
    publishes normalised signals to Redis Streams.

    Attributes:
        bot_token:   Telegram Bot API token (from @BotFather).
        channel_id:  Numeric Telegram channel ID for Glint alerts.
    """

    def __init__(self, bot_token: str, channel_id: str) -> None:
        """
        Initialise the parser with Telegram credentials.

        Args:
            bot_token:   Telegram bot token string.
            channel_id:  Telegram channel ID (numeric string, e.g. "-100...").
        """
        self.bot_token = bot_token
        self.channel_id = channel_id
        self._parse_count = 0
        self._skip_count = 0
        logger.info(
            "GlintTelegramParser initialised (channel_id=%s).", channel_id
        )

    # ------------------------------------------------------------------
    # Message parsing
    # ------------------------------------------------------------------

    def parse_alert(self, message_text: str, timestamp: Optional[datetime] = None) -> Optional[dict]:
        """
        Extract structured fields from a Glint alert message.

        Args:
            message_text:  Raw text content of the Telegram message.
            timestamp:     Message timestamp. Defaults to now (UTC) if not
                           provided.

        Returns:
            A dict matching the Glint signal schema if the message is a
            valid alert, or None if the message is not parseable (e.g. a
            casual/non-alert message).

        The returned dict contains:
            - signal_type:       str (e.g. "news_break", "data_release")
            - matched_contracts: list[str] -- market IDs or titles
            - impact_level:      str ("HIGH", "MEDIUM", "LOW")
            - relevance_score:   float (0.0 - 1.0)
            - headline:          str -- first line after the header
            - raw_text:          str -- full original message
            - timestamp:         str -- ISO-8601 formatted datetime
        """
        if not message_text or not message_text.strip():
            logger.debug("Skipping empty message.")
            self._skip_count += 1
            return None

        text = message_text.strip()

        # -- Step 1: Match the alert header ------------------------------------
        header_match = _RE_ALERT_HEADER.search(text)
        if not header_match:
            logger.debug(
                "Message does not match alert format (no header). "
                "Skipping: %.80s...",
                text.replace("\n", " "),
            )
            self._skip_count += 1
            return None

        raw_signal_type = header_match.group(1)
        raw_impact_level = header_match.group(2)

        # -- Step 2: Normalise signal type -------------------------------------
        signal_type = _SIGNAL_TYPE_MAP.get(raw_signal_type)
        if signal_type is None:
            logger.warning(
                "Unknown signal type '%s'. Treating as 'news_break'.",
                raw_signal_type,
            )
            signal_type = "news_break"

        # -- Step 3: Validate impact level -------------------------------------
        impact_level = raw_impact_level.upper()
        if impact_level not in _VALID_IMPACT_LEVELS:
            logger.warning(
                "Invalid impact level '%s'. Defaulting to 'LOW'.",
                raw_impact_level,
            )
            impact_level = "LOW"

        # -- Step 4: Extract headline ------------------------------------------
        # The headline is the line immediately after the ALERT header.
        lines = text.split("\n")
        headline = ""
        header_line_idx = None
        for idx, line in enumerate(lines):
            if _RE_ALERT_HEADER.match(line.strip()):
                header_line_idx = idx
                break

        if header_line_idx is not None and header_line_idx + 1 < len(lines):
            headline = lines[header_line_idx + 1].strip()

        if not headline:
            logger.warning(
                "No headline found after alert header. Using first 100 chars."
            )
            headline = text[:100]

        # -- Step 5: Extract matched contracts ---------------------------------
        matched_contracts: list[str] = []
        contracts_match = _RE_CONTRACTS.search(text)
        if contracts_match:
            raw_contracts = contracts_match.group(1)
            # Split on commas, strip whitespace, filter empties.
            for contract in raw_contracts.split(","):
                contract = contract.strip()
                if contract:
                    matched_contracts.append(contract)
        else:
            logger.debug("No 'Contracts:' line found in alert message.")

        # -- Step 6: Extract relevance score -----------------------------------
        relevance_score = 0.5  # default if not specified
        relevance_match = _RE_RELEVANCE.search(text)
        if relevance_match:
            try:
                raw_score = float(relevance_match.group(1))
                # Clamp to [0.0, 1.0].
                relevance_score = max(0.0, min(1.0, raw_score))
            except ValueError:
                logger.warning(
                    "Could not parse relevance score '%s'. Using default 0.5.",
                    relevance_match.group(1),
                )
                relevance_score = 0.5
        else:
            logger.debug(
                "No 'Relevance:' line found. Using default score 0.5."
            )

        # -- Step 7: Determine timestamp ---------------------------------------
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

        # -- Build signal dict -------------------------------------------------
        signal = {
            "timestamp": ts_str,
            "signal_type": signal_type,
            "matched_contracts": matched_contracts,
            "impact_level": impact_level,
            "relevance_score": relevance_score,
            "headline": headline,
            "raw_text": text,
        }

        self._parse_count += 1
        logger.info(
            "Parsed signal #%d: type=%s impact=%s relevance=%.2f contracts=%d headline=%.60s",
            self._parse_count,
            signal_type,
            impact_level,
            relevance_score,
            len(matched_contracts),
            headline,
        )

        return signal

    # ------------------------------------------------------------------
    # Redis publishing
    # ------------------------------------------------------------------

    async def _publish_signal(self, signal: dict) -> str:
        """
        Publish a parsed signal dict to the stream:news:signals Redis Stream.

        Args:
            signal: Parsed signal dict from parse_alert().

        Returns:
            The Redis message ID assigned to the published entry.
        """
        # Late import to avoid import-time dependency on redis_client when
        # running in fixture/test mode or when module is imported for parsing
        # only.
        from execution.utils.redis_client import publish

        msg_id = await publish(STREAM_NEWS_SIGNALS, signal)
        logger.info(
            "Published signal to %s: id=%s type=%s",
            STREAM_NEWS_SIGNALS,
            msg_id,
            signal.get("signal_type", "unknown"),
        )
        return msg_id

    # ------------------------------------------------------------------
    # Live listener (python-telegram-bot)
    # ------------------------------------------------------------------

    async def run_listener(self) -> None:
        """
        Main loop: listen for new messages in the Glint Telegram channel
        and publish parsed signals to Redis.

        Uses python-telegram-bot's Application + polling mechanism. Each
        incoming message is parsed via parse_alert(); valid signals are
        published to stream:news:signals.

        Implements reconnection with exponential backoff on connection
        drops (up to _RECONNECT_MAX_RETRIES attempts per cycle, with
        backoff capped at _RECONNECT_BACKOFF_MAX seconds).
        """
        # Late import so --test mode never requires the telegram package.
        try:
            from telegram import Update
            from telegram.ext import (
                Application,
                ContextTypes,
                MessageHandler,
                filters,
            )
        except ImportError:
            logger.error(
                "python-telegram-bot is not installed. "
                "Install with: pip install python-telegram-bot"
            )
            sys.exit(1)

        target_channel_id = int(self.channel_id)
        parser_self = self  # Capture for use in the nested handler.

        async def _handle_message(
            update: Update, context: ContextTypes.DEFAULT_TYPE
        ) -> None:
            """Handler invoked for each new channel post."""
            message = update.channel_post or update.message
            if message is None:
                logger.debug("Received update with no message. Skipping.")
                return

            # Only process messages from the target channel.
            if message.chat_id != target_channel_id:
                logger.debug(
                    "Ignoring message from chat_id=%s (expected %s).",
                    message.chat_id,
                    target_channel_id,
                )
                return

            text = message.text or message.caption or ""
            if not text.strip():
                logger.debug(
                    "Empty message from channel %s. Skipping.",
                    target_channel_id,
                )
                return

            # Parse the message timestamp.
            msg_ts = message.date
            if msg_ts and msg_ts.tzinfo is None:
                msg_ts = msg_ts.replace(tzinfo=timezone.utc)

            # Parse and publish.
            signal = parser_self.parse_alert(text, timestamp=msg_ts)
            if signal is not None:
                try:
                    await parser_self._publish_signal(signal)
                except Exception as pub_exc:
                    logger.error(
                        "Failed to publish signal to Redis: %s", pub_exc
                    )

        # -- Build and run the Application with reconnection -------------------
        backoff = _RECONNECT_BACKOFF_BASE
        attempt = 0

        while attempt < _RECONNECT_MAX_RETRIES:
            attempt += 1
            try:
                logger.info(
                    "Starting Telegram listener (attempt %d/%d) for channel %s...",
                    attempt,
                    _RECONNECT_MAX_RETRIES,
                    self.channel_id,
                )

                app = (
                    Application.builder()
                    .token(self.bot_token)
                    .build()
                )

                # Register handler for channel posts (text messages).
                app.add_handler(
                    MessageHandler(
                        filters.ChatType.CHANNEL & (filters.TEXT | filters.CAPTION),
                        _handle_message,
                    )
                )

                # Also register for regular messages in case the bot receives
                # forwarded messages from the channel.
                app.add_handler(
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        _handle_message,
                    )
                )

                # run_polling blocks until the application is stopped.
                # allowed_updates ensures we receive channel_post updates.
                await app.initialize()
                await app.start()
                await app.updater.start_polling(
                    allowed_updates=["channel_post", "message"],
                    drop_pending_updates=True,
                )

                logger.info(
                    "Telegram listener running. Waiting for messages in channel %s...",
                    self.channel_id,
                )

                # Reset backoff on successful start.
                backoff = _RECONNECT_BACKOFF_BASE
                attempt = 0

                # Keep running until interrupted.
                try:
                    while True:
                        await asyncio.sleep(1)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    logger.info("Listener interrupted. Shutting down...")
                finally:
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()

                # If we reach here cleanly (no exception), break the loop.
                break

            except (KeyboardInterrupt, asyncio.CancelledError):
                logger.info("Listener cancelled. Exiting.")
                break

            except Exception as exc:
                logger.error(
                    "Telegram listener error (attempt %d/%d): %s. "
                    "Reconnecting in %d s...",
                    attempt,
                    _RECONNECT_MAX_RETRIES,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX)

        if attempt >= _RECONNECT_MAX_RETRIES:
            logger.error(
                "Exhausted %d reconnection attempts. Giving up.",
                _RECONNECT_MAX_RETRIES,
            )
            sys.exit(1)

        # -- Final stats -------------------------------------------------------
        logger.info(
            "Listener stopped. Parsed: %d signals, Skipped: %d messages.",
            self._parse_count,
            self._skip_count,
        )


# ---------------------------------------------------------------------------
# Test / fixture mode
# ---------------------------------------------------------------------------

async def _run_fixture_test() -> None:
    """
    Load fixture alert messages from .tmp/fixtures/glint_alerts.json,
    parse each through GlintTelegramParser.parse_alert(), and publish
    valid signals to Redis (using the InMemoryRedis mock in --test mode).

    Prints a summary of parsed vs skipped messages.
    """
    # Ensure fixture file exists.
    if not FIXTURE_FILE.exists():
        logger.error(
            "Fixture file not found: %s. "
            "Create it with sample Glint alert messages.",
            FIXTURE_FILE,
        )
        sys.exit(1)

    # Load fixture data.
    raw = FIXTURE_FILE.read_text(encoding="utf-8")
    try:
        fixtures: list[dict] = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in fixture file: %s", exc)
        sys.exit(1)

    logger.info(
        "Loaded %d fixture messages from %s.", len(fixtures), FIXTURE_FILE
    )

    # Enable mock Redis.
    import execution.utils.redis_client as redis_mod
    redis_mod._use_mock = True

    # Ensure consumer groups exist on the mock.
    from execution.utils.redis_client import ensure_consumer_groups
    await ensure_consumer_groups()

    # Initialise parser with test credentials.
    from execution.utils.config import config
    parser = GlintTelegramParser(
        bot_token=config.TELEGRAM_BOT_TOKEN or "test_token",
        channel_id=config.TELEGRAM_CHANNEL_ID or "-1001234567890",
    )

    # Process each fixture message.
    published_count = 0

    print()
    print("=" * 72)
    print("  Glint Telegram Parser -- Fixture Test")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    for idx, fixture in enumerate(fixtures):
        msg_text = fixture.get("text", "")
        msg_date_str = fixture.get("date")
        msg_id = fixture.get("message_id", idx + 1)

        # Parse timestamp from fixture.
        msg_ts: Optional[datetime] = None
        if msg_date_str:
            try:
                msg_ts = datetime.fromisoformat(
                    msg_date_str.replace("Z", "+00:00")
                )
            except ValueError:
                logger.warning(
                    "Could not parse fixture timestamp '%s'. Using now().",
                    msg_date_str,
                )

        print(f"\n  --- Fixture message #{msg_id} ---")
        print(f"  Text: {msg_text[:80]}{'...' if len(msg_text) > 80 else ''}")

        signal = parser.parse_alert(msg_text, timestamp=msg_ts)

        if signal is None:
            print("  Result: SKIPPED (not a valid alert)")
            continue

        # Publish to mock Redis.
        try:
            redis_msg_id = await parser._publish_signal(signal)
            published_count += 1
            print(f"  Result: PUBLISHED (redis_id={redis_msg_id})")
            print(f"    signal_type:       {signal['signal_type']}")
            print(f"    impact_level:      {signal['impact_level']}")
            print(f"    relevance_score:   {signal['relevance_score']}")
            print(f"    matched_contracts: {signal['matched_contracts']}")
            print(f"    headline:          {signal['headline'][:60]}")
        except Exception as exc:
            print(f"  Result: PUBLISH FAILED ({exc})")
            logger.error("Failed to publish fixture signal: %s", exc)

    # -- Verify stream contents ------------------------------------------------
    from execution.utils.redis_client import stream_len, close_redis
    length = await stream_len(STREAM_NEWS_SIGNALS)

    print()
    print("-" * 72)
    print(f"  Summary:")
    print(f"    Total fixtures:  {len(fixtures)}")
    print(f"    Parsed:          {parser._parse_count}")
    print(f"    Skipped:         {parser._skip_count}")
    print(f"    Published:       {published_count}")
    print(f"    Stream length:   {length}")
    print("=" * 72)
    print()

    await close_redis()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point for the Glint Telegram feed parser.

    Modes:
        --test   Process fixture alert messages from .tmp/fixtures/ using
                 the InMemoryRedis mock. No network calls are made.

        (default) Start the live Telegram listener that watches the Glint
                  alert channel and publishes parsed signals to Redis.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Glint Telegram feed parser. Listens to the Glint alert "
            "Telegram channel, parses structured alert messages, and "
            "publishes signals to stream:news:signals."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Process fixture alert messages from .tmp/fixtures/ "
            "instead of connecting to Telegram. Uses mock Redis."
        ),
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode: processing fixture data.")
        asyncio.run(_run_fixture_test())
    else:
        logger.info("Running in live mode: starting Telegram listener.")

        # Load credentials from config.
        from execution.utils.config import config

        bot_token = config.TELEGRAM_BOT_TOKEN
        channel_id = config.TELEGRAM_CHANNEL_ID

        if not bot_token:
            logger.error(
                "TELEGRAM_BOT_TOKEN is not set. "
                "Configure it in .env or environment."
            )
            sys.exit(1)

        if not channel_id:
            logger.error(
                "TELEGRAM_CHANNEL_ID is not set. "
                "Configure it in .env or environment."
            )
            sys.exit(1)

        glint_parser = GlintTelegramParser(
            bot_token=bot_token,
            channel_id=channel_id,
        )
        asyncio.run(glint_parser.run_listener())


if __name__ == "__main__":
    main()
