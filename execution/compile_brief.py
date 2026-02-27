#!/usr/bin/env python3
"""
compile_brief.py - Compile daily brief from processed summaries.

Reads summary objects from .tmp/summaries.json (output of process_content.py),
sends them to Claude claude-opus-4-6 for synthesis into a single daily brief formatted
for Telegram delivery, and writes the result to .tmp/brief.txt.

The brief follows a strict professional financial analytics tone with
Telegram-compatible Markdown formatting. See the system prompt sent to Claude
for full formatting rules.

Usage:
    python execution/compile_brief.py          # Compile brief from summaries
    python execution/compile_brief.py --test   # Use fixture data, skip API call
"""

import argparse
import json
import logging
import os
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
# Path constants — resolved relative to project root (parent of execution/)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
SUMMARIES_PATH = TMP_DIR / "summaries.json"
BRIEF_PATH = TMP_DIR / "brief.txt"
ENV_PATH = PROJECT_ROOT / ".env"

# ---------------------------------------------------------------------------
# Model — hardcoded per directive. Always use the latest Opus for the final
# deliverable; this step requires the highest-quality synthesis.
# ---------------------------------------------------------------------------
MODEL = "claude-opus-4-6"

# Maximum tokens for the Claude response.
MAX_TOKENS = 8192

# ---------------------------------------------------------------------------
# Fallback messages
# ---------------------------------------------------------------------------
FALLBACK_EMPTY_SUMMARIES = (
    "*Daily Brief*\n\n"
    "No significant content was identified in the monitored channels "
    "over the last 24 hours. Normal monitoring continues."
)

FALLBACK_API_ERROR = (
    "*Daily Brief*\n\n"
    "Brief compilation encountered an error. The editorial team has been "
    "notified. A manual review of source channels is recommended."
)

# ---------------------------------------------------------------------------
# System prompt — contains all brief format instructions from the directive.
# This is the complete set of rules Claude must follow when synthesising
# the summaries into the final brief.
# ---------------------------------------------------------------------------
BRIEF_FORMAT_INSTRUCTIONS = """You are a senior macro analyst compiling a daily morning brief from a set of summarised content items. Produce a single, self-contained brief following these rules exactly.

### Tone
Professional financial analytics — precise, neutral, and authoritative. Write as though this is an internal morning brief distributed at a macro hedge fund. No casual language, no hype, no speculative framing beyond what the source material explicitly supports. Treat price movements, protocol events, and regulatory developments with the same measured register you would apply to equities or fixed income commentary.

### Formatting Rules
- No emojis. None. Not in headers, not inline, not as bullets.
- No exclamation marks.
- Use Telegram-compatible Markdown: bold with *, italic with _ for headers and key terms only. Do not over-format.

### Structure
1. *Market Overview* (opening): 4-6 sentence paragraph synthesising the dominant themes of the past 24 hours at a high level, written in prose. Cover the macro backdrop, digital asset market tone, and any standout moves or events.
2. *Key Developments*: Flag 2-4 highest signal items at the top — items with meaningful market implications, not just interesting news.
3. Thematic Sections (use only those that have material):
   - Macro & Risk
   - Digital Assets
   - DeFi & Protocols
   - Regulation & Policy
   - Notable Reads
4. Each item: 3-5 sentences. Synthesise the data point thoroughly — include context, specific figures, and why it matters. Follow with source link on a new line: Source: [url]
5. Numbers and percentages stated precisely where available (write "BTC declined 4.2% over the session" not "Bitcoin saw a notable drop").
6. Omit any section for which there is no material that day.

### Length
Do not artificially constrain overall length — the brief should be as long as needed to cover every meaningful data point with adequate depth. Prioritise thorough synthesis of each item over brevity. However, do not pad with filler; every sentence should carry informational weight.

### Important
- Do NOT use emojis anywhere in the output.
- Do NOT use exclamation marks anywhere in the output.
- Every item that has a source URL must include it on its own line as: Source: [url]
- If the summaries contain no meaningful content, state clearly that no significant developments were identified in the monitored channels over the last 24 hours."""


# ---------------------------------------------------------------------------
# Fixture data for --test mode
# ---------------------------------------------------------------------------
FIXTURE_SUMMARIES = [
    {
        "source_channel": "MacroInsights",
        "original_url": "https://www.reuters.com/markets/us/fed-holds-rates-steady-2026-02",
        "summary_text": (
            "The Federal Reserve held the federal funds rate steady at 4.25-4.50% "
            "at its February meeting, in line with consensus expectations. Chair "
            "Powell noted that the committee sees progress on disinflation but "
            "remains data-dependent. The dot plot was unchanged, still pointing to "
            "two cuts in the second half of 2026. Treasury yields were largely "
            "unmoved, with the 10Y settling at 4.18%."
        ),
        "content_type": "long_article",
    },
    {
        "source_channel": "CryptoDesk",
        "original_url": "https://www.coindesk.com/markets/btc-etf-flows-feb-2026",
        "summary_text": (
            "Bitcoin spot ETF products saw net inflows of $412M over the past week, "
            "the highest weekly figure since mid-January. BlackRock's IBIT accounted "
            "for roughly 60% of the total. BTC traded at $94,250 at time of "
            "publication, up 2.7% on the week. Ethereum ETFs were mixed, with net "
            "outflows of $38M driven primarily by Grayscale's ETHE redemptions."
        ),
        "content_type": "short_article",
    },
    {
        "source_channel": "DeFiAlpha",
        "original_url": "https://twitter.com/aaboronkov/status/1893627482910",
        "summary_text": (
            "Aave governance passed a proposal to onboard wstETH as collateral on "
            "Aave v3 Base deployment with an LTV of 72.5% and liquidation threshold "
            "of 80%. The proposal received 98.3% approval. This expands Lido's "
            "stETH utility across L2s and may drive additional TVL to Base."
        ),
        "content_type": "tweet",
    },
    {
        "source_channel": "RegWatch",
        "original_url": "https://www.ft.com/content/eu-mica-stablecoin-rules-2026",
        "summary_text": (
            "The European Banking Authority published updated guidance on MiCA "
            "stablecoin reserve requirements, clarifying that issuers of "
            "significant e-money tokens must hold at least 60% of reserves in "
            "EU-domiciled bank deposits. Tether and Circle are both evaluating "
            "compliance structures. The deadline for full compliance is June 2026."
        ),
        "content_type": "long_article",
    },
    {
        "source_channel": "MacroInsights",
        "original_url": None,
        "summary_text": (
            "Chart shared in channel shows the DXY index breaking below its "
            "200-day moving average for the first time since October 2025, "
            "currently trading at 101.4. The weakening dollar has historically "
            "been supportive of risk assets and commodity prices."
        ),
        "content_type": "image",
    },
]

# Sample brief output for --test mode (not sent to the API).
SAMPLE_TEST_BRIEF = """*Market Overview*

The macro backdrop remained largely unchanged following the Federal Reserve's decision to hold rates steady at 4.25-4.50%, with Chair Powell reiterating a data-dependent posture. Risk assets found modest support as the DXY broke below its 200-day moving average for the first time since October 2025, settling at 101.4. In digital assets, Bitcoin spot ETF inflows reached $412M for the week while BTC held above $94,000. On the regulatory front, the EBA issued updated MiCA guidance with implications for major stablecoin issuers.

*Key Developments*

*Fed Holds Rates, Dot Plot Unchanged*
The Federal Reserve maintained the federal funds rate at 4.25-4.50% at its February meeting. The dot plot continues to project two cuts in H2 2026, and 10Y Treasury yields settled at 4.18%, reflecting limited market surprise.
Source: https://www.reuters.com/markets/us/fed-holds-rates-steady-2026-02

*Bitcoin ETF Weekly Inflows Hit $412M*
Spot Bitcoin ETF products recorded their strongest weekly inflows since mid-January, totalling $412M. BlackRock's IBIT drove approximately 60% of the aggregate figure, while Ethereum ETFs posted net outflows of $38M.
Source: https://www.coindesk.com/markets/btc-etf-flows-feb-2026

*Macro & Risk*

*Dollar Weakness Extends*
The DXY index broke below its 200-day moving average, trading at 101.4. This is the first sustained breach of the level since October 2025 and has historically been constructive for risk assets and commodities.

*DeFi & Protocols*

*Aave Onboards wstETH on Base*
Aave governance approved wstETH as collateral on its v3 Base deployment with a 72.5% LTV and 80% liquidation threshold, passing with 98.3% approval. The move extends Lido stETH utility to L2 environments and may attract incremental TVL to the Base ecosystem.
Source: https://twitter.com/aaboronkov/status/1893627482910

*Regulation & Policy*

*EBA Clarifies MiCA Stablecoin Reserve Rules*
The European Banking Authority published updated guidance requiring significant e-money token issuers to maintain at least 60% of reserves in EU-domiciled bank deposits. Tether and Circle are reported to be evaluating compliance structures ahead of the June 2026 deadline.
Source: https://www.ft.com/content/eu-mica-stablecoin-rules-2026"""


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def load_summaries() -> list[dict]:
    """
    Load summary objects from .tmp/summaries.json.

    Returns a list of summary dicts. Returns an empty list if the file
    is missing, empty, or contains invalid JSON (with appropriate logging).
    """
    if not SUMMARIES_PATH.exists():
        logger.error("Summaries file not found at %s", SUMMARIES_PATH)
        return []

    raw = SUMMARIES_PATH.read_text(encoding="utf-8").strip()
    if not raw:
        logger.warning("Summaries file at %s is empty", SUMMARIES_PATH)
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse summaries JSON: %s", exc)
        return []

    if not isinstance(data, list):
        logger.error(
            "Expected a JSON array in summaries.json, got %s", type(data).__name__
        )
        return []

    logger.info("Loaded %d summary item(s) from %s", len(data), SUMMARIES_PATH)
    return data


def build_prompt(summaries: list[dict]) -> str:
    """
    Build the full prompt to send to Claude.

    Combines the format instructions with the serialised summary data.
    If the summaries list is empty, the prompt instructs Claude to produce
    a brief noting that no significant content was found.
    """
    # Start with the format instructions.
    prompt_parts = [BRIEF_FORMAT_INSTRUCTIONS, ""]

    if not summaries:
        # Empty summaries — instruct the model to produce a minimal brief.
        prompt_parts.append(
            "There are no summaries for today. Produce a brief that states "
            "no significant content was identified in the monitored channels "
            "over the last 24 hours. Keep it concise."
        )
    else:
        # Include all summaries as structured data for the model to work with.
        prompt_parts.append(
            f"Below are {len(summaries)} summarised content items from today's "
            "monitored channels. Synthesise them into a single daily brief "
            "following the rules above.\n"
        )
        for i, s in enumerate(summaries, start=1):
            channel = s.get("source_channel", "Unknown")
            url = s.get("original_url") or "N/A"
            text = s.get("summary_text", "").strip()
            content_type = s.get("content_type", "unknown")

            prompt_parts.append(
                f"--- Item {i} ---\n"
                f"Channel: {channel}\n"
                f"Type: {content_type}\n"
                f"URL: {url}\n"
                f"Summary: {text}\n"
            )

    return "\n".join(prompt_parts)


def call_claude(prompt: str) -> Optional[str]:
    """
    Make a single API call to Claude claude-opus-4-6 with the given prompt.

    Returns the model's text response, or None if the call fails or
    returns empty/malformed content.
    """
    # Import anthropic here so --test mode works without the library installed.
    import anthropic

    try:
        client = anthropic.Anthropic()
        logger.info("Sending prompt to %s (%d chars)...", MODEL, len(prompt))

        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )

        # Validate the response structure.
        if not response.content:
            logger.error("Claude returned an empty content list")
            return None

        brief = response.content[0].text

        if not brief or not brief.strip():
            logger.error("Claude returned empty or whitespace-only text")
            return None

        logger.info(
            "Received brief from Claude (%d chars, stop_reason=%s)",
            len(brief),
            response.stop_reason,
        )
        return brief.strip()

    except Exception as exc:
        # Catch all exceptions (network, auth, rate limit, etc.) and log them.
        # The caller will handle the fallback.
        logger.error("Claude API call failed: %s: %s", type(exc).__name__, exc)
        return None


def write_brief(text: str) -> None:
    """
    Write the brief text to .tmp/brief.txt, creating the directory if needed.
    """
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    BRIEF_PATH.write_text(text, encoding="utf-8")
    logger.info("Brief written to %s (%d chars)", BRIEF_PATH, len(text))


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------

def run_test_mode() -> None:
    """
    --test mode: use fixture summaries and write a sample brief to
    .tmp/brief.txt without making any API calls.

    This allows testing the pipeline end-to-end (compile_brief -> send_brief)
    without consuming API credits.
    """
    logger.info("Running in --test mode with %d fixture summaries", len(FIXTURE_SUMMARIES))

    # Show the prompt that would be sent to Claude for debugging.
    prompt = build_prompt(FIXTURE_SUMMARIES)
    logger.info("Generated prompt (%d chars) — skipping API call", len(prompt))

    # Write the pre-written sample brief instead of calling Claude.
    write_brief(SAMPLE_TEST_BRIEF)

    # Print summary to stdout for verification.
    print("=" * 60)
    print("TEST MODE — Fixture summaries used:")
    print("=" * 60)
    for i, s in enumerate(FIXTURE_SUMMARIES, start=1):
        print(f"  {i}. [{s['content_type']}] {s['source_channel']}: "
              f"{s['summary_text'][:80]}...")
    print()
    print(f"Sample brief written to: {BRIEF_PATH}")
    print(f"Brief length: {len(SAMPLE_TEST_BRIEF)} chars")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Production mode
# ---------------------------------------------------------------------------

def run_production() -> None:
    """
    Production path: load .env, read summaries, call Claude, write brief.

    Handles all edge cases: missing env vars, empty summaries, API failures.
    """
    # Load environment variables from .env file.
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ENV_PATH)

    # Validate that the API key is available.
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error(
            "ANTHROPIC_API_KEY is not set. Add it to your .env file at %s",
            ENV_PATH,
        )
        sys.exit(1)

    # Load summaries from the previous pipeline step.
    summaries = load_summaries()

    # Handle the empty summaries edge case. We still call Claude to generate
    # a well-formatted "no content" brief, but if that also fails we use the
    # hardcoded fallback.
    if not summaries:
        logger.warning("No summaries found — generating empty-day brief")

    # Build the prompt and call Claude.
    prompt = build_prompt(summaries)
    brief = call_claude(prompt)

    if brief is not None:
        # Success — write the Claude-generated brief.
        write_brief(brief)
    else:
        # API call failed or returned empty/malformed content.
        # Use the appropriate fallback depending on whether we had summaries.
        if not summaries:
            logger.warning("Using hardcoded fallback for empty summaries")
            write_brief(FALLBACK_EMPTY_SUMMARIES)
        else:
            logger.error(
                "Claude API returned no usable content. Writing fallback brief."
            )
            write_brief(FALLBACK_API_ERROR)

    # Log completion timestamp.
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("Brief compilation completed at %s", timestamp)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compile daily brief from processed summaries.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use fixture data and skip the API call. Writes a sample brief.",
    )
    args = parser.parse_args()

    if args.test:
        run_test_mode()
    else:
        run_production()


if __name__ == "__main__":
    main()
