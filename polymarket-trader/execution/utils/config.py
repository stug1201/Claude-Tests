#!/usr/bin/env python3
"""
config.py -- Environment variable loader and validation for polymarket-trader.

Loads .env file via python-dotenv (from project root), validates all required
environment variables are present at import time, and provides typed access
to configuration values through a singleton Config class.

Required variables (validated at import time unless --test mode):
    POSTGRES_URL    -- PostgreSQL connection string
    REDIS_URL       -- Redis connection string

Optional variables (accessed via properties or get_optional()):
    POLYMARKET_PRIVATE_KEY, POLYMARKET_API_KEY, POLYMARKET_API_SECRET,
    POLYMARKET_WALLET_ADDRESS, KALSHI_EMAIL, KALSHI_PASSWORD,
    KALSHI_API_KEY, ALCHEMY_API_KEY, TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHANNEL_ID, HASHDIVE_SESSION_COOKIE, ODDPOOL_SESSION_COOKIE

Trading parameters (with defaults):
    INITIAL_BANKROLL    -- Starting bankroll in USD (default: 1000.0)
    MAX_DRAWDOWN_PCT    -- Kill switch drawdown threshold (default: 0.15)

Usage:
    from execution.utils.config import config

    db_url = config.POSTGRES_URL
    kalshi_email = config.get_optional("KALSHI_EMAIL")
    api_key = config.get_required("POLYMARKET_API_KEY")

    python execution/utils/config.py          # Print all config values
    python execution/utils/config.py --test   # Use test defaults (no .env needed)

See: directives/10_ops_and_deployment.md
"""

import argparse
import logging
import os
import sys
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
# Resolve paths relative to the repository root (grandparent of utils/)
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/utils/
PROJECT_ROOT = SCRIPT_DIR.parent.parent               # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
ENV_FILE = PROJECT_ROOT / ".env"

# ---------------------------------------------------------------------------
# Test-mode defaults
# ---------------------------------------------------------------------------
# When running with --test, these values are used so no .env file is needed.
# This keeps test runs self-contained and avoids touching real infrastructure.
_TEST_DEFAULTS: dict[str, str] = {
    "POSTGRES_URL": "postgresql://localhost/polymarket_trader_test",
    "REDIS_URL": "redis://localhost:6379/1",
    "POLYMARKET_PRIVATE_KEY": "test_private_key",
    "POLYMARKET_API_KEY": "test_api_key",
    "POLYMARKET_API_SECRET": "test_api_secret",
    "POLYMARKET_WALLET_ADDRESS": "0x0000000000000000000000000000000000000000",
    "KALSHI_EMAIL": "test@example.com",
    "KALSHI_PASSWORD": "test_password",
    "KALSHI_API_KEY": "test_kalshi_key",
    "ALCHEMY_API_KEY": "test_alchemy_key",
    "TELEGRAM_BOT_TOKEN": "000000000:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "TELEGRAM_CHANNEL_ID": "-1001234567890",
    "INITIAL_BANKROLL": "1000.0",
    "MAX_DRAWDOWN_PCT": "0.15",
    "HASHDIVE_SESSION_COOKIE": "",
    "ODDPOOL_SESSION_COOKIE": "",
}

# Names of environment variables that contain secrets.
# These will be masked when printing config values.
_SECRET_KEYS: set[str] = {
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "KALSHI_PASSWORD",
    "KALSHI_API_KEY",
    "ALCHEMY_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "HASHDIVE_SESSION_COOKIE",
    "ODDPOOL_SESSION_COOKIE",
    "POSTGRES_URL",
    "REDIS_URL",
}

# Variables that are always required in non-test mode.
_REQUIRED_VARS: list[str] = [
    "POSTGRES_URL",
    "REDIS_URL",
]


# ---------------------------------------------------------------------------
# Config class (singleton)
# ---------------------------------------------------------------------------

class Config:
    """
    Centralised configuration for the polymarket-trader application.

    Loads environment variables from .env (via python-dotenv) on initialisation,
    validates that required variables are present, and exposes typed properties
    for every known configuration value.

    In test mode (test_mode=True), all variables receive safe defaults and no
    .env file is required.

    Obtain the singleton instance via the module-level ``config`` variable::

        from execution.utils.config import config
        url = config.POSTGRES_URL
    """

    _instance: Optional["Config"] = None

    def __new__(cls, *args, **kwargs) -> "Config":
        """Ensure only one Config instance exists (singleton pattern)."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, test_mode: bool = False) -> None:
        # Guard against re-initialisation of the singleton.
        if hasattr(self, "_initialised") and self._initialised:
            return
        self._initialised = True
        self._test_mode = test_mode

        # -- Load .env file ------------------------------------------------
        if not test_mode:
            self._load_dotenv()
            self._validate_required()
        else:
            # In test mode, inject defaults into os.environ so that the rest
            # of the code path works identically.
            logger.info("Test mode active -- using built-in defaults.")
            for key, value in _TEST_DEFAULTS.items():
                os.environ.setdefault(key, value)

        logger.info("Config initialised (test_mode=%s).", test_mode)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_dotenv(self) -> None:
        """Load the .env file from the project root using python-dotenv."""
        try:
            from dotenv import load_dotenv  # type: ignore[import-untyped]
        except ImportError:
            logger.warning(
                "python-dotenv is not installed. "
                "Reading environment variables from shell only. "
                "Install with: pip install python-dotenv"
            )
            return

        if ENV_FILE.exists():
            load_dotenv(dotenv_path=ENV_FILE)
            logger.info("Loaded .env from %s", ENV_FILE)
        else:
            logger.warning(
                ".env file not found at %s -- relying on shell environment.",
                ENV_FILE,
            )

    def _validate_required(self) -> None:
        """
        Check that every required environment variable is set.

        Raises SystemExit if any required variable is missing so the
        process fails fast with an actionable error message.
        """
        missing: list[str] = [
            var for var in _REQUIRED_VARS if not os.getenv(var)
        ]
        if missing:
            msg = (
                "Missing required environment variable(s): "
                + ", ".join(missing)
                + ". Set them in your .env file or shell environment. "
                "See .env.example for reference."
            )
            logger.error(msg)
            raise SystemExit(msg)

    # ------------------------------------------------------------------
    # Generic accessors
    # ------------------------------------------------------------------

    def get_required(self, name: str) -> str:
        """
        Return the value of environment variable *name*.

        Raises ValueError if the variable is unset or empty, providing a
        clear message indicating which variable is missing.
        """
        value = os.getenv(name)
        if not value:
            raise ValueError(
                f"Required environment variable '{name}' is not set. "
                "Check your .env file or shell environment."
            )
        return value

    def get_optional(self, name: str) -> Optional[str]:
        """
        Return the value of environment variable *name*, or None if unset.

        Use this for non-critical / Phase-2+ variables that may not be
        configured yet.
        """
        value = os.getenv(name)
        return value if value else None

    # ------------------------------------------------------------------
    # Infrastructure properties
    # ------------------------------------------------------------------

    @property
    def POSTGRES_URL(self) -> str:
        """PostgreSQL connection string (required)."""
        return self.get_required("POSTGRES_URL")

    @property
    def REDIS_URL(self) -> str:
        """Redis connection string (required)."""
        return self.get_required("REDIS_URL")

    @property
    def ALCHEMY_API_KEY(self) -> Optional[str]:
        """Alchemy API key for Polygon RPC access (optional)."""
        return self.get_optional("ALCHEMY_API_KEY")

    # ------------------------------------------------------------------
    # Polymarket properties
    # ------------------------------------------------------------------

    @property
    def POLYMARKET_PRIVATE_KEY(self) -> Optional[str]:
        """Ed25519 private key for Polymarket order signing."""
        return self.get_optional("POLYMARKET_PRIVATE_KEY")

    @property
    def POLYMARKET_API_KEY(self) -> Optional[str]:
        """Polymarket developer portal API key."""
        return self.get_optional("POLYMARKET_API_KEY")

    @property
    def POLYMARKET_API_SECRET(self) -> Optional[str]:
        """Polymarket HMAC request signing secret."""
        return self.get_optional("POLYMARKET_API_SECRET")

    @property
    def POLYMARKET_WALLET_ADDRESS(self) -> Optional[str]:
        """Polymarket wallet address (0x...)."""
        return self.get_optional("POLYMARKET_WALLET_ADDRESS")

    # ------------------------------------------------------------------
    # Kalshi properties
    # ------------------------------------------------------------------

    @property
    def KALSHI_EMAIL(self) -> Optional[str]:
        """Kalshi account email."""
        return self.get_optional("KALSHI_EMAIL")

    @property
    def KALSHI_PASSWORD(self) -> Optional[str]:
        """Kalshi account password."""
        return self.get_optional("KALSHI_PASSWORD")

    @property
    def KALSHI_API_KEY(self) -> Optional[str]:
        """Kalshi API key for enhanced access (optional)."""
        return self.get_optional("KALSHI_API_KEY")

    # ------------------------------------------------------------------
    # Telegram properties
    # ------------------------------------------------------------------

    @property
    def TELEGRAM_BOT_TOKEN(self) -> Optional[str]:
        """Telegram Bot token from @BotFather."""
        return self.get_optional("TELEGRAM_BOT_TOKEN")

    @property
    def TELEGRAM_CHANNEL_ID(self) -> Optional[str]:
        """Numeric Telegram channel ID for Glint alerts."""
        return self.get_optional("TELEGRAM_CHANNEL_ID")

    # ------------------------------------------------------------------
    # Trading configuration properties
    # ------------------------------------------------------------------

    @property
    def INITIAL_BANKROLL(self) -> float:
        """Starting bankroll in USD (default: 1000.0)."""
        raw = os.getenv("INITIAL_BANKROLL", "1000.0")
        try:
            return float(raw)
        except ValueError:
            logger.warning(
                "Invalid INITIAL_BANKROLL value '%s', using default 1000.0.",
                raw,
            )
            return 1000.0

    @property
    def MAX_DRAWDOWN_PCT(self) -> float:
        """Kill switch: halt trading at this 24h drawdown (default: 0.15)."""
        raw = os.getenv("MAX_DRAWDOWN_PCT", "0.15")
        try:
            return float(raw)
        except ValueError:
            logger.warning(
                "Invalid MAX_DRAWDOWN_PCT value '%s', using default 0.15.",
                raw,
            )
            return 0.15

    # ------------------------------------------------------------------
    # Scraping / Phase 2+ properties
    # ------------------------------------------------------------------

    @property
    def HASHDIVE_SESSION_COOKIE(self) -> Optional[str]:
        """HashDive session cookie for wallet scoring (Phase 2+)."""
        return self.get_optional("HASHDIVE_SESSION_COOKIE")

    @property
    def ODDPOOL_SESSION_COOKIE(self) -> Optional[str]:
        """OddPool session cookie for odds scraping (Phase 2+)."""
        return self.get_optional("ODDPOOL_SESSION_COOKIE")

    # ------------------------------------------------------------------
    # Utility properties
    # ------------------------------------------------------------------

    @property
    def test_mode(self) -> bool:
        """Whether the config was initialised in test mode."""
        return self._test_mode

    # ------------------------------------------------------------------
    # Display / debugging
    # ------------------------------------------------------------------

    def _mask(self, key: str, value: Optional[str]) -> str:
        """
        Return a display-safe version of a config value.

        Secrets are masked to their first 4 characters followed by asterisks.
        None values are shown as '<not set>'.
        """
        if value is None or value == "":
            return "<not set>"
        if key in _SECRET_KEYS and len(value) > 4:
            return value[:4] + "****"
        return value

    def print_all(self) -> None:
        """
        Print every known configuration value to stdout.

        Secret values are masked for safe logging / terminal output.
        """
        # Collect all properties that map to env vars.
        # We enumerate them explicitly to keep ordering predictable.
        props: list[tuple[str, str]] = [
            # Infrastructure
            ("POSTGRES_URL", self._mask("POSTGRES_URL", self.POSTGRES_URL)),
            ("REDIS_URL", self._mask("REDIS_URL", self.REDIS_URL)),
            ("ALCHEMY_API_KEY", self._mask("ALCHEMY_API_KEY", self.ALCHEMY_API_KEY)),
            # Polymarket
            ("POLYMARKET_PRIVATE_KEY", self._mask("POLYMARKET_PRIVATE_KEY", self.POLYMARKET_PRIVATE_KEY)),
            ("POLYMARKET_API_KEY", self._mask("POLYMARKET_API_KEY", self.POLYMARKET_API_KEY)),
            ("POLYMARKET_API_SECRET", self._mask("POLYMARKET_API_SECRET", self.POLYMARKET_API_SECRET)),
            ("POLYMARKET_WALLET_ADDRESS", self._mask("POLYMARKET_WALLET_ADDRESS", self.POLYMARKET_WALLET_ADDRESS)),
            # Kalshi
            ("KALSHI_EMAIL", self._mask("KALSHI_EMAIL", self.KALSHI_EMAIL)),
            ("KALSHI_PASSWORD", self._mask("KALSHI_PASSWORD", self.KALSHI_PASSWORD)),
            ("KALSHI_API_KEY", self._mask("KALSHI_API_KEY", self.KALSHI_API_KEY)),
            # Telegram
            ("TELEGRAM_BOT_TOKEN", self._mask("TELEGRAM_BOT_TOKEN", self.TELEGRAM_BOT_TOKEN)),
            ("TELEGRAM_CHANNEL_ID", self._mask("TELEGRAM_CHANNEL_ID", self.TELEGRAM_CHANNEL_ID)),
            # Trading
            ("INITIAL_BANKROLL", str(self.INITIAL_BANKROLL)),
            ("MAX_DRAWDOWN_PCT", str(self.MAX_DRAWDOWN_PCT)),
            # Scraping
            ("HASHDIVE_SESSION_COOKIE", self._mask("HASHDIVE_SESSION_COOKIE", self.HASHDIVE_SESSION_COOKIE)),
            ("ODDPOOL_SESSION_COOKIE", self._mask("ODDPOOL_SESSION_COOKIE", self.ODDPOOL_SESSION_COOKIE)),
        ]

        # Find the longest key name for alignment.
        max_key_len = max(len(k) for k, _ in props)

        print()
        print("=" * 72)
        print("  Polymarket Trader -- Configuration")
        print("  Mode: %s" % ("TEST" if self._test_mode else "LIVE"))
        print("=" * 72)
        for key, display_value in props:
            print(f"  {key:<{max_key_len}}  =  {display_value}")
        print("=" * 72)
        print()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
# The _is_test_mode flag is determined by checking sys.argv for --test.
# This allows `from execution.utils.config import config` to work correctly
# regardless of whether the caller used --test.

def _detect_test_mode() -> bool:
    """
    Check sys.argv for the --test flag.

    This inspection happens at import time so that the singleton is created
    with the correct mode before any other module accesses it.
    """
    return "--test" in sys.argv


# Create the singleton instance. Other modules import this directly:
#   from execution.utils.config import config
config = Config(test_mode=_detect_test_mode())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse --test flag and print all configuration values.

    Useful for verifying environment setup before running the trading system.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Polymarket Trader configuration loader. "
            "Prints all config values (secrets masked) for verification."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use test defaults for all values (no .env file required).",
    )
    args = parser.parse_args()

    # The singleton was already created at import time with the correct mode
    # (since sys.argv is inspected by _detect_test_mode). We just need to
    # verify consistency and print.
    if args.test and not config.test_mode:
        # This should not happen because _detect_test_mode reads sys.argv
        # before argparse runs, but guard against it defensively.
        logger.warning("Re-initialising config in test mode.")
        Config._instance = None
        new_config = Config(test_mode=True)
        new_config.print_all()
        return

    logger.info("Printing current configuration.")
    config.print_all()


if __name__ == "__main__":
    main()
