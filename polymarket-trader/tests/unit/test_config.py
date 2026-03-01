#!/usr/bin/env python3
"""
Unit tests for execution.utils.config -- Config singleton.

Tests cover:
    - Config loads with test_mode=True without .env file
    - Config provides typed access to INITIAL_BANKROLL (float) and MAX_DRAWDOWN_PCT (float)
    - Config.get_optional returns None for unset vars
    - Config.get_required raises ValueError for missing vars

Run:
    python -m unittest tests/unit/test_config.py
"""

import os
import sys
import unittest

# Ensure --test is in sys.argv BEFORE importing the config module.
# The config module creates a singleton at import time and checks
# sys.argv for --test. Without this, the import will fail because
# no .env file with POSTGRES_URL / REDIS_URL is available.
if "--test" not in sys.argv:
    sys.argv.append("--test")

from execution.utils.config import Config  # noqa: E402


class TestConfigTestMode(unittest.TestCase):
    """Tests for Config initialised with test_mode=True."""

    @classmethod
    def setUpClass(cls):
        """Create a fresh Config instance in test mode.

        We need to reset the singleton before each test class to ensure
        we get a clean instance with test_mode=True.
        """
        # Reset the singleton so we can re-initialise in test mode.
        Config._instance = None
        Config._instance = None
        cls.config = Config(test_mode=True)

    @classmethod
    def tearDownClass(cls):
        """Reset the Config singleton after tests so it does not pollute
        other test modules."""
        Config._instance = None

    # ------------------------------------------------------------------
    # Test: Config loads with test_mode=True without .env file
    # ------------------------------------------------------------------

    def test_config_loads_in_test_mode(self):
        """Config should initialise successfully in test mode without a .env file."""
        self.assertTrue(self.config.test_mode)

    def test_config_test_mode_flag(self):
        """The test_mode property should return True when initialised with test_mode=True."""
        self.assertIs(self.config.test_mode, True)

    # ------------------------------------------------------------------
    # Test: Typed access to INITIAL_BANKROLL
    # ------------------------------------------------------------------

    def test_initial_bankroll_is_float(self):
        """INITIAL_BANKROLL should return a float."""
        bankroll = self.config.INITIAL_BANKROLL
        self.assertIsInstance(bankroll, float)

    def test_initial_bankroll_default_value(self):
        """INITIAL_BANKROLL should default to 1000.0 in test mode."""
        self.assertEqual(self.config.INITIAL_BANKROLL, 1000.0)

    def test_initial_bankroll_custom_value(self):
        """INITIAL_BANKROLL should respect an overridden environment variable."""
        original = os.environ.get("INITIAL_BANKROLL")
        try:
            os.environ["INITIAL_BANKROLL"] = "5000.0"
            self.assertEqual(self.config.INITIAL_BANKROLL, 5000.0)
        finally:
            if original is not None:
                os.environ["INITIAL_BANKROLL"] = original
            else:
                os.environ.pop("INITIAL_BANKROLL", None)

    def test_initial_bankroll_invalid_falls_back(self):
        """INITIAL_BANKROLL should fall back to 1000.0 on an invalid value."""
        original = os.environ.get("INITIAL_BANKROLL")
        try:
            os.environ["INITIAL_BANKROLL"] = "not_a_number"
            self.assertEqual(self.config.INITIAL_BANKROLL, 1000.0)
        finally:
            if original is not None:
                os.environ["INITIAL_BANKROLL"] = original
            else:
                os.environ.pop("INITIAL_BANKROLL", None)

    # ------------------------------------------------------------------
    # Test: Typed access to MAX_DRAWDOWN_PCT
    # ------------------------------------------------------------------

    def test_max_drawdown_pct_is_float(self):
        """MAX_DRAWDOWN_PCT should return a float."""
        drawdown = self.config.MAX_DRAWDOWN_PCT
        self.assertIsInstance(drawdown, float)

    def test_max_drawdown_pct_default_value(self):
        """MAX_DRAWDOWN_PCT should default to 0.15 in test mode."""
        self.assertEqual(self.config.MAX_DRAWDOWN_PCT, 0.15)

    def test_max_drawdown_pct_custom_value(self):
        """MAX_DRAWDOWN_PCT should respect an overridden environment variable."""
        original = os.environ.get("MAX_DRAWDOWN_PCT")
        try:
            os.environ["MAX_DRAWDOWN_PCT"] = "0.20"
            self.assertEqual(self.config.MAX_DRAWDOWN_PCT, 0.20)
        finally:
            if original is not None:
                os.environ["MAX_DRAWDOWN_PCT"] = original
            else:
                os.environ.pop("MAX_DRAWDOWN_PCT", None)

    def test_max_drawdown_pct_invalid_falls_back(self):
        """MAX_DRAWDOWN_PCT should fall back to 0.15 on an invalid value."""
        original = os.environ.get("MAX_DRAWDOWN_PCT")
        try:
            os.environ["MAX_DRAWDOWN_PCT"] = "abc"
            self.assertEqual(self.config.MAX_DRAWDOWN_PCT, 0.15)
        finally:
            if original is not None:
                os.environ["MAX_DRAWDOWN_PCT"] = original
            else:
                os.environ.pop("MAX_DRAWDOWN_PCT", None)

    # ------------------------------------------------------------------
    # Test: get_optional returns None for unset vars
    # ------------------------------------------------------------------

    def test_get_optional_returns_none_for_unset(self):
        """get_optional should return None for a variable that is not set."""
        # Use a variable name that is guaranteed not to exist.
        result = self.config.get_optional("TOTALLY_NONEXISTENT_VAR_12345")
        self.assertIsNone(result)

    def test_get_optional_returns_none_for_empty(self):
        """get_optional should return None for a variable that is set to empty string."""
        original = os.environ.get("EMPTY_TEST_VAR")
        try:
            os.environ["EMPTY_TEST_VAR"] = ""
            result = self.config.get_optional("EMPTY_TEST_VAR")
            self.assertIsNone(result)
        finally:
            if original is not None:
                os.environ["EMPTY_TEST_VAR"] = original
            else:
                os.environ.pop("EMPTY_TEST_VAR", None)

    def test_get_optional_returns_value_when_set(self):
        """get_optional should return the value when the variable is set."""
        original = os.environ.get("TEST_OPT_VAR")
        try:
            os.environ["TEST_OPT_VAR"] = "some_value"
            result = self.config.get_optional("TEST_OPT_VAR")
            self.assertEqual(result, "some_value")
        finally:
            if original is not None:
                os.environ["TEST_OPT_VAR"] = original
            else:
                os.environ.pop("TEST_OPT_VAR", None)

    # ------------------------------------------------------------------
    # Test: get_required raises ValueError for missing vars
    # ------------------------------------------------------------------

    def test_get_required_raises_for_missing(self):
        """get_required should raise ValueError for a variable that is not set."""
        with self.assertRaises(ValueError) as ctx:
            self.config.get_required("COMPLETELY_MISSING_VAR_99999")
        self.assertIn("COMPLETELY_MISSING_VAR_99999", str(ctx.exception))

    def test_get_required_raises_for_empty(self):
        """get_required should raise ValueError for a variable set to empty string."""
        original = os.environ.get("EMPTY_REQUIRED_VAR")
        try:
            os.environ["EMPTY_REQUIRED_VAR"] = ""
            with self.assertRaises(ValueError):
                self.config.get_required("EMPTY_REQUIRED_VAR")
        finally:
            if original is not None:
                os.environ["EMPTY_REQUIRED_VAR"] = original
            else:
                os.environ.pop("EMPTY_REQUIRED_VAR", None)

    def test_get_required_returns_value_when_set(self):
        """get_required should return the value when the variable is set."""
        # POSTGRES_URL is set by test defaults.
        result = self.config.get_required("POSTGRES_URL")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    # ------------------------------------------------------------------
    # Test: properties backed by test defaults
    # ------------------------------------------------------------------

    def test_postgres_url_available_in_test_mode(self):
        """POSTGRES_URL should be available in test mode from test defaults."""
        url = self.config.POSTGRES_URL
        self.assertIsInstance(url, str)
        self.assertIn("postgresql://", url)

    def test_redis_url_available_in_test_mode(self):
        """REDIS_URL should be available in test mode from test defaults."""
        url = self.config.REDIS_URL
        self.assertIsInstance(url, str)
        self.assertIn("redis://", url)

    def test_optional_properties_return_strings_in_test_mode(self):
        """Optional properties with test defaults should return non-None strings."""
        # These have non-empty defaults in _TEST_DEFAULTS.
        self.assertIsNotNone(self.config.POLYMARKET_API_KEY)
        self.assertIsNotNone(self.config.KALSHI_EMAIL)
        self.assertIsNotNone(self.config.TELEGRAM_BOT_TOKEN)

    def test_mask_hides_secrets(self):
        """_mask should truncate secret values to first 4 chars + asterisks."""
        masked = self.config._mask("POLYMARKET_API_KEY", "test_api_key")
        self.assertEqual(masked, "test****")

    def test_mask_shows_not_set_for_none(self):
        """_mask should return '<not set>' for None values."""
        masked = self.config._mask("SOME_KEY", None)
        self.assertEqual(masked, "<not set>")

    def test_mask_shows_not_set_for_empty(self):
        """_mask should return '<not set>' for empty string values."""
        masked = self.config._mask("SOME_KEY", "")
        self.assertEqual(masked, "<not set>")


class TestConfigSingleton(unittest.TestCase):
    """Tests for the singleton pattern of Config."""

    @classmethod
    def setUpClass(cls):
        Config._instance = None
        cls.config1 = Config(test_mode=True)

    @classmethod
    def tearDownClass(cls):
        Config._instance = None

    def test_singleton_returns_same_instance(self):
        """Config() should return the same instance on repeated calls."""
        config2 = Config(test_mode=True)
        self.assertIs(self.config1, config2)


if __name__ == "__main__":
    unittest.main()
