#!/usr/bin/env python3
"""
config.py — Environment variable loader and validation.

Loads .env file via python-dotenv, validates all required environment
variables are present, and provides typed access to configuration values.

Required variables validated at import time:
    POSTGRES_URL, REDIS_URL

Optional variables (validated when accessed):
    POLYMARKET_PRIVATE_KEY, POLYMARKET_API_KEY, POLYMARKET_API_SECRET,
    POLYMARKET_WALLET_ADDRESS, KALSHI_EMAIL, KALSHI_PASSWORD,
    KALSHI_API_KEY, ALCHEMY_API_KEY, TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHANNEL_ID, HASHDIVE_SESSION_COOKIE, ODDPOOL_SESSION_COOKIE

Usage:
    from execution.utils.config import config
    db_url = config.POSTGRES_URL
    kalshi_email = config.get_optional("KALSHI_EMAIL")

See: directives/10_ops_and_deployment.md
"""
# TODO: Implementation by Phase 1 subagent
