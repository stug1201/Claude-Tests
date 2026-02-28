#!/usr/bin/env python3
"""
kalshi_rest.py — Kalshi REST API connector.

Handles RSA-PSS token authentication with 30-minute TTL,
proactive token refresh at 25-minute mark, market queries,
and order placement.

Base URL: https://api.elections.kalshi.com/trade-api/v2
Auth: RSA-PSS token (POST /login with email + password)

CRITICAL: Token expires every 30 minutes. Must refresh at 25min mark.
All requests need Authorization: Bearer <token>.

Usage:
    python execution/connectors/kalshi_rest.py          # Live mode
    python execution/connectors/kalshi_rest.py --test   # Fixture mode

Environment variables:
    KALSHI_EMAIL     — Kalshi account email
    KALSHI_PASSWORD  — Kalshi account password
    KALSHI_API_KEY   — Kalshi API key

See: directives/01_data_infrastructure.md
"""
# TODO: Implementation by Phase 2 subagent
