#!/usr/bin/env python3
"""
kalshi_ws.py — Kalshi WebSocket streaming connector.

Subscribes to live price updates for Kalshi markets and publishes
raw price ticks to stream:prices:kalshi.

Base URL: wss://api.elections.kalshi.com/trade-api/v2/ws
Auth: Same token as REST API (from kalshi_rest.py token management)

Usage:
    python execution/connectors/kalshi_ws.py          # Live mode
    python execution/connectors/kalshi_ws.py --test   # Fixture mode

Environment variables:
    KALSHI_EMAIL     — Kalshi account email
    KALSHI_PASSWORD  — Kalshi account password

See: directives/01_data_infrastructure.md
"""
# TODO: Implementation by Phase 2 subagent
