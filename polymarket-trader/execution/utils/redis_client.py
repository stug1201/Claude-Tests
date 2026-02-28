#!/usr/bin/env python3
"""
redis_client.py — Redis Streams helper functions.

Provides async Redis connection, consumer group management,
stream publishing (XADD), and reading (XREADGROUP).

Streams:
    stream:prices:polymarket    — Raw Polymarket price ticks
    stream:prices:kalshi        — Raw Kalshi price ticks
    stream:prices:normalised    — Unified MarketPrice objects
    stream:arb:signals          — Arb opportunity signals
    stream:insider:alerts       — High-IPS wallet trade events
    stream:news:signals         — Parsed Glint signals
    stream:orders:pending       — Orders queued for execution
    stream:orders:filled        — Execution confirmations

Usage:
    from execution.utils.redis_client import get_redis, publish, consume

Environment variables:
    REDIS_URL — Redis connection string

See: directives/02_unified_price_bus.md
     directives/10_ops_and_deployment.md
"""
# TODO: Implementation by Phase 1 subagent
