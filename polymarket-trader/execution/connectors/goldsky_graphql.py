#!/usr/bin/env python3
"""
goldsky_graphql.py — Goldsky GraphQL subgraph connector.

Queries Polymarket order-filled events for wallet trade history.
Used by wallet_profiler for IPS scoring and by S2 insider detection.

Base URL: https://subgraph.goldsky.com
Auth: None

Usage:
    python execution/connectors/goldsky_graphql.py          # Live mode
    python execution/connectors/goldsky_graphql.py --test   # Fixture mode

See: directives/01_data_infrastructure.md
"""
# TODO: Implementation by Phase 2 subagent
