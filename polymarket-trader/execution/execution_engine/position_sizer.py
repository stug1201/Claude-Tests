#!/usr/bin/env python3
"""
position_sizer.py — Kelly criterion position sizing.

Computes optimal position sizes using modified Kelly criterion
with per-strategy fractional Kelly multipliers and portfolio-wide
constraints (max per-position, per-strategy, per-market, total).

Fractional Kelly multipliers:
    S1 (Near-Res):   0.25x Kelly
    S2 (Insider):    0.125x Kelly
    S3 (Glint News): 0.25x Kelly
    S4 (Intra Arb):  0.50x Kelly
    S5 (Cross Arb):  0.50x Kelly, hard cap $500/pair

Usage:
    python execution/execution_engine/position_sizer.py          # Module import
    python execution/execution_engine/position_sizer.py --test   # Unit tests

See: directives/08_execution_engine.md
"""
# TODO: Implementation by Phase 5 subagent
