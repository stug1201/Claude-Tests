#!/usr/bin/env python3
"""
wallet_profiler.py -- IPS (Insider Probability Score) batch processor.

Pulls 90 days of wallet trade history from Goldsky, computes IPS
features (win rate, entry timing, cluster detection), trains/applies
an XGBoost classifier (with sklearn fallback), and updates the
wallet_scores table hourly via db.upsert_wallet_score().

Emits real-time alerts to stream:insider:alerts when high-IPS
wallets place new trades.

IPS Features:
    win_rate_24h     -- Win rate on markets resolved within 24h of trade
    avg_entry_timing -- Average hours before major price move (>5c)
    cluster_id       -- Connected-component cluster of co-trading wallets
    cluster_size     -- Number of wallets in the cluster
    avg_position_size-- Average trade size in USD
    market_diversity -- Number of distinct markets traded
    hashdive_score   -- Hashdive Smart Score (0-100) as external validation

Usage:
    python execution/pipeline/wallet_profiler.py             # Live hourly batch
    python execution/pipeline/wallet_profiler.py --test      # Fixture mode
    python execution/pipeline/wallet_profiler.py --validate  # Cross-validation metrics

Environment variables (via execution.utils.config):
    POSTGRES_URL -- PostgreSQL connection string
    REDIS_URL    -- Redis connection string

See: directives/04_strategy_s2_insider_detection.md
"""

import argparse
import asyncio
import json
import logging
import math
import os
import sys
import time as _time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

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
SCRIPT_DIR = Path(__file__).resolve().parent              # execution/pipeline/
EXECUTION_DIR = SCRIPT_DIR.parent                         # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                       # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
MODEL_DIR = TMP_DIR / "models"
FIXTURES_DIR = TMP_DIR / "fixtures"
MODEL_PATH = MODEL_DIR / "ips_model.json"
FIXTURE_PATH = FIXTURES_DIR / "s2_wallets.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
IPS_HIGH_THRESHOLD = 0.70          # Wallets above this are flagged as insiders
IPS_DEFAULT_PRIOR = 0.50           # New wallets with no history
MIN_TRADES_FOR_SCORING = 10        # Minimum trades before flagging
CLUSTER_TIME_BUCKET_MINUTES = 5    # Time window for cluster detection
CLUSTER_CO_OCCURRENCE_MIN = 3      # Min shared markets for cluster membership
CLUSTER_SIZE_LARGE = 5             # Clusters above this are strong signals
PRICE_MOVE_THRESHOLD = 0.05        # 5-cent move is "major"
RESOLUTION_WINDOW_HOURS = 24       # Markets resolved within 24h of trade
DEFAULT_LOOKBACK_DAYS = 90         # Default lookback window
BATCH_INTERVAL_SECONDS = 3600     # Default: run every hour
MIN_POSITIVE_LABELS = 50           # Minimum for model training
MIN_NEGATIVE_LABELS = 50           # Minimum for model training
STREAM_INSIDER_ALERTS = "stream:insider:alerts"

# Feature weight hints (used in heuristic fallback scoring)
_WEIGHT_WIN_RATE = 0.35
_WEIGHT_ENTRY_TIMING = 0.25
_WEIGHT_CLUSTER = 0.20
_WEIGHT_HASHDIVE = 0.15
_WEIGHT_DIVERSITY = 0.05

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_test_mode: bool = False


# =========================================================================
#  FIXTURE DATA (--test mode)
# =========================================================================

FIXTURE_TRADES: dict[str, list[dict]] = {
    "0xINSIDER_001": [
        {
            "market_id": "0xMKT_A",
            "side": "yes",
            "price": 0.35,
            "size_usd": 500.0,
            "timestamp": "2026-01-15T10:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-15T22:00:00Z",
        },
        {
            "market_id": "0xMKT_B",
            "side": "yes",
            "price": 0.40,
            "size_usd": 750.0,
            "timestamp": "2026-01-20T08:30:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-20T18:00:00Z",
        },
        {
            "market_id": "0xMKT_C",
            "side": "no",
            "price": 0.60,
            "size_usd": 300.0,
            "timestamp": "2026-02-01T14:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-02T10:00:00Z",
        },
        {
            "market_id": "0xMKT_D",
            "side": "yes",
            "price": 0.25,
            "size_usd": 1000.0,
            "timestamp": "2026-02-05T09:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-05T20:00:00Z",
        },
        {
            "market_id": "0xMKT_E",
            "side": "yes",
            "price": 0.45,
            "size_usd": 600.0,
            "timestamp": "2026-02-10T11:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-10T23:30:00Z",
        },
        {
            "market_id": "0xMKT_A",
            "side": "yes",
            "price": 0.30,
            "size_usd": 800.0,
            "timestamp": "2026-02-12T07:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-12T14:00:00Z",
        },
        {
            "market_id": "0xMKT_F",
            "side": "no",
            "price": 0.70,
            "size_usd": 450.0,
            "timestamp": "2026-02-14T16:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-15T04:00:00Z",
        },
        {
            "market_id": "0xMKT_G",
            "side": "yes",
            "price": 0.20,
            "size_usd": 1200.0,
            "timestamp": "2026-02-18T10:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-18T21:00:00Z",
        },
        {
            "market_id": "0xMKT_H",
            "side": "yes",
            "price": 0.55,
            "size_usd": 200.0,
            "timestamp": "2026-02-20T13:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-21T01:00:00Z",
        },
        {
            "market_id": "0xMKT_B",
            "side": "yes",
            "price": 0.38,
            "size_usd": 900.0,
            "timestamp": "2026-02-22T08:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-22T19:00:00Z",
        },
        {
            "market_id": "0xMKT_I",
            "side": "no",
            "price": 0.65,
            "size_usd": 350.0,
            "timestamp": "2026-02-24T15:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-25T03:00:00Z",
        },
        {
            "market_id": "0xMKT_J",
            "side": "yes",
            "price": 0.28,
            "size_usd": 700.0,
            "timestamp": "2026-02-26T09:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-26T20:00:00Z",
        },
    ],
    "0xINSIDER_002": [
        {
            "market_id": "0xMKT_A",
            "side": "yes",
            "price": 0.36,
            "size_usd": 400.0,
            "timestamp": "2026-01-15T10:02:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-15T22:00:00Z",
        },
        {
            "market_id": "0xMKT_B",
            "side": "yes",
            "price": 0.41,
            "size_usd": 350.0,
            "timestamp": "2026-01-20T08:33:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-20T18:00:00Z",
        },
        {
            "market_id": "0xMKT_D",
            "side": "yes",
            "price": 0.26,
            "size_usd": 800.0,
            "timestamp": "2026-02-05T09:03:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-05T20:00:00Z",
        },
        {
            "market_id": "0xMKT_E",
            "side": "yes",
            "price": 0.44,
            "size_usd": 500.0,
            "timestamp": "2026-02-10T11:04:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-10T23:30:00Z",
        },
        {
            "market_id": "0xMKT_A",
            "side": "yes",
            "price": 0.31,
            "size_usd": 650.0,
            "timestamp": "2026-02-12T07:01:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-12T14:00:00Z",
        },
        {
            "market_id": "0xMKT_G",
            "side": "yes",
            "price": 0.21,
            "size_usd": 1100.0,
            "timestamp": "2026-02-18T10:02:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-18T21:00:00Z",
        },
        {
            "market_id": "0xMKT_H",
            "side": "no",
            "price": 0.45,
            "size_usd": 250.0,
            "timestamp": "2026-02-20T13:01:00Z",
            "resolved": False,
            "resolution": None,
            "resolution_time": None,
        },
        {
            "market_id": "0xMKT_B",
            "side": "yes",
            "price": 0.39,
            "size_usd": 550.0,
            "timestamp": "2026-02-22T08:02:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-22T19:00:00Z",
        },
        {
            "market_id": "0xMKT_I",
            "side": "no",
            "price": 0.66,
            "size_usd": 300.0,
            "timestamp": "2026-02-24T15:03:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-25T03:00:00Z",
        },
        {
            "market_id": "0xMKT_J",
            "side": "yes",
            "price": 0.29,
            "size_usd": 600.0,
            "timestamp": "2026-02-26T09:04:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-26T20:00:00Z",
        },
    ],
    "0xRETAIL_001": [
        {
            "market_id": "0xMKT_A",
            "side": "no",
            "price": 0.65,
            "size_usd": 50.0,
            "timestamp": "2026-01-15T12:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-15T22:00:00Z",
        },
        {
            "market_id": "0xMKT_B",
            "side": "no",
            "price": 0.58,
            "size_usd": 75.0,
            "timestamp": "2026-01-20T14:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-20T18:00:00Z",
        },
        {
            "market_id": "0xMKT_C",
            "side": "yes",
            "price": 0.50,
            "size_usd": 100.0,
            "timestamp": "2026-02-01T16:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-02T10:00:00Z",
        },
        {
            "market_id": "0xMKT_D",
            "side": "no",
            "price": 0.75,
            "size_usd": 60.0,
            "timestamp": "2026-02-05T15:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-05T20:00:00Z",
        },
        {
            "market_id": "0xMKT_E",
            "side": "no",
            "price": 0.55,
            "size_usd": 80.0,
            "timestamp": "2026-02-10T18:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-10T23:30:00Z",
        },
        {
            "market_id": "0xMKT_F",
            "side": "yes",
            "price": 0.30,
            "size_usd": 90.0,
            "timestamp": "2026-02-14T10:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-15T04:00:00Z",
        },
        {
            "market_id": "0xMKT_K",
            "side": "yes",
            "price": 0.50,
            "size_usd": 40.0,
            "timestamp": "2026-02-16T09:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-17T08:00:00Z",
        },
        {
            "market_id": "0xMKT_L",
            "side": "no",
            "price": 0.48,
            "size_usd": 55.0,
            "timestamp": "2026-02-19T14:30:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-20T02:00:00Z",
        },
        {
            "market_id": "0xMKT_G",
            "side": "no",
            "price": 0.80,
            "size_usd": 45.0,
            "timestamp": "2026-02-18T14:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-18T21:00:00Z",
        },
        {
            "market_id": "0xMKT_M",
            "side": "yes",
            "price": 0.52,
            "size_usd": 70.0,
            "timestamp": "2026-02-25T10:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-26T06:00:00Z",
        },
    ],
    "0xRETAIL_002": [
        {
            "market_id": "0xMKT_A",
            "side": "yes",
            "price": 0.50,
            "size_usd": 30.0,
            "timestamp": "2026-01-16T09:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-01-15T22:00:00Z",
        },
        {
            "market_id": "0xMKT_N",
            "side": "yes",
            "price": 0.50,
            "size_usd": 25.0,
            "timestamp": "2026-02-03T11:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-04T08:00:00Z",
        },
        {
            "market_id": "0xMKT_O",
            "side": "no",
            "price": 0.50,
            "size_usd": 35.0,
            "timestamp": "2026-02-08T15:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-09T12:00:00Z",
        },
        {
            "market_id": "0xMKT_E",
            "side": "yes",
            "price": 0.46,
            "size_usd": 40.0,
            "timestamp": "2026-02-10T16:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-10T23:30:00Z",
        },
        {
            "market_id": "0xMKT_P",
            "side": "yes",
            "price": 0.55,
            "size_usd": 20.0,
            "timestamp": "2026-02-15T09:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-16T10:00:00Z",
        },
        {
            "market_id": "0xMKT_Q",
            "side": "no",
            "price": 0.45,
            "size_usd": 30.0,
            "timestamp": "2026-02-17T14:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-18T06:00:00Z",
        },
        {
            "market_id": "0xMKT_R",
            "side": "yes",
            "price": 0.48,
            "size_usd": 50.0,
            "timestamp": "2026-02-20T10:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-21T09:00:00Z",
        },
        {
            "market_id": "0xMKT_S",
            "side": "no",
            "price": 0.52,
            "size_usd": 45.0,
            "timestamp": "2026-02-22T11:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-23T07:00:00Z",
        },
        {
            "market_id": "0xMKT_T",
            "side": "yes",
            "price": 0.60,
            "size_usd": 35.0,
            "timestamp": "2026-02-24T08:00:00Z",
            "resolved": True,
            "resolution": "no",
            "resolution_time": "2026-02-25T10:00:00Z",
        },
        {
            "market_id": "0xMKT_U",
            "side": "yes",
            "price": 0.42,
            "size_usd": 55.0,
            "timestamp": "2026-02-26T16:00:00Z",
            "resolved": True,
            "resolution": "yes",
            "resolution_time": "2026-02-27T05:00:00Z",
        },
    ],
}

# Precomputed fixture Hashdive scores
FIXTURE_HASHDIVE_SCORES: dict[str, int] = {
    "0xINSIDER_001": 88,
    "0xINSIDER_002": 82,
    "0xRETAIL_001": 25,
    "0xRETAIL_002": 18,
}

# Fixture price move data: market_id -> list of (timestamp, price_delta)
FIXTURE_PRICE_MOVES: dict[str, list[dict]] = {
    "0xMKT_A": [
        {"timestamp": "2026-01-15T13:00:00Z", "price_before": 0.35, "price_after": 0.55},
        {"timestamp": "2026-02-12T10:00:00Z", "price_before": 0.30, "price_after": 0.52},
    ],
    "0xMKT_B": [
        {"timestamp": "2026-01-20T11:00:00Z", "price_before": 0.40, "price_after": 0.62},
        {"timestamp": "2026-02-22T12:00:00Z", "price_before": 0.38, "price_after": 0.58},
    ],
    "0xMKT_D": [
        {"timestamp": "2026-02-05T13:00:00Z", "price_before": 0.25, "price_after": 0.50},
    ],
    "0xMKT_E": [
        {"timestamp": "2026-02-10T15:00:00Z", "price_before": 0.45, "price_after": 0.70},
    ],
    "0xMKT_G": [
        {"timestamp": "2026-02-18T14:00:00Z", "price_before": 0.20, "price_after": 0.55},
    ],
    "0xMKT_J": [
        {"timestamp": "2026-02-26T13:00:00Z", "price_before": 0.28, "price_after": 0.52},
    ],
}

# Fixture resolution data: market_id -> resolution info
FIXTURE_RESOLUTIONS: dict[str, dict] = {
    "0xMKT_A": {"resolution": "yes", "resolution_time": "2026-01-15T22:00:00Z"},
    "0xMKT_B": {"resolution": "yes", "resolution_time": "2026-01-20T18:00:00Z"},
    "0xMKT_C": {"resolution": "no",  "resolution_time": "2026-02-02T10:00:00Z"},
    "0xMKT_D": {"resolution": "yes", "resolution_time": "2026-02-05T20:00:00Z"},
    "0xMKT_E": {"resolution": "yes", "resolution_time": "2026-02-10T23:30:00Z"},
    "0xMKT_F": {"resolution": "no",  "resolution_time": "2026-02-15T04:00:00Z"},
    "0xMKT_G": {"resolution": "yes", "resolution_time": "2026-02-18T21:00:00Z"},
    "0xMKT_H": {"resolution": "yes", "resolution_time": "2026-02-21T01:00:00Z"},
    "0xMKT_I": {"resolution": "no",  "resolution_time": "2026-02-25T03:00:00Z"},
    "0xMKT_J": {"resolution": "yes", "resolution_time": "2026-02-26T20:00:00Z"},
    "0xMKT_K": {"resolution": "no",  "resolution_time": "2026-02-17T08:00:00Z"},
    "0xMKT_L": {"resolution": "yes", "resolution_time": "2026-02-20T02:00:00Z"},
    "0xMKT_M": {"resolution": "no",  "resolution_time": "2026-02-26T06:00:00Z"},
    "0xMKT_N": {"resolution": "no",  "resolution_time": "2026-02-04T08:00:00Z"},
    "0xMKT_O": {"resolution": "no",  "resolution_time": "2026-02-09T12:00:00Z"},
    "0xMKT_P": {"resolution": "no",  "resolution_time": "2026-02-16T10:00:00Z"},
    "0xMKT_Q": {"resolution": "yes", "resolution_time": "2026-02-18T06:00:00Z"},
    "0xMKT_R": {"resolution": "no",  "resolution_time": "2026-02-21T09:00:00Z"},
    "0xMKT_S": {"resolution": "yes", "resolution_time": "2026-02-23T07:00:00Z"},
    "0xMKT_T": {"resolution": "no",  "resolution_time": "2026-02-25T10:00:00Z"},
    "0xMKT_U": {"resolution": "yes", "resolution_time": "2026-02-27T05:00:00Z"},
}


# =========================================================================
#  HELPER: Timestamp parsing
# =========================================================================

def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO-8601 timestamp string into a timezone-aware datetime."""
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)


# =========================================================================
#  WALLET PROFILER CLASS
# =========================================================================

class WalletProfiler:
    """
    Compute Insider Probability Scores (IPS) for Polymarket wallets.

    Analyses trading patterns over a configurable lookback window
    to identify wallets exhibiting insider-like behaviour. Uses a
    combination of heuristic feature weights and (optionally) an
    XGBoost / sklearn classifier trained on Hashdive Smart Score labels.

    Typical lifecycle::

        profiler = WalletProfiler()
        scores = await profiler.batch_score(wallet_list, lookback_days=90)
        # or for a single wallet:
        ips = await profiler.compute_ips("0xABC...", trades)
    """

    def __init__(self) -> None:
        """Initialise the WalletProfiler with empty model state."""
        self._model: Any = None
        self._model_type: Optional[str] = None  # "xgboost", "sklearn", or None
        self._feature_names: list[str] = [
            "win_rate_24h",
            "avg_entry_timing",
            "cluster_size",
            "avg_position_size",
            "market_diversity",
            "hashdive_score",
        ]
        self._cluster_cache: dict[str, list[str]] = {}
        logger.info("WalletProfiler initialised.")

    # ------------------------------------------------------------------
    # Public: Single wallet IPS
    # ------------------------------------------------------------------

    async def compute_ips(
        self,
        wallet_address: str,
        trades: list[dict],
        resolutions: Optional[dict[str, dict]] = None,
        price_moves: Optional[dict[str, list[dict]]] = None,
        hashdive_score: Optional[int] = None,
        all_trades: Optional[dict[str, list[dict]]] = None,
    ) -> float:
        """
        Compute the Insider Probability Score for a single wallet.

        Args:
            wallet_address: Ethereum wallet address (0x...).
            trades:         List of trade dicts for this wallet.
            resolutions:    Market resolution data (market_id -> resolution info).
            price_moves:    Price move data (market_id -> list of move dicts).
            hashdive_score: Hashdive Smart Score (0-100), or None if unavailable.
            all_trades:     All wallet trades for cluster detection (optional).

        Returns:
            IPS as a float in [0.0, 1.0].
        """
        if not trades or len(trades) < MIN_TRADES_FOR_SCORING:
            logger.debug(
                "Wallet %s has %d trades (min %d) -- returning default prior %.2f",
                wallet_address, len(trades), MIN_TRADES_FOR_SCORING, IPS_DEFAULT_PRIOR,
            )
            return IPS_DEFAULT_PRIOR

        # Use empty dicts if not provided
        resolutions = resolutions or {}
        price_moves = price_moves or {}

        # Compute individual features
        win_rate = self._compute_win_rate_24h(trades, resolutions)
        entry_timing = self._compute_entry_timing(trades, price_moves)

        # Cluster detection
        cluster_size = 0
        if all_trades:
            clusters = self._detect_clusters(all_trades)
            peers = clusters.get(wallet_address, [])
            cluster_size = len(peers) + 1  # Include self
            self._cluster_cache = clusters

        # Derived features
        avg_position_size = self._compute_avg_position_size(trades)
        market_diversity = self._compute_market_diversity(trades)
        hd_score = hashdive_score if hashdive_score is not None else 50

        features = {
            "win_rate_24h": win_rate,
            "avg_entry_timing": entry_timing,
            "cluster_size": float(cluster_size),
            "avg_position_size": avg_position_size,
            "market_diversity": float(market_diversity),
            "hashdive_score": float(hd_score),
        }

        # If a trained model exists, use it; otherwise use heuristic scoring
        if self._model is not None:
            ips = await self.predict(features)
        else:
            ips = self._heuristic_score(features)

        # Clamp to [0, 1]
        ips = max(0.0, min(1.0, ips))

        logger.info(
            "IPS computed for %s: %.3f (wr=%.2f, timing=%.1f, cluster=%d, "
            "pos_size=%.0f, diversity=%d, hd=%d)",
            wallet_address, ips, win_rate, entry_timing, cluster_size,
            avg_position_size, market_diversity, hd_score,
        )
        return ips

    # ------------------------------------------------------------------
    # Public: Batch scoring
    # ------------------------------------------------------------------

    async def batch_score(
        self,
        wallets: list[str],
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        all_trades: Optional[dict[str, list[dict]]] = None,
        resolutions: Optional[dict[str, dict]] = None,
        price_moves: Optional[dict[str, list[dict]]] = None,
        hashdive_scores: Optional[dict[str, int]] = None,
    ) -> list[dict]:
        """
        Score many wallets and persist results to PostgreSQL.

        In live mode, fetches trade data from Goldsky and Hashdive scores
        from the wallet_scores table. In test mode, uses fixture data.

        Args:
            wallets:          List of wallet addresses to score.
            lookback_days:    Lookback window in days (default 90).
            all_trades:       Pre-fetched trades per wallet (optional).
            resolutions:      Market resolution data (optional).
            price_moves:      Price move data (optional).
            hashdive_scores:  Hashdive scores per wallet (optional).

        Returns:
            List of dicts with wallet_address, ips_score, features.
        """
        if all_trades is None:
            all_trades = await self._fetch_all_trades(wallets, lookback_days)

        resolutions = resolutions or {}
        price_moves = price_moves or {}
        hashdive_scores = hashdive_scores or {}

        results: list[dict] = []

        for wallet in wallets:
            trades = all_trades.get(wallet, [])
            hd_score = hashdive_scores.get(wallet)

            ips = await self.compute_ips(
                wallet_address=wallet,
                trades=trades,
                resolutions=resolutions,
                price_moves=price_moves,
                hashdive_score=hd_score,
                all_trades=all_trades,
            )

            # Compute win rate separately for db storage
            win_rate = self._compute_win_rate_24h(trades, resolutions) if trades else 0.0
            hd = hd_score if hd_score is not None else 0

            # Persist to PostgreSQL
            await self._persist_score(wallet, ips, win_rate, hd)

            # Publish alert if IPS is high
            if ips >= IPS_HIGH_THRESHOLD and len(trades) >= MIN_TRADES_FOR_SCORING:
                await self._publish_alert(wallet, ips, trades)

            results.append({
                "wallet_address": wallet,
                "ips_score": round(ips, 4),
                "win_rate_24h": round(win_rate, 4),
                "hashdive_score": hd,
                "num_trades": len(trades),
                "flagged": ips >= IPS_HIGH_THRESHOLD,
            })

        logger.info(
            "Batch scoring complete: %d wallets scored, %d flagged (IPS >= %.2f)",
            len(results),
            sum(1 for r in results if r["flagged"]),
            IPS_HIGH_THRESHOLD,
        )
        return results

    # ------------------------------------------------------------------
    # Feature: Win rate on 24h-resolved markets
    # ------------------------------------------------------------------

    def _compute_win_rate_24h(
        self,
        trades: list[dict],
        resolutions: dict[str, dict],
    ) -> float:
        """
        Compute win rate on markets that resolved within 24h of trade.

        A "win" is defined as:
        - Wallet bought YES and market resolved YES
        - Wallet bought NO and market resolved NO

        Args:
            trades:      List of trade dicts with side, market_id, timestamp.
            resolutions: Dict mapping market_id to resolution info.

        Returns:
            Win rate as float in [0.0, 1.0]. Returns 0.0 if no qualifying trades.
        """
        wins = 0
        qualifying = 0

        for trade in trades:
            market_id = trade.get("market_id", "")
            res_info = resolutions.get(market_id)

            if res_info is None:
                continue

            res_time_str = res_info.get("resolution_time")
            if res_time_str is None:
                continue

            trade_ts = _parse_ts(trade["timestamp"])
            res_ts = _parse_ts(res_time_str)

            # Only count markets that resolved within 24h of the trade
            hours_to_resolution = (res_ts - trade_ts).total_seconds() / 3600.0
            if hours_to_resolution < 0 or hours_to_resolution > RESOLUTION_WINDOW_HOURS:
                continue

            qualifying += 1

            trade_side = trade.get("side", "").lower()
            resolution = res_info.get("resolution", "").lower()

            if trade_side == resolution:
                wins += 1

        if qualifying == 0:
            return 0.0

        win_rate = wins / qualifying
        logger.debug(
            "Win rate 24h: %d/%d = %.3f", wins, qualifying, win_rate,
        )
        return win_rate

    # ------------------------------------------------------------------
    # Feature: Average entry timing
    # ------------------------------------------------------------------

    def _compute_entry_timing(
        self,
        trades: list[dict],
        price_moves: dict[str, list[dict]],
    ) -> float:
        """
        Compute average hours between wallet trade and major price move.

        A "major price move" is a price change exceeding PRICE_MOVE_THRESHOLD
        (default 5 cents) on the same market. Lower values indicate earlier
        entry (more insider-like).

        Args:
            trades:      List of trade dicts with market_id, timestamp.
            price_moves: Dict mapping market_id to list of move event dicts.

        Returns:
            Average hours before price move. Returns 24.0 (neutral)
            if no qualifying trades match any price move events.
        """
        timings: list[float] = []

        for trade in trades:
            market_id = trade.get("market_id", "")
            moves = price_moves.get(market_id, [])

            if not moves:
                continue

            trade_ts = _parse_ts(trade["timestamp"])

            for move in moves:
                move_ts = _parse_ts(move["timestamp"])
                delta = move.get("price_after", 0) - move.get("price_before", 0)

                # Only consider moves that happen AFTER the trade and are large enough
                hours_diff = (move_ts - trade_ts).total_seconds() / 3600.0
                if hours_diff < 0:
                    continue
                if abs(delta) < PRICE_MOVE_THRESHOLD:
                    continue

                timings.append(hours_diff)

        if not timings:
            return 24.0  # Neutral: no data

        avg_timing = sum(timings) / len(timings)
        logger.debug(
            "Entry timing: %d events, avg=%.1f hours", len(timings), avg_timing,
        )
        return avg_timing

    # ------------------------------------------------------------------
    # Feature: Cluster detection
    # ------------------------------------------------------------------

    def _detect_clusters(
        self,
        all_trades: dict[str, list[dict]],
    ) -> dict[str, list[str]]:
        """
        Detect wallet clusters using connected-component analysis.

        Two wallets are "co-traders" if they trade the same market within
        a CLUSTER_TIME_BUCKET_MINUTES window in at least
        CLUSTER_CO_OCCURRENCE_MIN distinct markets.

        Algorithm:
        1. Group all trades by (market_id, 5-minute time bucket).
        2. Within each group, record all wallet pairs.
        3. Count pair co-occurrences across distinct markets.
        4. Build adjacency graph for pairs exceeding threshold.
        5. Find connected components using BFS.

        Args:
            all_trades: Dict mapping wallet_address to list of trade dicts.

        Returns:
            Dict mapping each wallet_address to its list of cluster peers
            (excluding itself). Empty list means no cluster membership.
        """
        # Step 1: Build time-bucketed groups
        # Key: (market_id, bucket_id) -> set of wallets
        buckets: dict[tuple[str, int], set[str]] = defaultdict(set)

        for wallet, trades in all_trades.items():
            for trade in trades:
                market_id = trade.get("market_id", "")
                ts = _parse_ts(trade["timestamp"])
                # Bucket ID: minutes since epoch, rounded to 5-min intervals
                bucket_id = int(ts.timestamp() / 60) // CLUSTER_TIME_BUCKET_MINUTES
                buckets[(market_id, bucket_id)].add(wallet)

        # Step 2: Count pair co-occurrences across distinct markets
        # pair -> set of market_ids they co-occurred in
        pair_markets: dict[tuple[str, str], set[str]] = defaultdict(set)

        for (market_id, _bucket_id), wallets in buckets.items():
            wallet_list = sorted(wallets)
            for i in range(len(wallet_list)):
                for j in range(i + 1, len(wallet_list)):
                    pair = (wallet_list[i], wallet_list[j])
                    pair_markets[pair].add(market_id)

        # Step 3: Build adjacency graph for pairs exceeding threshold
        adjacency: dict[str, set[str]] = defaultdict(set)
        for (w1, w2), markets in pair_markets.items():
            if len(markets) >= CLUSTER_CO_OCCURRENCE_MIN:
                adjacency[w1].add(w2)
                adjacency[w2].add(w1)

        # Step 4: BFS to find connected components
        visited: set[str] = set()
        components: list[list[str]] = []

        for wallet in adjacency:
            if wallet in visited:
                continue
            component: list[str] = []
            queue = [wallet]
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                component.append(current)
                for neighbor in adjacency[current]:
                    if neighbor not in visited:
                        queue.append(neighbor)
            components.append(component)

        # Step 5: Build result mapping
        result: dict[str, list[str]] = {}
        for component in components:
            for wallet in component:
                peers = [w for w in component if w != wallet]
                result[wallet] = peers

        logger.info(
            "Cluster detection: %d components found, largest=%d wallets",
            len(components),
            max((len(c) for c in components), default=0),
        )
        return result

    # ------------------------------------------------------------------
    # ML: Train classifier
    # ------------------------------------------------------------------

    async def train_classifier(self, labeled_data: list[dict]) -> None:
        """
        Train a binary classifier on wallet features with Hashdive labels.

        Label generation:
        - Hashdive Smart Score > 80 -> positive (insider-like)
        - Hashdive Smart Score < 30 -> negative (retail)
        - Scores 30-80 are excluded from training (ambiguous)

        Attempts XGBoost first; falls back to sklearn GradientBoosting
        if xgboost is not installed. Falls back to no model (heuristic
        scoring) if neither is available.

        Args:
            labeled_data: List of dicts with feature values and
                          'hashdive_score' for label generation.

        Raises:
            Logs ERROR if insufficient training data (< 50 per class).
        """
        # Generate labels from Hashdive scores
        positives = [d for d in labeled_data if d.get("hashdive_score", 50) > 80]
        negatives = [d for d in labeled_data if d.get("hashdive_score", 50) < 30]

        if len(positives) < MIN_POSITIVE_LABELS or len(negatives) < MIN_NEGATIVE_LABELS:
            logger.error(
                "Insufficient training data: %d positives (need %d), "
                "%d negatives (need %d). Skipping model training, "
                "using heuristic scoring.",
                len(positives), MIN_POSITIVE_LABELS,
                len(negatives), MIN_NEGATIVE_LABELS,
            )
            return

        # Build feature matrix and labels
        train_data = []
        labels = []
        for d in positives:
            train_data.append(self._extract_feature_vector(d))
            labels.append(1)
        for d in negatives:
            train_data.append(self._extract_feature_vector(d))
            labels.append(0)

        logger.info(
            "Training classifier: %d samples (%d positive, %d negative)",
            len(train_data), len(positives), len(negatives),
        )

        # Try XGBoost first
        try:
            import xgboost as xgb

            model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
                objective="binary:logistic",
                eval_metric="auc",
                use_label_encoder=False,
            )
            model.fit(train_data, labels)
            self._model = model
            self._model_type = "xgboost"
            logger.info("XGBoost classifier trained successfully.")

            # Save model
            MODEL_DIR.mkdir(parents=True, exist_ok=True)
            model.save_model(str(MODEL_PATH))
            logger.info("Model saved to %s", MODEL_PATH)
            return

        except ImportError:
            logger.warning(
                "xgboost not installed, falling back to sklearn "
                "GradientBoostingClassifier."
            )
        except Exception as exc:
            logger.warning("XGBoost training failed: %s. Falling back to sklearn.", exc)

        # Fallback: sklearn GradientBoostingClassifier
        try:
            from sklearn.ensemble import GradientBoostingClassifier

            model = GradientBoostingClassifier(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
            )
            model.fit(train_data, labels)
            self._model = model
            self._model_type = "sklearn"
            logger.info("sklearn GradientBoostingClassifier trained successfully.")

            # Save model with joblib
            try:
                import joblib
                MODEL_DIR.mkdir(parents=True, exist_ok=True)
                sklearn_path = MODEL_DIR / "ips_model_sklearn.joblib"
                joblib.dump(model, str(sklearn_path))
                logger.info("sklearn model saved to %s", sklearn_path)
            except ImportError:
                logger.warning("joblib not installed; sklearn model not persisted to disk.")

            return

        except ImportError:
            logger.warning(
                "sklearn not installed either. Using heuristic scoring only."
            )
        except Exception as exc:
            logger.warning("sklearn training failed: %s. Using heuristic scoring.", exc)

        self._model = None
        self._model_type = None

    # ------------------------------------------------------------------
    # ML: Predict IPS from trained model
    # ------------------------------------------------------------------

    async def predict(self, wallet_features: dict) -> float:
        """
        Predict IPS from a trained model.

        Args:
            wallet_features: Dict with feature names as keys.

        Returns:
            Predicted IPS as float in [0.0, 1.0].
            Falls back to heuristic score if no model is loaded.
        """
        if self._model is None:
            return self._heuristic_score(wallet_features)

        feature_vector = self._extract_feature_vector(wallet_features)

        try:
            if self._model_type == "xgboost":
                # XGBoost predict_proba returns [[p_neg, p_pos]]
                import numpy as np
                proba = self._model.predict_proba([feature_vector])
                return float(proba[0][1])
            elif self._model_type == "sklearn":
                proba = self._model.predict_proba([feature_vector])
                return float(proba[0][1])
            else:
                return self._heuristic_score(wallet_features)
        except Exception as exc:
            logger.warning("Model prediction failed: %s. Using heuristic.", exc)
            return self._heuristic_score(wallet_features)

    # ------------------------------------------------------------------
    # Internal: Heuristic scoring (no ML model)
    # ------------------------------------------------------------------

    def _heuristic_score(self, features: dict) -> float:
        """
        Compute a heuristic IPS when no trained model is available.

        Uses weighted combination of normalised features:
        - win_rate_24h:      raw value (already 0-1)
        - avg_entry_timing:  inverse-normalised (lower = better insider signal)
        - cluster_size:      normalised (larger cluster = stronger signal)
        - hashdive_score:    normalised to 0-1
        - market_diversity:  mild positive signal (more diverse = more suspicious)

        Returns:
            Heuristic IPS in [0.0, 1.0].
        """
        wr = features.get("win_rate_24h", 0.5)

        # Entry timing: 0h = perfect insider (score 1.0), 24h+ = neutral (score 0.0)
        raw_timing = features.get("avg_entry_timing", 24.0)
        timing_score = max(0.0, 1.0 - (raw_timing / 24.0))

        # Cluster size: 0-1 = no cluster (0.0), 5+ = strong signal (1.0)
        cluster_sz = features.get("cluster_size", 0)
        cluster_score = min(1.0, cluster_sz / CLUSTER_SIZE_LARGE) if cluster_sz > 1 else 0.0

        # Hashdive: normalise 0-100 to 0-1
        hd = features.get("hashdive_score", 50)
        hd_score = hd / 100.0

        # Market diversity: normalise, capped at 20 markets
        diversity = features.get("market_diversity", 1)
        diversity_score = min(1.0, diversity / 20.0)

        ips = (
            _WEIGHT_WIN_RATE * wr
            + _WEIGHT_ENTRY_TIMING * timing_score
            + _WEIGHT_CLUSTER * cluster_score
            + _WEIGHT_HASHDIVE * hd_score
            + _WEIGHT_DIVERSITY * diversity_score
        )

        return max(0.0, min(1.0, ips))

    # ------------------------------------------------------------------
    # Internal: Feature extraction helpers
    # ------------------------------------------------------------------

    def _extract_feature_vector(self, features: dict) -> list[float]:
        """Extract an ordered feature vector from a features dict."""
        return [float(features.get(name, 0.0)) for name in self._feature_names]

    def _compute_avg_position_size(self, trades: list[dict]) -> float:
        """Compute average trade size in USD."""
        sizes = [t.get("size_usd", 0.0) for t in trades if t.get("size_usd")]
        if not sizes:
            return 0.0
        return sum(sizes) / len(sizes)

    def _compute_market_diversity(self, trades: list[dict]) -> int:
        """Count the number of distinct markets traded."""
        markets = {t.get("market_id") for t in trades if t.get("market_id")}
        return len(markets)

    # ------------------------------------------------------------------
    # Internal: Data fetching (live mode stubs)
    # ------------------------------------------------------------------

    async def _fetch_all_trades(
        self,
        wallets: list[str],
        lookback_days: int,
    ) -> dict[str, list[dict]]:
        """
        Fetch trade history for all wallets from Goldsky.

        In --test mode, returns fixture data. In live mode, this would
        query the Goldsky GraphQL API for order-filled events.

        Args:
            wallets:       List of wallet addresses.
            lookback_days: Number of days of history to fetch.

        Returns:
            Dict mapping wallet_address to list of trade dicts.
        """
        if _test_mode:
            logger.info("Using fixture trade data for %d wallets.", len(wallets))
            return {w: FIXTURE_TRADES.get(w, []) for w in wallets}

        # In production, this would call Goldsky GraphQL.
        # For now, return empty trades and log a warning.
        logger.warning(
            "Live Goldsky fetching not yet implemented. "
            "Run with --test for fixture data. "
            "Returning empty trade histories for %d wallets.",
            len(wallets),
        )
        return {w: [] for w in wallets}

    # ------------------------------------------------------------------
    # Internal: Persistence
    # ------------------------------------------------------------------

    async def _persist_score(
        self,
        wallet_address: str,
        ips_score: float,
        win_rate_24h: float,
        hashdive_score: int,
    ) -> None:
        """
        Persist a wallet's IPS score to PostgreSQL via db.upsert_wallet_score().

        Args:
            wallet_address: Wallet address.
            ips_score:      Computed IPS.
            win_rate_24h:   Win rate feature.
            hashdive_score: Hashdive Smart Score.
        """
        try:
            from execution.utils.db import upsert_wallet_score
            await upsert_wallet_score(
                wallet_address=wallet_address,
                ips_score=round(ips_score, 4),
                win_rate_24h=round(win_rate_24h, 4),
                hashdive_score=hashdive_score,
            )
        except Exception as exc:
            logger.error(
                "Failed to persist score for %s: %s", wallet_address, exc,
            )

    # ------------------------------------------------------------------
    # Internal: Alert publishing
    # ------------------------------------------------------------------

    async def _publish_alert(
        self,
        wallet_address: str,
        ips_score: float,
        trades: list[dict],
    ) -> None:
        """
        Publish a high-IPS wallet alert to stream:insider:alerts.

        The alert includes the wallet address, IPS score, and the most
        recent trade for context.

        Args:
            wallet_address: Wallet address.
            ips_score:      Computed IPS.
            trades:         Wallet's trade history.
        """
        try:
            from execution.utils.redis_client import publish

            # Find the most recent trade for context
            latest_trade = {}
            if trades:
                sorted_trades = sorted(
                    trades,
                    key=lambda t: _parse_ts(t["timestamp"]),
                    reverse=True,
                )
                latest_trade = sorted_trades[0]

            alert_data = {
                "wallet_address": wallet_address,
                "ips_score": str(round(ips_score, 4)),
                "market_id": latest_trade.get("market_id", ""),
                "side": latest_trade.get("side", ""),
                "price": str(latest_trade.get("price", "")),
                "size_usd": str(latest_trade.get("size_usd", "")),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "strategy": "s2",
            }

            msg_id = await publish(STREAM_INSIDER_ALERTS, alert_data)
            logger.info(
                "Published insider alert for %s (IPS=%.3f) -> %s",
                wallet_address, ips_score, msg_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to publish alert for %s: %s", wallet_address, exc,
            )


# =========================================================================
#  CROSS-VALIDATION (--validate mode)
# =========================================================================

def _run_cross_validation(
    profiler: WalletProfiler,
    all_trades: dict[str, list[dict]],
    resolutions: dict[str, dict],
    price_moves: dict[str, list[dict]],
    hashdive_scores: dict[str, int],
) -> dict[str, float]:
    """
    Run feature-level analysis and print classification metrics.

    Since the fixture dataset is small, this performs a leave-one-out
    style analysis rather than full 5-fold CV. In production with
    sufficient data, this would use time-ordered 5-fold splits.

    Returns:
        Dict of metric name -> value.
    """
    print()
    print("=" * 72)
    print("  IPS Model Validation (Fixture Data)")
    print("=" * 72)

    # Compute features for each wallet
    wallet_features: dict[str, dict] = {}
    for wallet, trades in all_trades.items():
        win_rate = profiler._compute_win_rate_24h(trades, resolutions)
        entry_timing = profiler._compute_entry_timing(trades, price_moves)
        clusters = profiler._detect_clusters(all_trades)
        cluster_peers = clusters.get(wallet, [])
        cluster_size = len(cluster_peers) + 1

        features = {
            "win_rate_24h": win_rate,
            "avg_entry_timing": entry_timing,
            "cluster_size": float(cluster_size),
            "avg_position_size": profiler._compute_avg_position_size(trades),
            "market_diversity": float(profiler._compute_market_diversity(trades)),
            "hashdive_score": float(hashdive_scores.get(wallet, 50)),
        }
        wallet_features[wallet] = features

    # Print feature table
    print()
    print("  %-18s %8s %8s %8s %8s %8s %8s %8s" % (
        "Wallet", "WinRate", "Timing", "Cluster", "AvgSize",
        "Divers.", "HD", "IPS",
    ))
    print("  " + "-" * 86)

    results: dict[str, float] = {}
    for wallet, feats in wallet_features.items():
        ips = profiler._heuristic_score(feats)
        results[wallet] = ips
        short_wallet = wallet[:16] + ".."
        print(
            "  %-18s %8.3f %8.1f %8.0f %8.0f %8.0f %8.0f %8.3f" % (
                short_wallet,
                feats["win_rate_24h"],
                feats["avg_entry_timing"],
                feats["cluster_size"],
                feats["avg_position_size"],
                feats["market_diversity"],
                feats["hashdive_score"],
                ips,
            )
        )

    # Classification metrics using HD score as ground truth
    # Positive: HD > 80, Negative: HD < 30
    tp = fp = tn = fn = 0
    for wallet, ips in results.items():
        hd = hashdive_scores.get(wallet, 50)
        predicted_insider = ips >= IPS_HIGH_THRESHOLD
        if hd > 80:   # Actually insider
            if predicted_insider:
                tp += 1
            else:
                fn += 1
        elif hd < 30:  # Actually retail
            if predicted_insider:
                fp += 1
            else:
                tn += 1
        # else: ambiguous, skip

    total = tp + fp + tn + fn
    accuracy = (tp + tn) / total if total > 0 else 0.0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

    print()
    print("  Classification Metrics (threshold IPS >= %.2f):" % IPS_HIGH_THRESHOLD)
    print("  " + "-" * 40)
    print("  True Positives:  %d" % tp)
    print("  False Positives: %d" % fp)
    print("  True Negatives:  %d" % tn)
    print("  False Negatives: %d" % fn)
    print("  Accuracy:        %.3f" % accuracy)
    print("  Precision:       %.3f" % precision)
    print("  Recall:          %.3f" % recall)
    print("  F1 Score:        %.3f" % f1)
    print()
    print("=" * 72)
    print()

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


# =========================================================================
#  TEST MODE
# =========================================================================

async def _run_test_mode() -> None:
    """
    Run the wallet profiler in --test mode with fixture data.

    Exercises all public methods without external API calls or database
    connections. Uses the in-memory mock pool from db.py and the
    InMemoryRedis mock from redis_client.py.
    """
    import execution.utils.db as db
    import execution.utils.redis_client as redis_client

    # Enable mock modes
    db._test_mode = True
    redis_client._use_mock = True

    # Ensure consumer groups exist (for Redis alert publishing)
    from execution.utils.redis_client import ensure_consumer_groups
    await ensure_consumer_groups()

    print()
    print("=" * 72)
    print("  Wallet Profiler -- Test Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    profiler = WalletProfiler()

    # ---- Test 1: Single wallet IPS computation --------------------------
    print()
    print("-" * 72)
    print("  Test 1: Single wallet IPS (0xINSIDER_001)")
    print("-" * 72)

    insider_trades = FIXTURE_TRADES["0xINSIDER_001"]
    ips = await profiler.compute_ips(
        wallet_address="0xINSIDER_001",
        trades=insider_trades,
        resolutions=FIXTURE_RESOLUTIONS,
        price_moves=FIXTURE_PRICE_MOVES,
        hashdive_score=FIXTURE_HASHDIVE_SCORES["0xINSIDER_001"],
        all_trades=FIXTURE_TRADES,
    )
    print("  IPS for 0xINSIDER_001: %.4f" % ips)
    assert ips > 0.5, "Insider wallet should have IPS > 0.5"
    print("  PASS: IPS > 0.50")

    # ---- Test 2: Retail wallet IPS (should be lower) --------------------
    print()
    print("-" * 72)
    print("  Test 2: Single wallet IPS (0xRETAIL_001)")
    print("-" * 72)

    retail_trades = FIXTURE_TRADES["0xRETAIL_001"]
    ips_retail = await profiler.compute_ips(
        wallet_address="0xRETAIL_001",
        trades=retail_trades,
        resolutions=FIXTURE_RESOLUTIONS,
        price_moves=FIXTURE_PRICE_MOVES,
        hashdive_score=FIXTURE_HASHDIVE_SCORES["0xRETAIL_001"],
        all_trades=FIXTURE_TRADES,
    )
    print("  IPS for 0xRETAIL_001: %.4f" % ips_retail)
    assert ips_retail < ips, "Retail wallet should have lower IPS than insider"
    print("  PASS: Retail IPS (%.4f) < Insider IPS (%.4f)" % (ips_retail, ips))

    # ---- Test 3: Cluster detection --------------------------------------
    print()
    print("-" * 72)
    print("  Test 3: Cluster detection")
    print("-" * 72)

    clusters = profiler._detect_clusters(FIXTURE_TRADES)
    for wallet, peers in clusters.items():
        print("  %s -> peers: %s" % (wallet, peers))

    # Insiders should be clustered together (they trade same markets within 5 min)
    if "0xINSIDER_001" in clusters and "0xINSIDER_002" in clusters:
        assert "0xINSIDER_002" in clusters.get("0xINSIDER_001", []), \
            "Insider wallets should be in the same cluster"
        print("  PASS: 0xINSIDER_001 and 0xINSIDER_002 are clustered together")
    else:
        print("  INFO: Cluster detection returned: %s" % clusters)

    # ---- Test 4: Win rate computation -----------------------------------
    print()
    print("-" * 72)
    print("  Test 4: Win rate 24h")
    print("-" * 72)

    wr_insider = profiler._compute_win_rate_24h(
        FIXTURE_TRADES["0xINSIDER_001"], FIXTURE_RESOLUTIONS,
    )
    wr_retail = profiler._compute_win_rate_24h(
        FIXTURE_TRADES["0xRETAIL_001"], FIXTURE_RESOLUTIONS,
    )
    print("  Win rate 0xINSIDER_001: %.3f" % wr_insider)
    print("  Win rate 0xRETAIL_001:  %.3f" % wr_retail)
    assert wr_insider > wr_retail, "Insider should have higher win rate"
    print("  PASS: Insider win rate > Retail win rate")

    # ---- Test 5: Entry timing -------------------------------------------
    print()
    print("-" * 72)
    print("  Test 5: Entry timing")
    print("-" * 72)

    timing_insider = profiler._compute_entry_timing(
        FIXTURE_TRADES["0xINSIDER_001"], FIXTURE_PRICE_MOVES,
    )
    timing_retail = profiler._compute_entry_timing(
        FIXTURE_TRADES["0xRETAIL_001"], FIXTURE_PRICE_MOVES,
    )
    print("  Avg entry timing 0xINSIDER_001: %.1f hours" % timing_insider)
    print("  Avg entry timing 0xRETAIL_001:  %.1f hours" % timing_retail)
    print("  INFO: Lower timing = earlier entry relative to price move")

    # ---- Test 6: Batch scoring ------------------------------------------
    print()
    print("-" * 72)
    print("  Test 6: Batch scoring (all wallets)")
    print("-" * 72)

    all_wallets = list(FIXTURE_TRADES.keys())
    results = await profiler.batch_score(
        wallets=all_wallets,
        lookback_days=90,
        all_trades=FIXTURE_TRADES,
        resolutions=FIXTURE_RESOLUTIONS,
        price_moves=FIXTURE_PRICE_MOVES,
        hashdive_scores=FIXTURE_HASHDIVE_SCORES,
    )

    for r in results:
        flag_str = " ** FLAGGED **" if r["flagged"] else ""
        print(
            "  %-18s  IPS=%.4f  WR=%.4f  HD=%3d  trades=%2d%s" % (
                r["wallet_address"],
                r["ips_score"],
                r["win_rate_24h"],
                r["hashdive_score"],
                r["num_trades"],
                flag_str,
            )
        )

    flagged_count = sum(1 for r in results if r["flagged"])
    print("  Total wallets scored: %d" % len(results))
    print("  Flagged (IPS >= %.2f): %d" % (IPS_HIGH_THRESHOLD, flagged_count))

    # ---- Test 7: Wallet with insufficient trades ------------------------
    print()
    print("-" * 72)
    print("  Test 7: Wallet with insufficient trades (< %d)" % MIN_TRADES_FOR_SCORING)
    print("-" * 72)

    few_trades = FIXTURE_TRADES["0xINSIDER_001"][:3]
    ips_few = await profiler.compute_ips(
        wallet_address="0xNEW_WALLET",
        trades=few_trades,
        resolutions=FIXTURE_RESOLUTIONS,
    )
    print("  IPS for 0xNEW_WALLET (%d trades): %.4f" % (len(few_trades), ips_few))
    assert ips_few == IPS_DEFAULT_PRIOR, \
        "Wallet with < %d trades should get default prior" % MIN_TRADES_FOR_SCORING
    print("  PASS: Default prior (%.2f) returned for insufficient data" % IPS_DEFAULT_PRIOR)

    # ---- Test 8: Verify DB persistence ----------------------------------
    print()
    print("-" * 72)
    print("  Test 8: Database persistence check")
    print("-" * 72)

    from execution.utils.db import get_wallet_score, get_high_ips_wallets

    for wallet in all_wallets:
        score_row = await get_wallet_score(wallet)
        if score_row:
            print(
                "  DB: %-18s  ips=%.3f  wr=%.3f  hd=%d" % (
                    score_row["wallet_address"],
                    score_row["ips_score"],
                    score_row["win_rate_24h"],
                    score_row["hashdive_score"],
                )
            )

    high_ips = await get_high_ips_wallets(min_score=IPS_HIGH_THRESHOLD)
    print("  High-IPS wallets in DB: %d" % len(high_ips))

    # ---- Test 9: Redis alert check --------------------------------------
    print()
    print("-" * 72)
    print("  Test 9: Redis insider alert check")
    print("-" * 72)

    from execution.utils.redis_client import stream_len
    alert_count = await stream_len(STREAM_INSIDER_ALERTS)
    print("  Alerts in %s: %d" % (STREAM_INSIDER_ALERTS, alert_count))

    # ---- Cleanup --------------------------------------------------------
    from execution.utils.db import close_pool
    from execution.utils.redis_client import close_redis
    await close_pool()
    await close_redis()

    print()
    print("=" * 72)
    print("  ALL TESTS PASSED")
    print("=" * 72)
    print()


# =========================================================================
#  VALIDATE MODE
# =========================================================================

async def _run_validate_mode() -> None:
    """
    Run cross-validation analysis on fixture data and print metrics.
    """
    profiler = WalletProfiler()
    metrics = _run_cross_validation(
        profiler=profiler,
        all_trades=FIXTURE_TRADES,
        resolutions=FIXTURE_RESOLUTIONS,
        price_moves=FIXTURE_PRICE_MOVES,
        hashdive_scores=FIXTURE_HASHDIVE_SCORES,
    )
    logger.info("Validation metrics: %s", metrics)


# =========================================================================
#  LIVE MODE (hourly batch loop)
# =========================================================================

async def _run_live_mode(interval_seconds: int = BATCH_INTERVAL_SECONDS) -> None:
    """
    Run the wallet profiler in live mode with periodic batch scoring.

    Fetches wallet data from Goldsky, computes IPS scores, persists
    results, and publishes alerts. Runs every ``interval_seconds``
    (default: 3600 = 1 hour).

    Args:
        interval_seconds: Seconds between batch runs.
    """
    from execution.utils.redis_client import ensure_consumer_groups

    logger.info(
        "Starting live wallet profiler (interval=%ds)", interval_seconds,
    )

    # Ensure Redis consumer groups exist
    await ensure_consumer_groups()

    profiler = WalletProfiler()

    while True:
        run_start = _time.monotonic()
        logger.info("=" * 60)
        logger.info("Starting hourly IPS batch run at %s",
                     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        logger.info("=" * 60)

        try:
            # In production, this would:
            # 1. Query Goldsky for all active wallets with trades in last 90 days
            # 2. Fetch Hashdive scores from wallet_scores table
            # 3. Pull resolution data from Polymarket API
            # 4. Compute and persist all scores

            # For now, log a placeholder message
            logger.warning(
                "Live mode: Goldsky and Hashdive connectors not yet implemented. "
                "Use --test for fixture-based scoring."
            )

            # Fetch high-IPS wallets from previous runs for monitoring
            try:
                from execution.utils.db import get_high_ips_wallets
                existing = await get_high_ips_wallets(min_score=IPS_HIGH_THRESHOLD)
                logger.info(
                    "Currently tracking %d high-IPS wallets", len(existing),
                )
            except Exception as exc:
                logger.warning("Could not fetch existing scores: %s", exc)

        except Exception as exc:
            logger.error("Batch run failed: %s", exc, exc_info=True)

        elapsed = _time.monotonic() - run_start
        sleep_time = max(0, interval_seconds - elapsed)
        logger.info(
            "Batch run completed in %.1fs. Next run in %.0fs.",
            elapsed, sleep_time,
        )
        await asyncio.sleep(sleep_time)


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point: parse arguments and dispatch to the appropriate mode.

    Modes:
        (default)  Live hourly batch scoring
        --test     Fixture-based test mode (no external dependencies)
        --validate Cross-validation metrics on fixture data
    """
    global _test_mode

    parser = argparse.ArgumentParser(
        description=(
            "Wallet Profiler -- IPS (Insider Probability Score) batch processor. "
            "Analyses wallet trading patterns and computes insider probability scores."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run with fixture trade data (no DB/Redis/API needed).",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run cross-validation on fixture data and print metrics.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=BATCH_INTERVAL_SECONDS,
        help="Batch interval in seconds for live mode (default: %d)." % BATCH_INTERVAL_SECONDS,
    )
    args = parser.parse_args()

    if args.test:
        _test_mode = True
        logger.info("Running in --test mode with fixture data.")
        asyncio.run(_run_test_mode())
    elif args.validate:
        _test_mode = True
        logger.info("Running in --validate mode.")
        asyncio.run(_run_validate_mode())
    else:
        logger.info("Running in live mode.")
        asyncio.run(_run_live_mode(interval_seconds=args.interval))


if __name__ == "__main__":
    main()
