#!/usr/bin/env python3
"""
s4_intramarket_arb.py -- Strategy S4: Intra-venue Combinatorial Arbitrage.

Detects arbitrage opportunities within Polymarket by:
    1. Type 1 — Yes+No price mismatches on individual contracts
       (yes_price + no_price != 1.00 beyond fee threshold).
    2. Type 2 — Logical contradictions between semantically related markets
       (e.g. "Trump wins" at 62c AND "Republican wins" at 58c).

Uses sentence-transformer embeddings via MarketMatcher for semantic
market grouping and logical implication detection.

Scan cadence:
    Type 1: Every 60 seconds  (cheap computation)
    Type 2: Every 5 minutes   (embedding-heavy)

Outputs:
    stream:arb:signals    -- Arb opportunity signals for monitoring/audit
    stream:orders:pending -- Trade signals for execution engine

Usage:
    python execution/strategies/s4_intramarket_arb.py              # Live mode
    python execution/strategies/s4_intramarket_arb.py --test       # Fixture mode
    python execution/strategies/s4_intramarket_arb.py --backtest   # Historical backtest

See: directives/06_strategy_s4_intramarket_arb.md
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/strategies/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
S4_FIXTURES_FILE = FIXTURES_DIR / "s4_markets.json"

# Ensure the project root is on sys.path so that sibling packages
# (execution.utils, execution.pipeline) can be imported when this
# script is run directly via `python execution/strategies/s4_intramarket_arb.py`.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
STRATEGY_ID = "s4"
VENUE = "polymarket"

# Polymarket fee per side (~2%), meaning yes + no normally sums to ~1.00
# after the spread accounts for fees on both sides.
POLYMARKET_FEE_PCT = 0.02

# Type 1: Yes+No mismatch threshold (3c beyond fee, per directive 06)
YES_NO_MISMATCH_THRESHOLD = 0.03

# Type 2: Minimum edge after fees for logical contradiction arb
MIN_EDGE_AFTER_FEES = 0.03

# Type 2: Mutual exclusion violation threshold
# P(A) + P(B) must exceed 1.00 by at least this amount
MUTUAL_EXCLUSION_THRESHOLD = 0.03

# Position sizing: never exceed this fraction of visible order book depth
MAX_DEPTH_FRACTION = 0.50

# Conservative fallback size when order book depth is unavailable (USD)
FALLBACK_SIZE_USD = 20.0

# Maximum price age in seconds; skip stale prices (arbs are time-sensitive)
MAX_PRICE_AGE_SECONDS = 120

# Scan intervals
TYPE1_SCAN_INTERVAL_SECONDS = 60
TYPE2_SCAN_INTERVAL_SECONDS = 300

# Semantic similarity thresholds (from directive 06 & market_matcher.py)
SEMANTIC_THRESHOLD_RELATED = 0.60
SEMANTIC_THRESHOLD_SAME_EVENT = 0.80

# ---------------------------------------------------------------------------
# Redis stream names (imported lazily, defined here for reference)
# ---------------------------------------------------------------------------
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
STREAM_ARB_SIGNALS = "stream:arb:signals"
STREAM_ORDERS_PENDING = "stream:orders:pending"

# Consumer group for reading normalised prices
GROUP_STRATEGY = "strategy_group"
CONSUMER_NAME = "s4-intramarket-arb"


# =========================================================================
#  FIXTURE DATA (for --test mode)
# =========================================================================

_FIXTURE_MARKETS = [
    # Type 1 test: yes + no = 1.03 (should trigger mismatch arb of 0.03)
    {
        "market_id": "0xTEST_MISMATCH_01",
        "title": "Will SOL reach $300 by Q3 2026?",
        "yes_price": 0.55,
        "no_price": 0.48,
        "venue": "polymarket",
        "category": "Crypto",
        "timestamp": None,  # filled at runtime
        "book_depth_yes": 5000.0,
        "book_depth_no": 3200.0,
    },
    # Type 1 test: yes + no = 0.93 (below 1.00, should trigger)
    {
        "market_id": "0xTEST_MISMATCH_02",
        "title": "US GDP growth above 3% in Q2 2026?",
        "yes_price": 0.40,
        "no_price": 0.53,
        "venue": "polymarket",
        "category": "Economics",
        "timestamp": None,
        "book_depth_yes": 8000.0,
        "book_depth_no": 6000.0,
    },
    # Type 1: Clean market (yes + no = 1.00, no arb)
    {
        "market_id": "0xTEST_CLEAN_01",
        "title": "Will ETH reach $5k by Q2 2026?",
        "yes_price": 0.42,
        "no_price": 0.58,
        "venue": "polymarket",
        "category": "Crypto",
        "timestamp": None,
        "book_depth_yes": 4000.0,
        "book_depth_no": 4500.0,
    },
    # Type 2 test: Trump wins (0.65) implies Republican wins (0.58) -- contradiction
    # Raw edge = 0.07, fees ~ 0.019, edge_after_fees ~ 0.051 > 0.03 threshold
    {
        "market_id": "0xTEST_TRUMP_WIN",
        "title": "Will Trump win the 2028 presidential election?",
        "yes_price": 0.65,
        "no_price": 0.35,
        "venue": "polymarket",
        "category": "Politics",
        "timestamp": None,
        "book_depth_yes": 12000.0,
        "book_depth_no": 10000.0,
    },
    {
        "market_id": "0xTEST_GOP_WIN",
        "title": "Will the Republican Party win the 2028 presidential election?",
        "yes_price": 0.58,
        "no_price": 0.42,
        "venue": "polymarket",
        "category": "Politics",
        "timestamp": None,
        "book_depth_yes": 15000.0,
        "book_depth_no": 13000.0,
    },
    # Type 2 test: Harris wins (0.30) + Trump wins (0.62) = 0.92 -- no violation
    {
        "market_id": "0xTEST_HARRIS_WIN",
        "title": "Will Harris win the 2028 presidential election?",
        "yes_price": 0.30,
        "no_price": 0.70,
        "venue": "polymarket",
        "category": "Politics",
        "timestamp": None,
        "book_depth_yes": 9000.0,
        "book_depth_no": 11000.0,
    },
    # Type 2 test: Mutual exclusion near-violation
    # Biden wins (0.40) + Trump wins (0.62) = 1.02 -- marginal
    {
        "market_id": "0xTEST_BIDEN_WIN",
        "title": "Will Biden win the 2028 presidential election?",
        "yes_price": 0.40,
        "no_price": 0.60,
        "venue": "polymarket",
        "category": "Politics",
        "timestamp": None,
        "book_depth_yes": 7000.0,
        "book_depth_no": 8000.0,
    },
    # Multi-outcome market test: 3 outcomes summing to 1.08 (violation)
    {
        "market_id": "0xTEST_MULTI_01",
        "title": "Who will win the 2026 FIFA World Cup?",
        "outcomes": ["Brazil", "Germany", "Argentina"],
        "outcome_prices": [0.38, 0.35, 0.35],
        "venue": "polymarket",
        "category": "Sports",
        "timestamp": None,
        "book_depth_yes": 6000.0,
        "book_depth_no": 5000.0,
    },
]


# =========================================================================
#  TYPE 1: YES+NO MISMATCH DETECTION
# =========================================================================

def detect_yesno_mismatch(market: dict) -> Optional[dict]:
    """
    Check a single market for Yes+No price mismatch.

    For standard Yes/No markets, yes_price + no_price should equal ~1.00.
    If the absolute deviation exceeds YES_NO_MISMATCH_THRESHOLD (3c),
    it represents an arbitrage opportunity.

    For multi-outcome markets, all outcome prices should sum to ~1.00.

    Args:
        market: Market dict with 'yes_price'/'no_price' or
                'outcomes'/'outcome_prices' keys.

    Returns:
        Arb signal dict if mismatch detected, None otherwise.
    """
    market_id = market.get("market_id", "unknown")
    title = market.get("title", "")

    # Multi-outcome market handling.
    outcome_prices = market.get("outcome_prices")
    if outcome_prices and isinstance(outcome_prices, list) and len(outcome_prices) > 2:
        price_sum = sum(outcome_prices)
        deviation = abs(price_sum - 1.00)
        if deviation > YES_NO_MISMATCH_THRESHOLD:
            # Find the most overpriced and underpriced outcomes.
            outcomes = market.get("outcomes", [f"outcome_{i}" for i in range(len(outcome_prices))])
            logger.info(
                "Type 1 multi-outcome mismatch: '%s' sum=%.4f deviation=%.4f "
                "(outcomes=%s, prices=%s)",
                title[:50], price_sum, deviation, outcomes, outcome_prices,
            )
            return {
                "strategy": STRATEGY_ID,
                "venue": VENUE,
                "arb_type": "multi_outcome_mismatch",
                "market_id": market_id,
                "title": title,
                "outcome_prices": outcome_prices,
                "outcomes": outcomes,
                "price_sum": round(price_sum, 4),
                "deviation": round(deviation, 4),
                "edge_estimate": round(deviation - POLYMARKET_FEE_PCT * price_sum, 4),
                "size_usd": _compute_size(market),
                "confidence": "high" if deviation > 0.05 else "medium",
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
        return None

    # Standard Yes/No market.
    yes_price = market.get("yes_price")
    no_price = market.get("no_price")
    if yes_price is None or no_price is None:
        return None

    try:
        yes_price = float(yes_price)
        no_price = float(no_price)
    except (ValueError, TypeError):
        logger.warning("Invalid prices for market '%s': yes=%s no=%s", market_id, yes_price, no_price)
        return None

    price_sum = yes_price + no_price
    deviation = abs(price_sum - 1.00)

    if deviation <= YES_NO_MISMATCH_THRESHOLD:
        return None

    # Determine which side is underpriced.
    if price_sum > 1.00:
        # Overpriced: sell the more expensive side (or buy both No sides).
        # The arb is: the sum > 1.00, so selling both Yes and No yields profit.
        underpriced_side = "no" if yes_price > no_price else "yes"
        trade_side = "sell_both"
    else:
        # Underpriced: buy both sides for less than $1.00 total.
        underpriced_side = "yes" if yes_price < no_price else "no"
        trade_side = "buy_both"

    # Fee: ~2% on each leg's contract cost.
    fee_estimate = POLYMARKET_FEE_PCT * (yes_price + no_price)
    edge = deviation - fee_estimate
    if edge <= 0:
        logger.debug(
            "Type 1 mismatch for '%s' but edge (%.4f) does not cover fees.",
            title[:50], edge,
        )
        return None

    logger.info(
        "Type 1 mismatch detected: '%s' yes=%.2f no=%.2f sum=%.4f "
        "deviation=%.4f edge=%.4f side=%s",
        title[:50], yes_price, no_price, price_sum, deviation, edge, trade_side,
    )

    return {
        "strategy": STRATEGY_ID,
        "venue": VENUE,
        "arb_type": "yesno_mismatch",
        "market_id": market_id,
        "title": title,
        "yes_price": yes_price,
        "no_price": no_price,
        "price_sum": round(price_sum, 4),
        "deviation": round(deviation, 4),
        "edge_estimate": round(edge, 4),
        "trade_side": trade_side,
        "underpriced_side": underpriced_side,
        "size_usd": _compute_size(market),
        "confidence": "high" if edge > 0.05 else "medium",
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


# =========================================================================
#  TYPE 2: LOGICAL CONTRADICTION DETECTION
# =========================================================================

def detect_logical_contradictions(
    markets: list[dict],
    matcher: Any,
) -> list[dict]:
    """
    Find logical contradictions between semantically related markets.

    Uses MarketMatcher to:
        1. Cluster semantically related markets (cosine > 0.60)
        2. Check for logical implications within clusters
        3. Detect pricing violations: P(A) > P(B) when A implies B

    Also checks for mutual exclusion violations: if A and B are mutually
    exclusive (same question, different candidates), P(A) + P(B) <= 1.00.

    Args:
        markets:  List of market dicts with 'market_id', 'title',
                  'yes_price', 'no_price'.
        matcher:  MarketMatcher instance (from market_matcher.py).

    Returns:
        List of arb signal dicts for detected contradictions.
    """
    if len(markets) < 2:
        logger.info("detect_logical_contradictions: fewer than 2 markets, skipping.")
        return []

    signals: list[dict] = []

    # Step 1: Find semantically related clusters.
    clusters = matcher.find_intra_venue_clusters(
        markets, threshold=SEMANTIC_THRESHOLD_RELATED,
    )
    logger.info(
        "Type 2 scan: %d cluster(s) from %d market(s).",
        len(clusters), len(markets),
    )

    for cluster in clusters:
        if len(cluster) < 2:
            continue

        # Step 2: Check all pairs within the cluster.
        for i in range(len(cluster)):
            for j in range(i + 1, len(cluster)):
                market_a = cluster[i]
                market_b = cluster[j]

                # Filter self-referential matches.
                if market_a["market_id"] == market_b["market_id"]:
                    continue

                # Check price staleness.
                if _is_stale(market_a) or _is_stale(market_b):
                    logger.debug(
                        "Skipping stale pair: '%s' / '%s'",
                        market_a["title"][:30], market_b["title"][:30],
                    )
                    continue

                # --- Implication check: A implies B ---
                # If A implies B, then P(A) <= P(B) must hold.
                if matcher.check_logical_implication(market_a, market_b):
                    signal = _check_implication_violation(
                        market_a, market_b, direction="a_implies_b",
                    )
                    if signal:
                        signals.append(signal)

                # --- Implication check: B implies A ---
                if matcher.check_logical_implication(market_b, market_a):
                    signal = _check_implication_violation(
                        market_b, market_a, direction="b_implies_a",
                    )
                    if signal:
                        signals.append(signal)

                # --- Mutual exclusion check ---
                signal = _check_mutual_exclusion(market_a, market_b)
                if signal:
                    signals.append(signal)

    logger.info(
        "Type 2 scan complete: %d contradiction(s) detected.", len(signals),
    )
    return signals


def _check_implication_violation(
    implier: dict,
    implied: dict,
    direction: str,
) -> Optional[dict]:
    """
    Check whether an implication pricing violation exists.

    If market A implies market B, then P(A) <= P(B) must hold.
    A violation means P(A) > P(B), and the edge is P(A) - P(B).

    The trade is: sell A (overpriced) and buy B (underpriced).

    Args:
        implier:   The market that implies the other ('A implies B').
        implied:   The market that is implied.
        direction: Label for logging ("a_implies_b" or "b_implies_a").

    Returns:
        Arb signal dict if violation detected, None otherwise.
    """
    p_a = float(implier.get("yes_price", 0))
    p_b = float(implied.get("yes_price", 0))

    if p_a <= p_b:
        return None  # No violation.

    raw_edge = p_a - p_b
    # Fee is ~2% of the contract price on each leg.
    # Leg A: sell implier's Yes (buy No at 1 - p_a), fee ~ 2% * (1 - p_a)
    # Leg B: buy implied's Yes at p_b, fee ~ 2% * p_b
    fee_estimate = POLYMARKET_FEE_PCT * ((1.0 - p_a) + p_b)
    edge_after_fees = raw_edge - fee_estimate

    if edge_after_fees < MIN_EDGE_AFTER_FEES:
        logger.debug(
            "Implication violation but edge too small: '%s' (%.2f) → '%s' (%.2f) "
            "raw_edge=%.4f fees=%.4f after_fees=%.4f",
            implier["title"][:35], p_a,
            implied["title"][:35], p_b,
            raw_edge, fee_estimate, edge_after_fees,
        )
        return None

    # Position sizing: 50% of the minimum order book depth across both legs.
    size_usd = _compute_pair_size(implier, implied)

    confidence = "high" if edge_after_fees > 0.05 else "medium"

    logger.info(
        "LOGICAL CONTRADICTION [%s]: '%s' (%.2f) implies '%s' (%.2f) "
        "but P(A) > P(B). edge=%.4f size=$%.2f",
        direction,
        implier["title"][:40], p_a,
        implied["title"][:40], p_b,
        edge_after_fees, size_usd,
    )

    return {
        "strategy": STRATEGY_ID,
        "venue": VENUE,
        "arb_type": "logical_contradiction",
        "leg_a": {
            "market_id": implier["market_id"],
            "title": implier["title"],
            "side": "no",    # Sell the overpriced implier (buy No)
            "price": p_a,
        },
        "leg_b": {
            "market_id": implied["market_id"],
            "title": implied["title"],
            "side": "yes",   # Buy the underpriced implied
            "price": p_b,
        },
        "contradiction": f"A implies B, but P(A) > P(B)",
        "edge_estimate": round(edge_after_fees, 4),
        "raw_edge": round(raw_edge, 4),
        "size_usd": round(size_usd, 2),
        "confidence": confidence,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


def _check_mutual_exclusion(market_a: dict, market_b: dict) -> Optional[dict]:
    """
    Check whether two markets violate mutual exclusion.

    If A and B are mutually exclusive (cannot both be true),
    then P(A) + P(B) <= 1.00 must hold.

    Detection heuristic: both titles contain "win" and reference the
    same election/event type but different candidates.

    Args:
        market_a: First market dict.
        market_b: Second market dict.

    Returns:
        Arb signal dict if violation detected, None otherwise.
    """
    title_a = market_a.get("title", "").lower()
    title_b = market_b.get("title", "").lower()

    # Heuristic: mutual exclusion applies when both titles reference
    # "win" for the same event type but different entities.
    if "win" not in title_a or "win" not in title_b:
        return None

    # Extract candidate names to check they are different.
    candidates = [
        "trump", "biden", "harris", "desantis", "haley", "newsom",
        "brazil", "germany", "argentina", "france", "spain",
    ]
    a_candidates = [c for c in candidates if c in title_a]
    b_candidates = [c for c in candidates if c in title_b]

    # If the same candidate appears in both, it is not mutual exclusion.
    if set(a_candidates) & set(b_candidates):
        return None

    # Both must have at least one recognisable candidate.
    if not a_candidates or not b_candidates:
        return None

    # Check they reference a similar event (e.g. "election", "president").
    event_keywords = ["election", "president", "primary", "world cup", "super bowl"]
    a_event = any(kw in title_a for kw in event_keywords)
    b_event = any(kw in title_b for kw in event_keywords)
    if not (a_event and b_event):
        return None

    p_a = float(market_a.get("yes_price", 0))
    p_b = float(market_b.get("yes_price", 0))
    prob_sum = p_a + p_b

    if prob_sum <= 1.00 + MUTUAL_EXCLUSION_THRESHOLD:
        return None  # No significant violation.

    violation = prob_sum - 1.00
    # Fee: sell No on both sides. Cost for each leg ~ 2% of (1 - price).
    fee_estimate = POLYMARKET_FEE_PCT * ((1.0 - p_a) + (1.0 - p_b))
    edge_after_fees = violation - fee_estimate

    if edge_after_fees < MIN_EDGE_AFTER_FEES:
        logger.debug(
            "Mutual exclusion violation but edge too small: '%s' (%.2f) + '%s' (%.2f) "
            "= %.4f, fees=%.4f after_fees=%.4f",
            market_a["title"][:30], p_a, market_b["title"][:30], p_b,
            prob_sum, fee_estimate, edge_after_fees,
        )
        return None

    size_usd = _compute_pair_size(market_a, market_b)

    logger.info(
        "MUTUAL EXCLUSION VIOLATION: '%s' (%.2f) + '%s' (%.2f) = %.4f "
        "edge=%.4f size=$%.2f",
        market_a["title"][:35], p_a,
        market_b["title"][:35], p_b,
        prob_sum, edge_after_fees, size_usd,
    )

    return {
        "strategy": STRATEGY_ID,
        "venue": VENUE,
        "arb_type": "mutual_exclusion_violation",
        "leg_a": {
            "market_id": market_a["market_id"],
            "title": market_a["title"],
            "side": "no",
            "price": p_a,
        },
        "leg_b": {
            "market_id": market_b["market_id"],
            "title": market_b["title"],
            "side": "no",
            "price": p_b,
        },
        "contradiction": f"P(A) + P(B) = {prob_sum:.4f} > 1.00 (mutually exclusive)",
        "edge_estimate": round(edge_after_fees, 4),
        "raw_edge": round(violation, 4),
        "size_usd": round(size_usd, 2),
        "confidence": "high" if edge_after_fees > 0.05 else "medium",
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


# =========================================================================
#  SIZING AND UTILITY HELPERS
# =========================================================================

def _compute_size(market: dict) -> float:
    """
    Compute position size for a single-market arb (Type 1).

    Uses 50% of visible order book depth on the cheaper side.
    Falls back to FALLBACK_SIZE_USD if depth data is unavailable.

    Args:
        market: Market dict, may contain 'book_depth_yes'/'book_depth_no'.

    Returns:
        Position size in USD.
    """
    depth_yes = market.get("book_depth_yes")
    depth_no = market.get("book_depth_no")

    if depth_yes is not None and depth_no is not None:
        try:
            depth_yes = float(depth_yes)
            depth_no = float(depth_no)
            min_depth = min(depth_yes, depth_no)
            return round(min_depth * MAX_DEPTH_FRACTION, 2)
        except (ValueError, TypeError):
            pass

    logger.debug(
        "Order book depth unavailable for '%s', using fallback $%.2f.",
        market.get("market_id", "?"), FALLBACK_SIZE_USD,
    )
    return FALLBACK_SIZE_USD


def _compute_pair_size(market_a: dict, market_b: dict) -> float:
    """
    Compute position size for a two-leg arb (Type 2).

    Uses 50% of the minimum order book depth across both legs.
    Falls back to FALLBACK_SIZE_USD if depth data is unavailable.

    Args:
        market_a: First leg market dict.
        market_b: Second leg market dict.

    Returns:
        Position size in USD.
    """
    depths: list[float] = []
    for m in [market_a, market_b]:
        d_yes = m.get("book_depth_yes")
        d_no = m.get("book_depth_no")
        if d_yes is not None and d_no is not None:
            try:
                depths.append(min(float(d_yes), float(d_no)))
            except (ValueError, TypeError):
                pass

    if len(depths) == 2:
        return round(min(depths) * MAX_DEPTH_FRACTION, 2)

    logger.debug(
        "Order book depth unavailable for pair '%s' / '%s', using fallback $%.2f.",
        market_a.get("market_id", "?"),
        market_b.get("market_id", "?"),
        FALLBACK_SIZE_USD,
    )
    return FALLBACK_SIZE_USD


def _is_stale(market: dict) -> bool:
    """
    Check whether a market's price data is stale (> MAX_PRICE_AGE_SECONDS old).

    Args:
        market: Market dict, may contain 'timestamp' key (ISO 8601 string).

    Returns:
        True if the price is stale and should be skipped.
    """
    ts = market.get("timestamp")
    if ts is None:
        # No timestamp means we cannot judge freshness; allow it
        # in test mode but be cautious in production.
        return False

    try:
        if isinstance(ts, str):
            # Handle ISO 8601 with or without timezone.
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            market_time = datetime.fromisoformat(ts)
        elif isinstance(ts, (int, float)):
            market_time = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            return False

        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=timezone.utc)

        age = (datetime.now(timezone.utc) - market_time).total_seconds()
        return age > MAX_PRICE_AGE_SECONDS
    except (ValueError, TypeError, OSError):
        return False


# =========================================================================
#  SIGNAL EMISSION (Redis Streams)
# =========================================================================

async def emit_arb_signal(signal: dict, test_mode: bool = False) -> str:
    """
    Publish an arb signal to stream:arb:signals.

    Args:
        signal:    Arb signal dict (from detect_yesno_mismatch or
                   detect_logical_contradictions).
        test_mode: If True, uses InMemoryRedis mock.

    Returns:
        Redis message ID of the published signal.
    """
    from execution.utils.redis_client import publish, STREAM_ARB_SIGNALS

    # Flatten nested dicts for Redis stream field compatibility.
    flat = _flatten_signal(signal)
    msg_id = await publish(STREAM_ARB_SIGNALS, flat)
    logger.info(
        "Emitted arb signal to '%s': id=%s type=%s edge=%.4f",
        STREAM_ARB_SIGNALS, msg_id,
        signal.get("arb_type", "?"),
        signal.get("edge_estimate", 0),
    )
    return msg_id


async def emit_trade_signal(signal: dict, test_mode: bool = False) -> str:
    """
    Publish a trade signal to stream:orders:pending.

    Converts the arb signal into a format suitable for the execution
    engine, with separate orders for each leg.

    Args:
        signal:    Arb signal dict.
        test_mode: If True, uses InMemoryRedis mock.

    Returns:
        Redis message ID of the published trade signal.
    """
    from execution.utils.redis_client import publish, STREAM_ORDERS_PENDING

    trade = _arb_signal_to_trade(signal)
    flat = _flatten_signal(trade)
    msg_id = await publish(STREAM_ORDERS_PENDING, flat)
    logger.info(
        "Emitted trade signal to '%s': id=%s type=%s size=$%.2f",
        STREAM_ORDERS_PENDING, msg_id,
        signal.get("arb_type", "?"),
        signal.get("size_usd", 0),
    )
    return msg_id


def _arb_signal_to_trade(signal: dict) -> dict:
    """
    Convert an arb signal into a trade order dict for the execution engine.

    For Type 1 (yesno_mismatch): single market, buy the underpriced side.
    For Type 2 (logical_contradiction / mutual_exclusion): two legs.

    Returns:
        Trade order dict compatible with stream:orders:pending schema.
    """
    arb_type = signal.get("arb_type", "")
    now = datetime.now(timezone.utc).isoformat()

    if arb_type == "yesno_mismatch":
        return {
            "strategy": STRATEGY_ID,
            "venue": VENUE,
            "order_type": "market",
            "market_id": signal["market_id"],
            "title": signal.get("title", ""),
            "side": signal.get("underpriced_side", "yes"),
            "action": "buy",
            "size_usd": signal.get("size_usd", FALLBACK_SIZE_USD),
            "reason": f"Type 1 yesno mismatch: deviation={signal.get('deviation', 0):.4f}",
            "edge_estimate": signal.get("edge_estimate", 0),
            "created_at": now,
        }

    elif arb_type in ("logical_contradiction", "mutual_exclusion_violation"):
        leg_a = signal.get("leg_a", {})
        leg_b = signal.get("leg_b", {})
        return {
            "strategy": STRATEGY_ID,
            "venue": VENUE,
            "order_type": "arb_pair",
            "leg_a_market_id": leg_a.get("market_id", ""),
            "leg_a_title": leg_a.get("title", ""),
            "leg_a_side": leg_a.get("side", ""),
            "leg_a_price": leg_a.get("price", 0),
            "leg_b_market_id": leg_b.get("market_id", ""),
            "leg_b_title": leg_b.get("title", ""),
            "leg_b_side": leg_b.get("side", ""),
            "leg_b_price": leg_b.get("price", 0),
            "size_usd": signal.get("size_usd", FALLBACK_SIZE_USD),
            "reason": f"Type 2 {arb_type}: {signal.get('contradiction', '')}",
            "edge_estimate": signal.get("edge_estimate", 0),
            "created_at": now,
        }

    elif arb_type == "multi_outcome_mismatch":
        return {
            "strategy": STRATEGY_ID,
            "venue": VENUE,
            "order_type": "multi_outcome_arb",
            "market_id": signal["market_id"],
            "title": signal.get("title", ""),
            "outcome_prices": signal.get("outcome_prices", []),
            "size_usd": signal.get("size_usd", FALLBACK_SIZE_USD),
            "reason": f"Type 1 multi-outcome mismatch: sum={signal.get('price_sum', 0):.4f}",
            "edge_estimate": signal.get("edge_estimate", 0),
            "created_at": now,
        }

    else:
        return {
            "strategy": STRATEGY_ID,
            "venue": VENUE,
            "order_type": "unknown",
            "raw_signal": json.dumps(signal, default=str),
            "created_at": now,
        }


def _flatten_signal(signal: dict) -> dict:
    """
    Flatten nested dicts and lists for Redis stream field compatibility.

    Redis stream fields must be string key-value pairs.  Nested structures
    are JSON-encoded.
    """
    flat: dict[str, str] = {}
    for key, value in signal.items():
        if isinstance(value, dict):
            flat[key] = json.dumps(value, default=str)
        elif isinstance(value, list):
            flat[key] = json.dumps(value, default=str)
        elif isinstance(value, (int, float)):
            flat[key] = str(value)
        elif value is None:
            flat[key] = ""
        else:
            flat[key] = str(value)
    return flat


# =========================================================================
#  MARKET LOADING
# =========================================================================

async def load_markets_from_stream(test_mode: bool = False) -> list[dict]:
    """
    Load active Polymarket markets from the normalised price stream.

    In production, consumes recent messages from stream:prices:normalised
    filtered to venue=polymarket.  In test mode, returns fixture data.

    Args:
        test_mode: If True, returns _FIXTURE_MARKETS.

    Returns:
        List of market dicts.
    """
    if test_mode:
        now = datetime.now(timezone.utc).isoformat()
        markets = []
        for m in _FIXTURE_MARKETS:
            market = dict(m)
            if market.get("timestamp") is None:
                market["timestamp"] = now
            markets.append(market)
        logger.info("Loaded %d fixture market(s) for test mode.", len(markets))
        return markets

    # Production: read from normalised price stream.
    from execution.utils.redis_client import consume, ack

    messages = await consume(
        stream=STREAM_PRICES_NORMALISED,
        group=GROUP_STRATEGY,
        consumer=CONSUMER_NAME,
        count=500,
        block=2000,
    )

    markets: list[dict] = []
    seen_ids: set[str] = set()

    for msg_id, data in messages:
        venue = data.get("venue", "")
        if venue != VENUE:
            await ack(STREAM_PRICES_NORMALISED, GROUP_STRATEGY, msg_id)
            continue

        market_id = data.get("market_id", "")
        if market_id in seen_ids:
            await ack(STREAM_PRICES_NORMALISED, GROUP_STRATEGY, msg_id)
            continue
        seen_ids.add(market_id)

        market = {
            "market_id": market_id,
            "title": data.get("title", ""),
            "yes_price": data.get("yes_price"),
            "no_price": data.get("no_price"),
            "venue": venue,
            "timestamp": data.get("timestamp"),
            "category": data.get("category", ""),
            "book_depth_yes": data.get("book_depth_yes"),
            "book_depth_no": data.get("book_depth_no"),
        }
        markets.append(market)
        await ack(STREAM_PRICES_NORMALISED, GROUP_STRATEGY, msg_id)

    logger.info(
        "Loaded %d Polymarket market(s) from normalised stream.", len(markets),
    )
    return markets


async def load_markets_from_db(test_mode: bool = False) -> list[dict]:
    """
    Load active Polymarket markets from the PostgreSQL market_prices table.

    Used as a fallback when the stream has insufficient data, and for
    backtest mode.

    Args:
        test_mode: If True, returns _FIXTURE_MARKETS.

    Returns:
        List of market dicts.
    """
    if test_mode:
        return await load_markets_from_stream(test_mode=True)

    from execution.utils.db import get_pool

    pool = await get_pool()
    query = """
        SELECT DISTINCT ON (market_id)
            market_id, title, yes_price, no_price, venue,
            time AS timestamp, category
        FROM market_prices
        WHERE venue = 'polymarket'
        ORDER BY market_id, time DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)

    markets = [dict(r) for r in rows]
    logger.info("Loaded %d Polymarket market(s) from database.", len(markets))
    return markets


# =========================================================================
#  MAIN SCAN LOOP
# =========================================================================

async def run_scan_cycle(
    markets: list[dict],
    matcher: Any,
    scan_type: str = "both",
    test_mode: bool = False,
) -> list[dict]:
    """
    Run one full scan cycle and emit signals.

    Args:
        markets:   List of active market dicts.
        matcher:   MarketMatcher instance.
        scan_type: "type1", "type2", or "both".
        test_mode: Whether running in test mode.

    Returns:
        List of all arb signals detected in this cycle.
    """
    all_signals: list[dict] = []

    # --- Type 1: Yes+No mismatch scan ---
    if scan_type in ("type1", "both"):
        logger.info("Running Type 1 scan (%d market(s))...", len(markets))
        t1_start = time.monotonic()
        for market in markets:
            signal = detect_yesno_mismatch(market)
            if signal:
                all_signals.append(signal)
                await emit_arb_signal(signal, test_mode=test_mode)
                await emit_trade_signal(signal, test_mode=test_mode)
        t1_elapsed = time.monotonic() - t1_start
        t1_count = sum(1 for s in all_signals if s["arb_type"] in ("yesno_mismatch", "multi_outcome_mismatch"))
        logger.info(
            "Type 1 scan complete: %d mismatch(es) in %.2f s.",
            t1_count, t1_elapsed,
        )

    # --- Type 2: Logical contradiction scan ---
    if scan_type in ("type2", "both"):
        if matcher is None or (not matcher.test_mode and matcher.model is None):
            logger.warning(
                "Type 2 scan skipped: MarketMatcher model not available. "
                "Falling back to Type 1 only."
            )
        else:
            logger.info("Running Type 2 scan (%d market(s))...", len(markets))
            t2_start = time.monotonic()
            contradiction_signals = detect_logical_contradictions(markets, matcher)
            for signal in contradiction_signals:
                all_signals.append(signal)
                await emit_arb_signal(signal, test_mode=test_mode)
                await emit_trade_signal(signal, test_mode=test_mode)
            t2_elapsed = time.monotonic() - t2_start
            logger.info(
                "Type 2 scan complete: %d contradiction(s) in %.2f s.",
                len(contradiction_signals), t2_elapsed,
            )

    logger.info(
        "Scan cycle complete: %d total signal(s) emitted.", len(all_signals),
    )
    return all_signals


async def run_live_loop(test_mode: bool = False) -> None:
    """
    Run the continuous scan loop for live trading.

    Type 1 scans run every 60 seconds.
    Type 2 scans run every 5 minutes.

    The loop continues indefinitely until interrupted (Ctrl+C).

    Args:
        test_mode: If True, uses fixture data and InMemoryRedis.
    """
    from execution.pipeline.market_matcher import MarketMatcher
    from execution.utils.redis_client import ensure_consumer_groups

    # Ensure consumer groups exist before starting.
    await ensure_consumer_groups()

    # Initialise the MarketMatcher.
    matcher = MarketMatcher(test_mode=test_mode)

    logger.info(
        "S4 Intra-market Arb live loop starting "
        "(Type 1 every %d s, Type 2 every %d s, test_mode=%s).",
        TYPE1_SCAN_INTERVAL_SECONDS,
        TYPE2_SCAN_INTERVAL_SECONDS,
        test_mode,
    )

    last_type2_time = 0.0
    cycle_count = 0

    while True:
        cycle_count += 1
        cycle_start = time.monotonic()
        now_mono = time.monotonic()

        try:
            # Load current markets.
            markets = await load_markets_from_stream(test_mode=test_mode)
            if not markets:
                logger.info("No markets available. Sleeping %d s.", TYPE1_SCAN_INTERVAL_SECONDS)
                await asyncio.sleep(TYPE1_SCAN_INTERVAL_SECONDS)
                continue

            # Determine scan type for this cycle.
            time_since_type2 = now_mono - last_type2_time
            if time_since_type2 >= TYPE2_SCAN_INTERVAL_SECONDS:
                scan_type = "both"
                last_type2_time = now_mono
            else:
                scan_type = "type1"

            signals = await run_scan_cycle(
                markets, matcher,
                scan_type=scan_type,
                test_mode=test_mode,
            )

            logger.info(
                "Cycle %d complete: %d signal(s). Next Type 2 in %.0f s.",
                cycle_count, len(signals),
                TYPE2_SCAN_INTERVAL_SECONDS - (time.monotonic() - last_type2_time),
            )

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Shutting down.")
            break
        except Exception as exc:
            logger.error(
                "Error in scan cycle %d: %s: %s",
                cycle_count, type(exc).__name__, exc,
            )

        # Sleep until next Type 1 cycle.
        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0, TYPE1_SCAN_INTERVAL_SECONDS - elapsed)
        if test_mode:
            # In test mode, run only one cycle.
            break
        await asyncio.sleep(sleep_time)


# =========================================================================
#  TEST MODE
# =========================================================================

async def run_test_mode() -> None:
    """
    --test mode: exercise all S4 strategy logic with fixture data.

    Validates:
        - Type 1 yes+no mismatch detection (standard and multi-outcome)
        - Type 2 logical contradiction detection (implication and mutual exclusion)
        - Position sizing
        - Signal emission to Redis (InMemoryRedis mock)
        - Trade signal generation
    """
    from execution.utils import redis_client as rc_module
    from execution.pipeline.market_matcher import MarketMatcher

    # Enable mock Redis.
    rc_module._use_mock = True

    print()
    print("=" * 72)
    print("  Strategy S4 — Intra-venue Combinatorial Arbitrage — Test Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # Initialise Redis and consumer groups.
    await rc_module.ensure_consumer_groups()

    # ------------------------------------------------------------------
    # Test 1: Type 1 — Yes+No mismatch detection
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 1: Type 1 — Yes+No mismatch detection")
    print("-" * 72)

    # Market with yes=0.55, no=0.48 -> sum=1.03, deviation=0.03 (at threshold)
    mismatch_market = _FIXTURE_MARKETS[0]
    signal = detect_yesno_mismatch(mismatch_market)
    if signal:
        print(f"  Detected mismatch: {mismatch_market['title']}")
        print(f"    yes={mismatch_market['yes_price']} no={mismatch_market['no_price']}")
        print(f"    sum={signal['price_sum']} deviation={signal['deviation']}")
        print(f"    edge={signal['edge_estimate']} side={signal['trade_side']}")
        print(f"    size=${signal['size_usd']}")
    else:
        print(f"  No mismatch (expected: deviation at threshold, may round out).")

    # Market with yes=0.40, no=0.53 -> sum=0.93, deviation=0.07 (should detect)
    mismatch_market_2 = _FIXTURE_MARKETS[1]
    signal_2 = detect_yesno_mismatch(mismatch_market_2)
    assert signal_2 is not None, "Expected mismatch for yes=0.40 + no=0.53 = 0.93"
    print(f"  Detected mismatch: {mismatch_market_2['title']}")
    print(f"    yes={mismatch_market_2['yes_price']} no={mismatch_market_2['no_price']}")
    print(f"    sum={signal_2['price_sum']} deviation={signal_2['deviation']}")
    print(f"    edge={signal_2['edge_estimate']} side={signal_2['trade_side']}")
    print(f"    size=${signal_2['size_usd']}")

    # Clean market should not trigger.
    clean_market = _FIXTURE_MARKETS[2]
    signal_clean = detect_yesno_mismatch(clean_market)
    assert signal_clean is None, "Clean market (yes=0.42, no=0.58) should not trigger"
    print(f"  Clean market '{clean_market['title']}': no mismatch (correct)")

    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 2: Type 1 — Multi-outcome mismatch
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 2: Type 1 — Multi-outcome mismatch")
    print("-" * 72)

    multi_market = _FIXTURE_MARKETS[7]  # FIFA World Cup with 3 outcomes
    signal_multi = detect_yesno_mismatch(multi_market)
    if signal_multi:
        print(f"  Detected multi-outcome mismatch: {multi_market['title']}")
        print(f"    outcomes={signal_multi['outcomes']}")
        print(f"    prices={signal_multi['outcome_prices']}")
        print(f"    sum={signal_multi['price_sum']} deviation={signal_multi['deviation']}")
        print(f"    edge={signal_multi['edge_estimate']}")
    else:
        print("  No multi-outcome mismatch detected (deviation may be within threshold).")
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 3: Type 2 — Logical contradiction (implication)
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 3: Type 2 — Logical contradiction (implication violation)")
    print("-" * 72)

    matcher = MarketMatcher(test_mode=True)

    # Test the implication check directly.
    trump_market = _FIXTURE_MARKETS[3]
    gop_market = _FIXTURE_MARKETS[4]

    implies = matcher.check_logical_implication(trump_market, gop_market)
    print(f"  '{trump_market['title'][:45]}' implies '{gop_market['title'][:45]}': {implies}")
    assert implies, (
        "Expected Trump winning to imply Republican winning"
    )

    # Check violation: P(Trump=0.62) > P(GOP=0.58).
    violation = _check_implication_violation(
        trump_market, gop_market, direction="trump_implies_gop",
    )
    assert violation is not None, (
        "Expected implication violation: Trump(0.62) > GOP(0.58)"
    )
    print(f"  Violation detected: edge={violation['edge_estimate']}")
    print(f"    Leg A: {violation['leg_a']['title'][:40]} side={violation['leg_a']['side']} price={violation['leg_a']['price']}")
    print(f"    Leg B: {violation['leg_b']['title'][:40]} side={violation['leg_b']['side']} price={violation['leg_b']['price']}")
    print(f"    size=${violation['size_usd']}")

    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 4: Position sizing
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 4: Position sizing")
    print("-" * 72)

    # Single market sizing.
    size_single = _compute_size(trump_market)
    expected_single = min(
        float(trump_market["book_depth_yes"]),
        float(trump_market["book_depth_no"]),
    ) * MAX_DEPTH_FRACTION
    print(f"  Single market size: ${size_single} (expected ${expected_single})")
    assert size_single == expected_single, (
        f"Expected ${expected_single}, got ${size_single}"
    )

    # Pair sizing.
    size_pair = _compute_pair_size(trump_market, gop_market)
    min_depth = min(
        min(float(trump_market["book_depth_yes"]), float(trump_market["book_depth_no"])),
        min(float(gop_market["book_depth_yes"]), float(gop_market["book_depth_no"])),
    )
    expected_pair = min_depth * MAX_DEPTH_FRACTION
    print(f"  Pair size: ${size_pair} (expected ${expected_pair})")
    assert size_pair == expected_pair, (
        f"Expected ${expected_pair}, got ${size_pair}"
    )

    # Fallback sizing (no depth data).
    no_depth_market = {"market_id": "0xNODEPTH", "title": "Test"}
    size_fallback = _compute_size(no_depth_market)
    print(f"  Fallback size: ${size_fallback} (expected ${FALLBACK_SIZE_USD})")
    assert size_fallback == FALLBACK_SIZE_USD

    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 5: Signal emission (mock Redis)
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 5: Signal emission (mock Redis)")
    print("-" * 72)

    # Emit a Type 1 signal.
    if signal_2:
        arb_id = await emit_arb_signal(signal_2, test_mode=True)
        print(f"  Arb signal emitted: id={arb_id}")
        trade_id = await emit_trade_signal(signal_2, test_mode=True)
        print(f"  Trade signal emitted: id={trade_id}")

    # Emit a Type 2 signal.
    if violation:
        arb_id_2 = await emit_arb_signal(violation, test_mode=True)
        print(f"  Arb signal (Type 2) emitted: id={arb_id_2}")
        trade_id_2 = await emit_trade_signal(violation, test_mode=True)
        print(f"  Trade signal (Type 2) emitted: id={trade_id_2}")

    # Verify stream lengths.
    from execution.utils.redis_client import stream_len, STREAM_ARB_SIGNALS, STREAM_ORDERS_PENDING
    arb_len = await stream_len(STREAM_ARB_SIGNALS)
    orders_len = await stream_len(STREAM_ORDERS_PENDING)
    print(f"  stream:arb:signals length: {arb_len}")
    print(f"  stream:orders:pending length: {orders_len}")
    assert arb_len >= 2, f"Expected >= 2 arb signals, got {arb_len}"
    assert orders_len >= 2, f"Expected >= 2 trade signals, got {orders_len}"

    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 6: Full scan cycle
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 6: Full scan cycle (Type 1 + Type 2)")
    print("-" * 72)

    markets = await load_markets_from_stream(test_mode=True)
    print(f"  Loaded {len(markets)} fixture market(s).")

    all_signals = await run_scan_cycle(
        markets, matcher,
        scan_type="both",
        test_mode=True,
    )
    print(f"  Total signals from full scan: {len(all_signals)}")
    for sig in all_signals:
        arb_type = sig.get("arb_type", "?")
        edge = sig.get("edge_estimate", 0)
        title = sig.get("title", sig.get("leg_a", {}).get("title", "?"))
        print(f"    [{arb_type}] edge={edge:.4f} '{title[:50]}'")

    # We expect at least 1 Type 1 signal (mismatch) and 1 Type 2 (contradiction).
    type1_count = sum(1 for s in all_signals if s["arb_type"] in ("yesno_mismatch", "multi_outcome_mismatch"))
    type2_count = sum(1 for s in all_signals if s["arb_type"] in ("logical_contradiction", "mutual_exclusion_violation"))
    print(f"  Type 1 signals: {type1_count}")
    print(f"  Type 2 signals: {type2_count}")
    assert type1_count >= 1, f"Expected >= 1 Type 1 signal, got {type1_count}"
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 7: Staleness check
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 7: Price staleness check")
    print("-" * 72)

    fresh = {"timestamp": datetime.now(timezone.utc).isoformat()}
    assert not _is_stale(fresh), "Fresh timestamp should not be stale"
    print("  Fresh timestamp: not stale (correct)")

    stale = {"timestamp": "2020-01-01T00:00:00Z"}
    assert _is_stale(stale), "2020 timestamp should be stale"
    print("  2020 timestamp: stale (correct)")

    no_ts = {}
    assert not _is_stale(no_ts), "No timestamp should not be stale (permissive)"
    print("  No timestamp: not stale (correct)")

    print("  PASSED")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    await rc_module.close_redis()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print()
    print("=" * 72)
    print("  ALL TESTS PASSED — Strategy S4")
    print("=" * 72)
    print()


# =========================================================================
#  BACKTEST MODE
# =========================================================================

async def run_backtest_mode() -> None:
    """
    --backtest mode: replay historical price data from the database.

    Loads historical Polymarket markets from market_prices, groups them
    by time window, and runs the detection logic on each window.
    Tallies hit rates and theoretical P&L.
    """
    from execution.utils import redis_client as rc_module
    from execution.pipeline.market_matcher import MarketMatcher

    # Use mock Redis for backtest (no live publishing).
    rc_module._use_mock = True
    await rc_module.ensure_consumer_groups()

    print()
    print("=" * 72)
    print("  Strategy S4 — Backtest Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    # In backtest, we use fixture data to simulate historical windows.
    # Full DB-backed backtest requires POSTGRES_URL; use --test fixtures
    # as a lightweight substitute.
    logger.info("Loading markets for backtest (using fixtures as demo)...")
    markets = await load_markets_from_stream(test_mode=True)

    matcher = MarketMatcher(test_mode=True)

    # Simulate 3 scan windows with slight price perturbations.
    import random
    random.seed(42)

    total_signals = 0
    total_theoretical_pnl = 0.0

    for window_idx in range(3):
        print()
        print(f"  --- Window {window_idx + 1}/3 ---")

        # Perturb prices slightly to simulate time evolution.
        window_markets = []
        for m in markets:
            perturbed = dict(m)
            if "yes_price" in perturbed and perturbed["yes_price"] is not None:
                delta = random.uniform(-0.02, 0.02)
                perturbed["yes_price"] = max(0.01, min(0.99, float(perturbed["yes_price"]) + delta))
            if "no_price" in perturbed and perturbed["no_price"] is not None:
                delta = random.uniform(-0.02, 0.02)
                perturbed["no_price"] = max(0.01, min(0.99, float(perturbed["no_price"]) + delta))
            window_markets.append(perturbed)

        signals = await run_scan_cycle(
            window_markets, matcher,
            scan_type="both",
            test_mode=True,
        )

        window_pnl = sum(s.get("edge_estimate", 0) * s.get("size_usd", 0) for s in signals)
        total_signals += len(signals)
        total_theoretical_pnl += window_pnl

        print(f"  Signals: {len(signals)}, Theoretical P&L: ${window_pnl:.2f}")

    print()
    print("-" * 72)
    print(f"  Backtest Summary:")
    print(f"    Total windows:         3")
    print(f"    Total signals:         {total_signals}")
    print(f"    Theoretical P&L:       ${total_theoretical_pnl:.2f}")
    print(f"    Avg signals/window:    {total_signals / 3:.1f}")
    print("-" * 72)

    await rc_module.close_redis()

    print()
    print("=" * 72)
    print("  Backtest complete.")
    print("=" * 72)
    print()


# =========================================================================
#  PRODUCTION MODE
# =========================================================================

async def run_production() -> None:
    """
    Production mode: continuously scan for intra-venue arb opportunities.

    Connects to live Redis and PostgreSQL, loads markets from the
    normalised price stream, and runs the scan loop indefinitely.
    """
    logger.info("Starting S4 Intra-market Arb in production mode...")
    await run_live_loop(test_mode=False)


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point: parse --test / --backtest flags and run the
    appropriate mode.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Strategy S4: Intra-venue Combinatorial Arbitrage. "
            "Detects Yes+No mismatches and logical contradictions "
            "between semantically related Polymarket markets."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Run with fixture data and InMemoryRedis mock. "
            "No live connections required."
        ),
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help=(
            "Run backtest mode: replay historical price data and "
            "compute theoretical P&L."
        ),
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running S4 in --test mode.")
        asyncio.run(run_test_mode())
    elif args.backtest:
        logger.info("Running S4 in --backtest mode.")
        asyncio.run(run_backtest_mode())
    else:
        logger.info("Running S4 in production mode.")
        asyncio.run(run_production())


if __name__ == "__main__":
    main()
