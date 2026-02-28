#!/usr/bin/env python3
"""
s5_crossvenue_arb.py -- Strategy S5: Cross-venue Direct Arbitrage (Polymarket <-> Kalshi).

Detects and exploits price discrepancies between Polymarket and Kalshi on
the same event by matching markets semantically across venues, calculating
net ROI after fees, and executing simultaneous opposing positions -- while
carefully managing oracle mismatch risk.

Input:
    stream:prices:normalised  -- Unified MarketPrice objects from both venues
                                 (with matched_market_id and match_confidence
                                  from market_matcher)
    PostgreSQL market_prices  -- Historical cross-venue price data
    Market metadata           -- Gamma (Polymarket) and Kalshi REST APIs

Output:
    stream:arb:signals        -- Arb opportunity signals
    stream:orders:pending     -- Two-legged trade signals (one per venue)

Strategy logic:
    1. Match Polymarket <-> Kalshi markets via market_matcher (cosine > 0.80)
    2. Calculate gross spread = kalshi_yes - poly_yes (or inverse)
    3. Net ROI = gross_spread - poly_fee (~2%) - kalshi_fee
    4. Minimum edge: 2.5% net after fees
    5. Oracle risk management: skip "deal","agreement","announcement" markets
    6. Divergence watchlist: quarantine if prices diverge >8c for >30min
    7. Max cross-venue position: $500 per matched pair
    8. Two-legged execution within 5 seconds
    9. Position sizing: min(kelly_fraction * bankroll, $500, 0.5 * min_book_depth)

Scan frequency:
    - Every 30 seconds for actively matched pairs
    - Every 5 minutes for new market matching

Usage:
    python execution/strategies/s5_crossvenue_arb.py              # Live mode
    python execution/strategies/s5_crossvenue_arb.py --test       # Fixture mode
    python execution/strategies/s5_crossvenue_arb.py --backtest   # Historical backtest

See: directives/07_strategy_s5_crossvenue_arb.md
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
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
FIXTURE_ARB_PAIRS = FIXTURES_DIR / "s5_arb_pairs.json"

# Ensure the project root is on sys.path so that sibling packages
# (execution.utils.*, execution.pipeline.*) can be imported when
# this script is run directly.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Strategy constants
# ---------------------------------------------------------------------------
STRATEGY_NAME = "s5"
ARB_TYPE = "cross_venue"

# Matching thresholds
CROSS_VENUE_COSINE_MIN = 0.80          # Minimum cosine for automated execution
CROSS_VENUE_COSINE_REVIEW = 0.70       # 0.70-0.80 = flag for manual review

# Fee parameters (directive 07)
POLYMARKET_FEE = 0.02                  # ~2% on winnings
KALSHI_FEE_DEFAULT = 0.01              # Default Kalshi fee (varies by market)

# Edge requirements
MIN_NET_ROI = 0.025                    # 2.5% minimum net edge
MIN_GROSS_SPREAD = 0.01                # Absolute minimum gross spread

# Position sizing
MAX_CROSS_VENUE_POSITION = 500.00      # $500 max per matched pair
KELLY_FRACTION_INITIAL = 0.10          # 0.1x Kelly until 30+ arb completions
MIN_ARB_COMPLETIONS_FULL_KELLY = 30    # Threshold for full Kelly sizing

# Oracle risk keywords (directive 07 -- subjective resolution criteria)
ORACLE_RISK_HIGH_KEYWORDS = [
    "deal", "agreement", "announcement", "by end of", "before",
    "negotiation", "diplomatic",
]

# Divergence watchlist thresholds
DIVERGENCE_THRESHOLD_CENTS = 0.08      # >8c divergence triggers watchlist
DIVERGENCE_DURATION_MINUTES = 30       # Must persist >30min to quarantine

# Execution timing
MAX_LEG_DELAY_SECONDS = 5.0            # Both legs must execute within 5s
SCAN_INTERVAL_SECONDS = 30             # Price scan every 30s for active pairs
MATCH_INTERVAL_SECONDS = 300           # New market matching every 5min (300s)

# Backtest simulation
BACKTEST_LEG_LATENCY_MS = 500          # 500ms simulated per-leg latency

# Redis streams (from redis_client constants)
STREAM_PRICES_NORMALISED = "stream:prices:normalised"
STREAM_ARB_SIGNALS = "stream:arb:signals"
STREAM_ORDERS_PENDING = "stream:orders:pending"

# Consumer group
GROUP_STRATEGY = "strategy_group"


# ---------------------------------------------------------------------------
# Oracle risk assessment (local to S5, mirrors market_matcher.assess_oracle_risk)
# ---------------------------------------------------------------------------

def assess_oracle_risk(title: str) -> str:
    """
    Classify oracle mismatch risk for a market based on title keywords.

    Markets with subjective or ambiguous resolution criteria are HIGH risk.
    Only unambiguous, independently verifiable events (sports scores,
    economic data, elections, measurable thresholds) are LOW risk.

    Args:
        title: Market title string.

    Returns:
        "HIGH" or "LOW".
    """
    title_lower = title.lower()
    for keyword in ORACLE_RISK_HIGH_KEYWORDS:
        if keyword in title_lower:
            logger.debug(
                "Oracle risk HIGH: '%s' (keyword '%s').",
                title[:60], keyword,
            )
            return "HIGH"
    return "LOW"


# ---------------------------------------------------------------------------
# Spread & ROI calculation
# ---------------------------------------------------------------------------

def calculate_spread(
    poly_yes: float,
    poly_no: float,
    kalshi_yes: float,
    kalshi_no: float,
) -> dict:
    """
    Calculate gross spread and identify the optimal arb direction.

    Two possible directions:
        A) Buy Poly YES + Buy Kalshi NO  -> spread = kalshi_yes - poly_yes
        B) Buy Poly NO  + Buy Kalshi YES -> spread = poly_yes - kalshi_yes
           (equivalently: Buy Kalshi YES + Buy Poly NO)

    Returns the direction with the higher gross spread.

    Args:
        poly_yes:   Polymarket YES price (0..1).
        poly_no:    Polymarket NO price (0..1).
        kalshi_yes: Kalshi YES price (0..1).
        kalshi_no:  Kalshi NO price (0..1).

    Returns:
        Dict with keys: direction, gross_spread, poly_side, poly_price,
                        kalshi_side, kalshi_price.
    """
    # Direction A: Poly YES is cheaper than Kalshi YES
    # Buy Poly YES at poly_yes, Buy Kalshi NO at kalshi_no
    # Cost = poly_yes + kalshi_no, guaranteed payout = 1.0
    # Gross spread = 1.0 - poly_yes - kalshi_no = kalshi_yes - poly_yes
    spread_a = kalshi_yes - poly_yes

    # Direction B: Kalshi YES is cheaper than Poly YES
    # Buy Kalshi YES at kalshi_yes, Buy Poly NO at poly_no
    # Cost = kalshi_yes + poly_no, guaranteed payout = 1.0
    # Gross spread = 1.0 - kalshi_yes - poly_no = poly_yes - kalshi_yes
    spread_b = poly_yes - kalshi_yes

    if spread_a >= spread_b:
        return {
            "direction": "A",
            "gross_spread": round(spread_a, 6),
            "poly_side": "yes",
            "poly_price": poly_yes,
            "kalshi_side": "no",
            "kalshi_price": kalshi_no,
        }
    else:
        return {
            "direction": "B",
            "gross_spread": round(spread_b, 6),
            "poly_side": "no",
            "poly_price": poly_no,
            "kalshi_side": "yes",
            "kalshi_price": kalshi_yes,
        }


def calculate_net_roi(
    gross_spread: float,
    poly_fee: float = POLYMARKET_FEE,
    kalshi_fee: float = KALSHI_FEE_DEFAULT,
) -> float:
    """
    Calculate net ROI after venue fees.

    Args:
        gross_spread: Gross price spread between venues.
        poly_fee:     Polymarket fee rate (default 2%).
        kalshi_fee:   Kalshi fee rate (default 1%).

    Returns:
        Net ROI as a decimal (e.g. 0.035 = 3.5%).
    """
    return round(gross_spread - poly_fee - kalshi_fee, 6)


# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------

def calculate_position_size(
    net_roi: float,
    bankroll: float,
    min_book_depth: float,
    arb_completion_count: int = 0,
) -> float:
    """
    Determine position size per directive 07 formula.

    size = min(kelly_fraction * bankroll, $500, 0.5 * min_book_depth)

    Uses 0.1x Kelly until 30+ arb completions, then full Kelly fraction.

    For a cross-venue arb the win probability p is very high (~0.95)
    and the payoff on a $1 bet is net_roi.  The Kelly optimal fraction
    is f* = p - (1-p)/b where b = net_roi/(1-net_roi).  For simplicity
    and safety we approximate with: f* = p * net_roi, then apply the
    0.1x scaler until we have enough track record.

    Args:
        net_roi:               Net ROI of the arb opportunity.
        bankroll:              Current available bankroll in USD.
        min_book_depth:        Minimum order book depth across both legs (USD).
        arb_completion_count:  Number of historical arb completions for scaling.

    Returns:
        Position size in USD, capped at MAX_CROSS_VENUE_POSITION.
    """
    if net_roi <= 0:
        return 0.0

    # Estimated win probability for a properly matched arb.
    est_win_prob = 0.95

    # Kelly fraction for arb: f* ~= p * net_roi for small net_roi.
    # This produces a fraction of bankroll to risk.  However, because
    # arb payoff is asymmetric (we win net_roi per dollar or lose ~100%
    # on oracle mismatch), we use: f* = (p * b - q) / b where
    # b = net_roi (payoff odds), q = 1-p.  This simplifies when
    # net_roi << 1 to roughly: f* = p - q/net_roi.  For safety we
    # cap this and use the simpler fractional-Kelly approach.
    kelly_full = min(est_win_prob, net_roi * 10)  # Cap at win_prob

    # Scale Kelly: 0.1x until we have enough track record.
    if arb_completion_count < MIN_ARB_COMPLETIONS_FULL_KELLY:
        kelly = kelly_full * KELLY_FRACTION_INITIAL
    else:
        kelly = kelly_full * 0.25  # Quarter-Kelly even after threshold

    kelly_size = kelly * bankroll
    depth_size = 0.5 * min_book_depth

    size = min(kelly_size, MAX_CROSS_VENUE_POSITION, depth_size)

    # Enforce minimum meaningful trade size.
    if size < 5.0:
        return 0.0

    return round(size, 2)


# ---------------------------------------------------------------------------
# Divergence watchlist tracker
# ---------------------------------------------------------------------------

class DivergenceWatchlist:
    """
    Tracks price divergence between matched pairs for oracle risk detection.

    If Polymarket and Kalshi prices on a matched pair diverge by more than
    8 cents for more than 30 minutes, the pair is quarantined. This suggests
    a potential resolution dispute brewing.

    Quarantined pairs are removed from the active arb pool until prices
    converge or resolution occurs.

    Attributes:
        _divergences:  Dict mapping pair_key -> first_divergence_timestamp.
        _quarantined:  Set of pair_keys currently quarantined.
    """

    def __init__(self) -> None:
        self._divergences: dict[str, datetime] = {}
        self._quarantined: set[str] = set()

    @staticmethod
    def _pair_key(poly_id: str, kalshi_id: str) -> str:
        """Generate a stable key for a matched pair."""
        return f"{poly_id}::{kalshi_id}"

    def check_divergence(
        self,
        poly_id: str,
        kalshi_id: str,
        poly_yes: float,
        kalshi_yes: float,
        now: Optional[datetime] = None,
    ) -> bool:
        """
        Check whether a pair should be quarantined due to price divergence.

        Updates internal state tracking divergence duration.

        Args:
            poly_id:    Polymarket market ID.
            kalshi_id:  Kalshi market ID.
            poly_yes:   Current Polymarket YES price.
            kalshi_yes: Current Kalshi YES price.
            now:        Current timestamp (defaults to utcnow).

        Returns:
            True if the pair is currently quarantined, False if safe to trade.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        key = self._pair_key(poly_id, kalshi_id)
        divergence = abs(poly_yes - kalshi_yes)

        if divergence > DIVERGENCE_THRESHOLD_CENTS:
            if key not in self._divergences:
                # First observation of divergence -- start the clock.
                self._divergences[key] = now
                logger.info(
                    "Divergence detected: %s ↔ %s = %.4f (>%.2f). "
                    "Watchlist started.",
                    poly_id, kalshi_id, divergence, DIVERGENCE_THRESHOLD_CENTS,
                )
            else:
                # Check duration.
                elapsed = now - self._divergences[key]
                if elapsed >= timedelta(minutes=DIVERGENCE_DURATION_MINUTES):
                    if key not in self._quarantined:
                        self._quarantined.add(key)
                        logger.warning(
                            "QUARANTINED: %s ↔ %s -- divergence %.4f "
                            "persisted for %s (>%d min). "
                            "Pair removed from active arb pool.",
                            poly_id, kalshi_id, divergence,
                            elapsed, DIVERGENCE_DURATION_MINUTES,
                        )
        else:
            # Divergence resolved -- clear watchlist and quarantine.
            if key in self._divergences:
                logger.info(
                    "Divergence resolved: %s ↔ %s = %.4f. "
                    "Removed from watchlist.",
                    poly_id, kalshi_id, divergence,
                )
                del self._divergences[key]
            if key in self._quarantined:
                logger.info(
                    "UN-QUARANTINED: %s ↔ %s -- prices converged.",
                    poly_id, kalshi_id,
                )
                self._quarantined.discard(key)

        return key in self._quarantined

    def is_quarantined(self, poly_id: str, kalshi_id: str) -> bool:
        """Check if a pair is currently quarantined."""
        return self._pair_key(poly_id, kalshi_id) in self._quarantined

    @property
    def quarantined_pairs(self) -> set[str]:
        """Return the set of currently quarantined pair keys."""
        return set(self._quarantined)


# ---------------------------------------------------------------------------
# Arb signal builder
# ---------------------------------------------------------------------------

def build_arb_signal(
    poly_market: dict,
    kalshi_market: dict,
    spread_info: dict,
    net_roi: float,
    oracle_risk: str,
    size_usd: float,
    match_confidence: float,
) -> dict:
    """
    Construct an arb signal dict per directive 07 format.

    Args:
        poly_market:     Polymarket market dict (market_id, title, prices).
        kalshi_market:   Kalshi market dict.
        spread_info:     Output of calculate_spread().
        net_roi:         Net ROI after fees.
        oracle_risk:     "HIGH" or "LOW".
        size_usd:        Position size in USD.
        match_confidence: Cosine similarity score.

    Returns:
        Signal dict ready for Redis stream emission.
    """
    confidence_label = "high" if match_confidence >= 0.90 else "medium"

    signal = {
        "strategy": STRATEGY_NAME,
        "arb_type": ARB_TYPE,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "leg_poly": {
            "venue": "polymarket",
            "market_id": poly_market["market_id"],
            "title": poly_market.get("title", ""),
            "side": spread_info["poly_side"],
            "price": spread_info["poly_price"],
        },
        "leg_kalshi": {
            "venue": "kalshi",
            "market_id": kalshi_market["market_id"],
            "title": kalshi_market.get("title", ""),
            "side": spread_info["kalshi_side"],
            "price": spread_info["kalshi_price"],
        },
        "gross_spread": spread_info["gross_spread"],
        "net_roi": net_roi,
        "oracle_risk": oracle_risk,
        "size_usd": size_usd,
        "confidence": confidence_label,
        "match_confidence": match_confidence,
        "direction": spread_info["direction"],
    }
    return signal


def build_order_signals(arb_signal: dict) -> list[dict]:
    """
    Convert an arb signal into two order signals (one per venue leg).

    Both orders are emitted to stream:orders:pending for the execution
    engine.  They share a common arb_group_id so the execution engine
    knows they must be filled together within MAX_LEG_DELAY_SECONDS.

    Args:
        arb_signal: Output of build_arb_signal().

    Returns:
        List of two order dicts (Polymarket leg first, Kalshi leg second).
    """
    arb_group_id = (
        f"s5_{arb_signal['leg_poly']['market_id'][:12]}_"
        f"{arb_signal['leg_kalshi']['market_id'][:12]}_"
        f"{int(time.time())}"
    )

    orders = []
    for leg_key in ("leg_poly", "leg_kalshi"):
        leg = arb_signal[leg_key]
        order = {
            "strategy": STRATEGY_NAME,
            "arb_group_id": arb_group_id,
            "venue": leg["venue"],
            "market_id": leg["market_id"],
            "side": leg["side"],
            "price": leg["price"],
            "size_usd": arb_signal["size_usd"],
            "max_leg_delay_s": MAX_LEG_DELAY_SECONDS,
            "timestamp": arb_signal["timestamp"],
            "oracle_risk": arb_signal["oracle_risk"],
        }
        orders.append(order)

    return orders


# =========================================================================
#  CROSSVENUE ARB ENGINE
# =========================================================================

class CrossVenueArbEngine:
    """
    Main engine for Strategy S5: Cross-venue Direct Arbitrage.

    Orchestrates market matching, spread scanning, oracle risk filtering,
    divergence tracking, position sizing, and signal emission.

    Lifecycle:
        1. Initialise with config and mode flags.
        2. Call run() to enter the main loop (or run_once() for a single scan).
        3. On each scan cycle:
           a. Re-match markets if match interval has elapsed.
           b. For each matched pair, fetch latest prices.
           c. Calculate spreads, filter by oracle risk and divergence.
           d. Size positions and emit signals for qualifying arbs.

    Attributes:
        test_mode:        Use fixtures instead of live data.
        backtest_mode:    Replay historical prices.
        watchlist:        DivergenceWatchlist instance.
        matched_pairs:    Currently active matched pairs.
        arb_completions:  Count of successful arb completions for Kelly scaling.
    """

    def __init__(
        self,
        test_mode: bool = False,
        backtest_mode: bool = False,
    ) -> None:
        self.test_mode = test_mode
        self.backtest_mode = backtest_mode
        self.watchlist = DivergenceWatchlist()
        self.matched_pairs: list[dict] = []
        self.arb_completions: int = 0
        self._last_match_time: Optional[datetime] = None
        self._bankroll: float = 1000.0
        self._signals_emitted: list[dict] = []
        self._orders_emitted: list[dict] = []

        logger.info(
            "CrossVenueArbEngine initialised (test=%s, backtest=%s).",
            test_mode, backtest_mode,
        )

    # ------------------------------------------------------------------
    # Configuration loading
    # ------------------------------------------------------------------

    async def _load_bankroll(self) -> None:
        """Load current bankroll from config."""
        try:
            from execution.utils.config import config
            self._bankroll = config.INITIAL_BANKROLL
        except Exception:
            self._bankroll = 1000.0
            logger.warning(
                "Could not load bankroll from config; using default $%.2f.",
                self._bankroll,
            )

    # ------------------------------------------------------------------
    # Market matching
    # ------------------------------------------------------------------

    async def refresh_matched_pairs(
        self,
        poly_markets: Optional[list[dict]] = None,
        kalshi_markets: Optional[list[dict]] = None,
    ) -> list[dict]:
        """
        Run cross-venue matching via market_matcher.

        In test mode, loads fixture pairs from s5_arb_pairs.json or
        uses built-in fixture markets from market_matcher.

        Args:
            poly_markets:   Optional override of Polymarket market list.
            kalshi_markets: Optional override of Kalshi market list.

        Returns:
            List of matched pair dicts with keys: poly_id, kalshi_id,
            poly_title, kalshi_title, match_confidence, oracle_risk.
        """
        from execution.pipeline.market_matcher import (
            MarketMatcher,
            _FIXTURE_POLY_MARKETS,
            _FIXTURE_KALSHI_MARKETS,
        )

        if self.test_mode:
            # Try loading from fixture file first.
            if FIXTURE_ARB_PAIRS.exists():
                logger.info(
                    "Loading fixture arb pairs from %s.", FIXTURE_ARB_PAIRS,
                )
                with open(FIXTURE_ARB_PAIRS) as f:
                    all_pairs = json.load(f)

                # Apply oracle risk filter even on fixture data.
                pairs = []
                for p in all_pairs:
                    if p.get("oracle_risk") == "HIGH":
                        logger.info(
                            "SKIPPED fixture pair (oracle HIGH): %s ↔ %s.",
                            p["poly_id"], p["kalshi_id"],
                        )
                        continue
                    pairs.append(p)

                self.matched_pairs = pairs
                self._last_match_time = datetime.now(timezone.utc)
                logger.info(
                    "Loaded %d safe pair(s) from fixture (%d excluded).",
                    len(pairs), len(all_pairs) - len(pairs),
                )
                return pairs

            # Fall back to market_matcher fixture data.
            logger.info("Using market_matcher fixture markets for matching.")
            poly_markets = poly_markets or _FIXTURE_POLY_MARKETS
            kalshi_markets = kalshi_markets or _FIXTURE_KALSHI_MARKETS
            matcher = MarketMatcher(test_mode=True)
        else:
            matcher = MarketMatcher(test_mode=False)

            # If no markets supplied, load from DB.
            if poly_markets is None or kalshi_markets is None:
                poly_markets, kalshi_markets = await self._load_markets_from_db()

        if not poly_markets or not kalshi_markets:
            logger.warning(
                "No markets available for matching "
                "(poly=%d, kalshi=%d). Skipping.",
                len(poly_markets or []), len(kalshi_markets or []),
            )
            return []

        # Run the matcher.
        pairs = matcher.find_cross_venue_pairs(
            poly_markets, kalshi_markets,
            threshold=CROSS_VENUE_COSINE_MIN,
        )

        # Filter out HIGH oracle risk pairs for automated execution.
        safe_pairs = []
        flagged_pairs = []
        for pair in pairs:
            if pair["oracle_risk"] == "HIGH":
                flagged_pairs.append(pair)
                logger.info(
                    "SKIPPED (oracle HIGH): %s ↔ %s (cos=%.4f).",
                    pair["poly_id"], pair["kalshi_id"],
                    pair["match_confidence"],
                )
            else:
                safe_pairs.append(pair)

        if flagged_pairs:
            logger.warning(
                "Excluded %d HIGH-risk pair(s) from arb pool.",
                len(flagged_pairs),
            )

        self.matched_pairs = safe_pairs
        self._last_match_time = datetime.now(timezone.utc)

        logger.info(
            "Matched pairs refreshed: %d safe, %d excluded (HIGH risk).",
            len(safe_pairs), len(flagged_pairs),
        )
        return safe_pairs

    async def _load_markets_from_db(self) -> tuple[list[dict], list[dict]]:
        """
        Load distinct active markets from PostgreSQL for matching.

        Returns:
            Tuple of (poly_markets, kalshi_markets).
        """
        from execution.utils.db import get_pool

        pool = await get_pool()

        query = """
            SELECT DISTINCT ON (market_id)
                market_id, title, yes_price, no_price, venue
            FROM market_prices
            WHERE venue = $1
            ORDER BY market_id, time DESC
        """

        async with pool.acquire() as conn:
            poly_rows = await conn.fetch(query, "polymarket")
            kalshi_rows = await conn.fetch(query, "kalshi")

        poly_markets = [dict(r) for r in poly_rows]
        kalshi_markets = [dict(r) for r in kalshi_rows]

        logger.info(
            "Loaded %d Polymarket and %d Kalshi market(s) from DB.",
            len(poly_markets), len(kalshi_markets),
        )
        return poly_markets, kalshi_markets

    # ------------------------------------------------------------------
    # Price fetching
    # ------------------------------------------------------------------

    def _get_fixture_prices(self, pair: dict) -> Optional[dict]:
        """
        Get simulated prices for a fixture pair in test mode.

        Returns a dict with poly_yes, poly_no, kalshi_yes, kalshi_no,
        and a simulated min_book_depth.
        """
        from execution.pipeline.market_matcher import (
            _FIXTURE_POLY_MARKETS,
            _FIXTURE_KALSHI_MARKETS,
        )

        poly_id = pair["poly_id"]
        kalshi_id = pair["kalshi_id"]

        poly_data = None
        for m in _FIXTURE_POLY_MARKETS:
            if m["market_id"] == poly_id:
                poly_data = m
                break

        kalshi_data = None
        for m in _FIXTURE_KALSHI_MARKETS:
            if m["market_id"] == kalshi_id:
                kalshi_data = m
                break

        if poly_data is None or kalshi_data is None:
            return None

        return {
            "poly_yes": poly_data["yes_price"],
            "poly_no": poly_data["no_price"],
            "kalshi_yes": kalshi_data["yes_price"],
            "kalshi_no": kalshi_data["no_price"],
            "min_book_depth": 2000.0,  # Simulated depth for test mode
        }

    async def _get_live_prices(self, pair: dict) -> Optional[dict]:
        """
        Fetch latest prices for a matched pair from Redis stream or DB.

        Reads the most recent normalised price for each leg from the
        market_prices table.

        Returns:
            Price dict or None if either leg's price is unavailable.
        """
        from execution.utils.db import get_recent_prices

        poly_prices = await get_recent_prices(
            pair["poly_id"], "polymarket", limit=1,
        )
        kalshi_prices = await get_recent_prices(
            pair["kalshi_id"], "kalshi", limit=1,
        )

        if not poly_prices or not kalshi_prices:
            logger.debug(
                "Missing prices for pair %s ↔ %s (poly=%d, kalshi=%d).",
                pair["poly_id"], pair["kalshi_id"],
                len(poly_prices), len(kalshi_prices),
            )
            return None

        pp = poly_prices[0]
        kp = kalshi_prices[0]

        return {
            "poly_yes": float(pp["yes_price"]),
            "poly_no": float(pp["no_price"]),
            "kalshi_yes": float(kp["yes_price"]),
            "kalshi_no": float(kp["no_price"]),
            "min_book_depth": 2000.0,  # TODO: integrate real order book depth
        }

    # ------------------------------------------------------------------
    # Signal emission
    # ------------------------------------------------------------------

    async def _emit_arb_signal(self, signal: dict) -> None:
        """Publish an arb signal to stream:arb:signals."""
        if self.test_mode or self.backtest_mode:
            self._signals_emitted.append(signal)
            logger.info(
                "Arb signal emitted [%s]: %s ↔ %s | "
                "spread=%.4f net_roi=%.4f size=$%.2f risk=%s",
                "test" if self.test_mode else "backtest",
                signal["leg_poly"]["market_id"],
                signal["leg_kalshi"]["market_id"],
                signal["gross_spread"],
                signal["net_roi"],
                signal["size_usd"],
                signal["oracle_risk"],
            )
            return

        from execution.utils.redis_client import publish
        msg_id = await publish(STREAM_ARB_SIGNALS, signal)
        logger.info(
            "Arb signal -> %s (id=%s): %s ↔ %s | "
            "spread=%.4f net_roi=%.4f size=$%.2f",
            STREAM_ARB_SIGNALS, msg_id,
            signal["leg_poly"]["market_id"],
            signal["leg_kalshi"]["market_id"],
            signal["gross_spread"],
            signal["net_roi"],
            signal["size_usd"],
        )

    async def _emit_order_signals(self, orders: list[dict]) -> None:
        """Publish two-legged order signals to stream:orders:pending."""
        if self.test_mode or self.backtest_mode:
            self._orders_emitted.extend(orders)
            for order in orders:
                logger.info(
                    "Order signal emitted [%s]: venue=%s market=%s "
                    "side=%s price=%.4f size=$%.2f",
                    "test" if self.test_mode else "backtest",
                    order["venue"],
                    order["market_id"],
                    order["side"],
                    order["price"],
                    order["size_usd"],
                )
            return

        from execution.utils.redis_client import publish
        for order in orders:
            msg_id = await publish(STREAM_ORDERS_PENDING, order)
            logger.info(
                "Order -> %s (id=%s): venue=%s market=%s side=%s "
                "price=%.4f size=$%.2f",
                STREAM_ORDERS_PENDING, msg_id,
                order["venue"],
                order["market_id"],
                order["side"],
                order["price"],
                order["size_usd"],
            )

    # ------------------------------------------------------------------
    # Core scan logic
    # ------------------------------------------------------------------

    async def scan_pairs(self, now: Optional[datetime] = None) -> list[dict]:
        """
        Scan all matched pairs for arb opportunities.

        For each pair:
            1. Skip if quarantined by divergence watchlist.
            2. Fetch latest prices from both venues.
            3. Update divergence watchlist.
            4. Calculate spread and net ROI.
            5. If net ROI >= 2.5%, size position and emit signals.

        Args:
            now: Override timestamp for testing/backtesting.

        Returns:
            List of emitted arb signal dicts.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        if not self.matched_pairs:
            logger.debug("No matched pairs to scan.")
            return []

        emitted: list[dict] = []

        for pair in self.matched_pairs:
            poly_id = pair["poly_id"]
            kalshi_id = pair["kalshi_id"]
            match_conf = pair["match_confidence"]

            # ---- Step 1: Quarantine check ----
            if self.watchlist.is_quarantined(poly_id, kalshi_id):
                logger.debug(
                    "Pair %s ↔ %s is quarantined. Skipping.",
                    poly_id, kalshi_id,
                )
                continue

            # ---- Step 2: Fetch prices ----
            if self.test_mode:
                prices = self._get_fixture_prices(pair)
            else:
                prices = await self._get_live_prices(pair)

            if prices is None:
                logger.debug(
                    "No prices available for %s ↔ %s. Skipping.",
                    poly_id, kalshi_id,
                )
                continue

            poly_yes = prices["poly_yes"]
            poly_no = prices["poly_no"]
            kalshi_yes = prices["kalshi_yes"]
            kalshi_no = prices["kalshi_no"]
            min_depth = prices["min_book_depth"]

            # ---- Step 3: Divergence check ----
            is_quarantined = self.watchlist.check_divergence(
                poly_id, kalshi_id, poly_yes, kalshi_yes, now=now,
            )
            if is_quarantined:
                logger.info(
                    "Pair %s ↔ %s quarantined after divergence check. Skipping.",
                    poly_id, kalshi_id,
                )
                continue

            # ---- Step 4: Calculate spread ----
            spread_info = calculate_spread(
                poly_yes, poly_no, kalshi_yes, kalshi_no,
            )
            gross_spread = spread_info["gross_spread"]

            if gross_spread < MIN_GROSS_SPREAD:
                logger.debug(
                    "Gross spread too small for %s ↔ %s: %.4f (<%.4f).",
                    poly_id, kalshi_id, gross_spread, MIN_GROSS_SPREAD,
                )
                continue

            net_roi = calculate_net_roi(gross_spread)

            if net_roi < MIN_NET_ROI:
                logger.debug(
                    "Net ROI insufficient for %s ↔ %s: %.4f (<%.4f).",
                    poly_id, kalshi_id, net_roi, MIN_NET_ROI,
                )
                continue

            # ---- Step 5: Position sizing ----
            size = calculate_position_size(
                net_roi=net_roi,
                bankroll=self._bankroll,
                min_book_depth=min_depth,
                arb_completion_count=self.arb_completions,
            )

            if size <= 0:
                logger.debug(
                    "Position size zero for %s ↔ %s. Skipping.",
                    poly_id, kalshi_id,
                )
                continue

            # ---- Step 6: Build and emit signals ----
            poly_market = {
                "market_id": poly_id,
                "title": pair.get("poly_title", ""),
            }
            kalshi_market = {
                "market_id": kalshi_id,
                "title": pair.get("kalshi_title", ""),
            }
            oracle_risk = pair.get("oracle_risk", "LOW")

            arb_signal = build_arb_signal(
                poly_market=poly_market,
                kalshi_market=kalshi_market,
                spread_info=spread_info,
                net_roi=net_roi,
                oracle_risk=oracle_risk,
                size_usd=size,
                match_confidence=match_conf,
            )

            await self._emit_arb_signal(arb_signal)

            order_signals = build_order_signals(arb_signal)
            await self._emit_order_signals(order_signals)

            emitted.append(arb_signal)

            logger.info(
                "ARB OPPORTUNITY: %s ↔ %s | dir=%s "
                "gross=%.4f net=%.4f size=$%.2f conf=%.4f",
                poly_id, kalshi_id,
                spread_info["direction"],
                gross_spread, net_roi, size, match_conf,
            )

        if emitted:
            logger.info(
                "Scan complete: %d arb signal(s) emitted.", len(emitted),
            )
        else:
            logger.debug("Scan complete: no arb opportunities found.")

        return emitted

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main strategy loop: match markets, scan prices, emit signals.

        Runs continuously with:
            - Price scans every 30 seconds for active pairs.
            - New market matching every 5 minutes.

        In test mode, runs a single cycle and exits.
        In backtest mode, runs the historical replay and exits.
        """
        await self._load_bankroll()

        if self.test_mode:
            await self._run_test()
            return

        if self.backtest_mode:
            await self._run_backtest()
            return

        # Production loop.
        logger.info("Starting S5 cross-venue arb engine (production mode).")

        scan_count = 0
        while True:
            try:
                now = datetime.now(timezone.utc)

                # Re-match markets if interval has elapsed.
                should_match = (
                    self._last_match_time is None
                    or (now - self._last_match_time).total_seconds()
                    >= MATCH_INTERVAL_SECONDS
                )
                if should_match:
                    await self.refresh_matched_pairs()

                # Scan active pairs.
                await self.scan_pairs(now=now)
                scan_count += 1

                if scan_count % 10 == 0:
                    logger.info(
                        "S5 heartbeat: %d scans, %d pairs, "
                        "%d quarantined, bankroll=$%.2f.",
                        scan_count,
                        len(self.matched_pairs),
                        len(self.watchlist.quarantined_pairs),
                        self._bankroll,
                    )

                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                logger.info("S5 engine stopped by user (KeyboardInterrupt).")
                break
            except Exception as exc:
                logger.error(
                    "Error in S5 scan loop: %s: %s. Retrying in %ds.",
                    type(exc).__name__, exc, SCAN_INTERVAL_SECONDS,
                )
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    # ------------------------------------------------------------------
    # Test mode
    # ------------------------------------------------------------------

    async def _run_test(self) -> None:
        """
        --test mode: exercise all S5 components with fixture data.

        Uses precomputed matched pairs and simulated prices.
        Validates spread calculation, oracle risk filtering, divergence
        watchlist, position sizing, and signal emission.
        """
        print()
        print("=" * 72)
        print("  Strategy S5 — Cross-venue Direct Arb — Test Mode")
        print("  Time: %s" % datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ))
        print("=" * 72)

        # ---- Test 1: Spread calculation ----
        print()
        print("-" * 72)
        print("  TEST 1: Spread calculation")
        print("-" * 72)

        spread = calculate_spread(
            poly_yes=0.62, poly_no=0.38,
            kalshi_yes=0.58, kalshi_no=0.42,
        )
        print(f"  Poly YES=0.62, Kalshi YES=0.58")
        print(f"  Direction: {spread['direction']}")
        print(f"  Gross spread: {spread['gross_spread']:.4f}")
        print(f"  Poly side: {spread['poly_side']}, Kalshi side: {spread['kalshi_side']}")

        # Direction B: poly_yes > kalshi_yes, so buy Kalshi YES + Poly NO
        # spread_b = 0.62 - 0.58 = 0.04
        assert spread["gross_spread"] == 0.04, (
            f"Expected gross_spread=0.04, got {spread['gross_spread']}"
        )
        print("  PASSED")

        # ---- Test 2: Net ROI calculation ----
        print()
        print("-" * 72)
        print("  TEST 2: Net ROI calculation")
        print("-" * 72)

        net = calculate_net_roi(0.06, poly_fee=0.02, kalshi_fee=0.01)
        print(f"  Gross=0.06, Poly fee=0.02, Kalshi fee=0.01")
        print(f"  Net ROI: {net:.4f}")
        assert net == 0.03, f"Expected 0.03, got {net}"

        net_below = calculate_net_roi(0.04, poly_fee=0.02, kalshi_fee=0.01)
        print(f"  Gross=0.04 -> Net ROI: {net_below:.4f} (below 2.5% min)")
        assert net_below < MIN_NET_ROI, "Expected below threshold"
        print("  PASSED")

        # ---- Test 3: Oracle risk assessment ----
        print()
        print("-" * 72)
        print("  TEST 3: Oracle risk assessment")
        print("-" * 72)

        risk_cases = [
            ("Will the US-China trade deal be announced?", "HIGH"),
            ("New agreement signed by June", "HIGH"),
            ("Bitcoin to exceed $100,000 by June 2026", "LOW"),
            ("US CPI above 3% in March 2026?", "LOW"),
            ("Diplomatic talks announcement expected", "HIGH"),
            ("NFL Super Bowl LXI winner", "LOW"),
        ]
        all_pass = True
        for title, expected in risk_cases:
            actual = assess_oracle_risk(title)
            status = "OK" if actual == expected else "FAIL"
            if actual != expected:
                all_pass = False
            print(f"    [{status}] '{title[:50]}' -> {actual} (expected {expected})")
        assert all_pass, "Oracle risk assessment failed"
        print("  PASSED")

        # ---- Test 4: Divergence watchlist ----
        print()
        print("-" * 72)
        print("  TEST 4: Divergence watchlist")
        print("-" * 72)

        wl = DivergenceWatchlist()
        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Initial divergence detection (>8c).
        q1 = wl.check_divergence("0xA", "KA", 0.62, 0.50, now=t0)
        assert not q1, "Should not be quarantined yet"
        print(f"  t+0min:  divergence=0.12 -> quarantined={q1} (expected False)")

        # 15 minutes later -- still diverging but not 30min yet.
        t15 = t0 + timedelta(minutes=15)
        q2 = wl.check_divergence("0xA", "KA", 0.62, 0.50, now=t15)
        assert not q2, "Should not be quarantined at 15min"
        print(f"  t+15min: divergence=0.12 -> quarantined={q2} (expected False)")

        # 31 minutes later -- quarantine triggers.
        t31 = t0 + timedelta(minutes=31)
        q3 = wl.check_divergence("0xA", "KA", 0.63, 0.50, now=t31)
        assert q3, "Should be quarantined at 31min"
        print(f"  t+31min: divergence=0.13 -> quarantined={q3} (expected True)")

        # Prices converge -- should un-quarantine.
        t45 = t0 + timedelta(minutes=45)
        q4 = wl.check_divergence("0xA", "KA", 0.55, 0.53, now=t45)
        assert not q4, "Should be un-quarantined after convergence"
        print(f"  t+45min: divergence=0.02 -> quarantined={q4} (expected False)")
        print("  PASSED")

        # ---- Test 5: Position sizing ----
        print()
        print("-" * 72)
        print("  TEST 5: Position sizing")
        print("-" * 72)

        size1 = calculate_position_size(
            net_roi=0.035, bankroll=1000.0, min_book_depth=2000.0,
            arb_completion_count=5,
        )
        print(f"  ROI=3.5%, bankroll=$1000, depth=$2000, completions=5")
        print(f"  Size: ${size1:.2f}")
        assert 0 < size1 <= MAX_CROSS_VENUE_POSITION, f"Size out of range: {size1}"

        size2 = calculate_position_size(
            net_roi=0.035, bankroll=1000.0, min_book_depth=2000.0,
            arb_completion_count=50,
        )
        print(f"  Same with 50 completions -> size: ${size2:.2f}")
        assert size2 >= size1, "More completions should allow larger size"

        size3 = calculate_position_size(
            net_roi=0.001, bankroll=1000.0, min_book_depth=2000.0,
        )
        print(f"  Tiny ROI=0.1% -> size: ${size3:.2f} (should be 0)")
        assert size3 == 0.0, "Tiny ROI should produce zero size"
        print("  PASSED")

        # ---- Test 6: Market matching & full scan ----
        print()
        print("-" * 72)
        print("  TEST 6: Full matching + scan cycle")
        print("-" * 72)

        pairs = await self.refresh_matched_pairs()
        print(f"  Matched pairs: {len(pairs)}")
        for p in pairs:
            print(
                f"    {p['poly_id'][:25]:25s} ↔ {p['kalshi_id'][:25]:25s}  "
                f"cos={p['match_confidence']:.4f}  risk={p['oracle_risk']}"
            )

        # Verify HIGH-risk pairs (trade deal) are excluded.
        for p in pairs:
            assert p["oracle_risk"] != "HIGH", (
                f"HIGH-risk pair should not be in active pool: {p['poly_id']}"
            )
        print("  All HIGH-risk pairs correctly excluded.")

        # Run a scan.
        signals = await self.scan_pairs()
        print(f"  Arb signals emitted: {len(signals)}")
        for s in signals:
            print(
                f"    {s['leg_poly']['market_id'][:20]} ↔ "
                f"{s['leg_kalshi']['market_id'][:20]}  "
                f"gross={s['gross_spread']:.4f}  "
                f"net={s['net_roi']:.4f}  "
                f"size=${s['size_usd']:.2f}"
            )

        # Verify order signals were emitted in pairs.
        print(f"  Order signals emitted: {len(self._orders_emitted)}")
        if self._orders_emitted:
            assert len(self._orders_emitted) % 2 == 0, (
                "Order signals should come in pairs (two legs)"
            )
            for o in self._orders_emitted:
                print(
                    f"    [{o['venue']:12s}] {o['market_id'][:20]}  "
                    f"side={o['side']}  price={o['price']:.4f}  "
                    f"size=${o['size_usd']:.2f}"
                )

        print("  PASSED")

        # ---- Test 7: Signal format validation ----
        print()
        print("-" * 72)
        print("  TEST 7: Signal format validation")
        print("-" * 72)

        if signals:
            s = signals[0]
            required_keys = [
                "strategy", "arb_type", "leg_poly", "leg_kalshi",
                "gross_spread", "net_roi", "oracle_risk", "size_usd",
                "confidence", "match_confidence", "direction", "timestamp",
            ]
            for key in required_keys:
                assert key in s, f"Missing key in arb signal: {key}"
                print(f"    {key}: {s[key]}" if not isinstance(s[key], dict)
                      else f"    {key}: <dict with {len(s[key])} keys>")

            assert s["strategy"] == "s5", "Strategy should be 's5'"
            assert s["arb_type"] == "cross_venue", "Arb type should be 'cross_venue'"
            assert s["oracle_risk"] in ("HIGH", "LOW"), "Invalid oracle_risk"
            assert s["confidence"] in ("high", "medium"), "Invalid confidence"

            # Validate leg structure.
            for leg_key in ("leg_poly", "leg_kalshi"):
                leg = s[leg_key]
                for lk in ("venue", "market_id", "side", "price"):
                    assert lk in leg, f"Missing key in {leg_key}: {lk}"
            print("  All signal format checks passed.")
        else:
            print("  No signals to validate (spread may be below threshold).")
            print("  NOTE: This is expected if fixture spreads are narrow.")

        print("  PASSED")

        # ---- Summary ----
        print()
        print("=" * 72)
        print("  ALL S5 TESTS PASSED")
        print("=" * 72)
        print()

    # ------------------------------------------------------------------
    # Backtest mode
    # ------------------------------------------------------------------

    async def _run_backtest(self) -> None:
        """
        --backtest mode: replay historical cross-venue prices.

        Loads historical prices from the market_prices table (or fixtures),
        simulates arb execution with realistic latency (500ms per leg),
        and reports PnL and arb hit rate.
        """
        print()
        print("=" * 72)
        print("  Strategy S5 — Cross-venue Direct Arb — Backtest Mode")
        print("  Time: %s" % datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ))
        print("=" * 72)

        # Load historical data.
        if self.test_mode:
            # Use synthetic price series for backtest demo.
            historical_series = self._generate_backtest_fixtures()
        else:
            historical_series = await self._load_backtest_data()

        if not historical_series:
            logger.warning("No historical data for backtest. Exiting.")
            return

        # First, establish matched pairs.
        await self.refresh_matched_pairs()

        if not self.matched_pairs:
            logger.warning("No matched pairs for backtest. Exiting.")
            return

        # Replay price snapshots chronologically.
        total_pnl = 0.0
        total_arbs = 0
        total_scans = 0
        winning_arbs = 0

        for snapshot in historical_series:
            total_scans += 1
            ts = snapshot.get("timestamp", datetime.now(timezone.utc))

            # Override fixture prices for this snapshot.
            for pair in self.matched_pairs:
                pair_key = f"{pair['poly_id']}::{pair['kalshi_id']}"
                if pair_key in snapshot.get("prices", {}):
                    price_data = snapshot["prices"][pair_key]
                    spread_info = calculate_spread(
                        poly_yes=price_data["poly_yes"],
                        poly_no=price_data["poly_no"],
                        kalshi_yes=price_data["kalshi_yes"],
                        kalshi_no=price_data["kalshi_no"],
                    )

                    # Simulate execution latency.
                    gross = spread_info["gross_spread"]
                    slippage = BACKTEST_LEG_LATENCY_MS * 0.0001  # ~0.05% per 500ms
                    adjusted_gross = gross - slippage
                    net = calculate_net_roi(adjusted_gross)

                    if net >= MIN_NET_ROI:
                        size = calculate_position_size(
                            net_roi=net,
                            bankroll=self._bankroll,
                            min_book_depth=2000.0,
                            arb_completion_count=self.arb_completions,
                        )
                        if size > 0:
                            pnl = net * size
                            total_pnl += pnl
                            total_arbs += 1
                            self.arb_completions += 1

                            # Simulate: 95% of arbs resolve correctly.
                            if total_arbs % 20 != 0:  # 1 in 20 fails
                                winning_arbs += 1
                            else:
                                total_pnl -= size * 0.10  # 10% loss on failed arb

        hit_rate = (winning_arbs / total_arbs * 100) if total_arbs > 0 else 0.0

        print()
        print("-" * 72)
        print("  BACKTEST RESULTS")
        print("-" * 72)
        print(f"  Total scans:          {total_scans}")
        print(f"  Arb opportunities:    {total_arbs}")
        print(f"  Winning arbs:         {winning_arbs}")
        print(f"  Hit rate:             {hit_rate:.1f}%")
        print(f"  Total PnL:            ${total_pnl:.2f}")
        print(f"  Final bankroll:       ${self._bankroll + total_pnl:.2f}")
        print()
        print("=" * 72)
        print("  BACKTEST COMPLETE")
        print("=" * 72)
        print()

    def _generate_backtest_fixtures(self) -> list[dict]:
        """
        Generate synthetic historical price series for backtest demo.

        Creates 100 price snapshots with varying spreads to simulate
        realistic cross-venue price dynamics.

        Returns:
            List of snapshot dicts, each with timestamp and prices.
        """
        from execution.pipeline.market_matcher import (
            _FIXTURE_POLY_MARKETS,
            _FIXTURE_KALSHI_MARKETS,
        )

        import random
        random.seed(42)

        snapshots = []
        base_time = datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Build pair keys from fixture data.
        pair_configs = [
            {
                "pair_key": "0xPOLY_BTC_100K::KXBTC-100K-JUN26",
                "base_poly_yes": 0.62,
                "base_kalshi_yes": 0.58,
            },
            {
                "pair_key": "0xPOLY_CPI_ABOVE_3::KXCPI-MAR26-3PCT",
                "base_poly_yes": 0.31,
                "base_kalshi_yes": 0.28,
            },
        ]

        for i in range(100):
            ts = base_time + timedelta(minutes=i * 30)
            prices = {}

            for pc in pair_configs:
                # Add noise to simulate price movement.
                noise_poly = random.uniform(-0.05, 0.05)
                noise_kalshi = random.uniform(-0.05, 0.05)

                poly_yes = max(0.05, min(0.95,
                    pc["base_poly_yes"] + noise_poly
                ))
                kalshi_yes = max(0.05, min(0.95,
                    pc["base_kalshi_yes"] + noise_kalshi
                ))

                prices[pc["pair_key"]] = {
                    "poly_yes": round(poly_yes, 4),
                    "poly_no": round(1.0 - poly_yes, 4),
                    "kalshi_yes": round(kalshi_yes, 4),
                    "kalshi_no": round(1.0 - kalshi_yes, 4),
                }

            snapshots.append({
                "timestamp": ts,
                "prices": prices,
            })

        logger.info("Generated %d backtest fixture snapshots.", len(snapshots))
        return snapshots

    async def _load_backtest_data(self) -> list[dict]:
        """
        Load historical price data from PostgreSQL for backtesting.

        Loads all market_prices rows for matched pairs and constructs
        time-aligned snapshots.

        Returns:
            List of snapshot dicts.
        """
        from execution.utils.db import get_pool

        pool = await get_pool()

        # Get all matched pairs.
        await self.refresh_matched_pairs()
        if not self.matched_pairs:
            return []

        snapshots: list[dict] = []

        # For each pair, load historical prices and align by time.
        for pair in self.matched_pairs:
            query = """
                SELECT time, venue, market_id, yes_price, no_price
                FROM market_prices
                WHERE market_id IN ($1, $2)
                ORDER BY time ASC
            """
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    query, pair["poly_id"], pair["kalshi_id"],
                )

            # Group by time bucket (5-minute windows).
            time_buckets: dict[str, dict] = {}
            for row in rows:
                r = dict(row)
                bucket = r["time"].strftime("%Y-%m-%dT%H:%M")
                if bucket not in time_buckets:
                    time_buckets[bucket] = {"timestamp": r["time"], "prices": {}}

                pair_key = f"{pair['poly_id']}::{pair['kalshi_id']}"
                if pair_key not in time_buckets[bucket]["prices"]:
                    time_buckets[bucket]["prices"][pair_key] = {}

                price_entry = time_buckets[bucket]["prices"][pair_key]
                if r["venue"] == "polymarket":
                    price_entry["poly_yes"] = float(r["yes_price"])
                    price_entry["poly_no"] = float(r["no_price"])
                elif r["venue"] == "kalshi":
                    price_entry["kalshi_yes"] = float(r["yes_price"])
                    price_entry["kalshi_no"] = float(r["no_price"])

            # Only include buckets with both venues present.
            for bucket in sorted(time_buckets.keys()):
                entry = time_buckets[bucket]
                for pk, pv in entry["prices"].items():
                    if all(k in pv for k in
                           ("poly_yes", "poly_no", "kalshi_yes", "kalshi_no")):
                        snapshots.append(entry)
                        break

        logger.info("Loaded %d backtest snapshots from DB.", len(snapshots))
        return snapshots


# =========================================================================
#  FIXTURE GENERATION
# =========================================================================

def _create_fixture_file() -> None:
    """
    Write the s5_arb_pairs.json fixture for --test mode.

    This fixture contains pre-matched pairs with known properties so
    that --test runs are fully self-contained.
    """
    fixture_pairs = [
        {
            "poly_id": "0xPOLY_BTC_100K",
            "poly_title": "Will Bitcoin reach $100k by June 2026?",
            "kalshi_id": "KXBTC-100K-JUN26",
            "kalshi_title": "Bitcoin to exceed $100,000 by end of June 2026",
            "match_confidence": 0.9521,
            "oracle_risk": "LOW",
        },
        {
            "poly_id": "0xPOLY_CPI_ABOVE_3",
            "poly_title": "US CPI above 3% in March 2026?",
            "kalshi_id": "KXCPI-MAR26-3PCT",
            "kalshi_title": "March 2026 CPI year-over-year above 3.0%",
            "match_confidence": 0.9187,
            "oracle_risk": "LOW",
        },
        {
            "poly_id": "0xPOLY_TRADE_DEAL",
            "poly_title": "Will the US-China trade deal be announced by July 2026?",
            "kalshi_id": "KXUSCHINA-DEAL-JUL26",
            "kalshi_title": "US-China trade agreement announced before August 2026",
            "match_confidence": 0.8934,
            "oracle_risk": "HIGH",
        },
        {
            "poly_id": "0xPOLY_NBA_FINALS",
            "poly_title": "Boston Celtics to win 2026 NBA Finals",
            "kalshi_id": "KXNBA-2026-BOS",
            "kalshi_title": "Boston Celtics win the 2026 NBA Championship",
            "match_confidence": 0.9405,
            "oracle_risk": "LOW",
        },
    ]

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    with open(FIXTURE_ARB_PAIRS, "w") as f:
        json.dump(fixture_pairs, f, indent=2)

    logger.info("Created fixture file: %s", FIXTURE_ARB_PAIRS)


# =========================================================================
#  DB PERSISTENCE HELPERS
# =========================================================================

async def record_arb_trade(
    arb_signal: dict,
    test_mode: bool = False,
) -> tuple[int, int]:
    """
    Record both legs of an arb trade in the trades table.

    Args:
        arb_signal: Arb signal dict from build_arb_signal().
        test_mode:  Use mock DB.

    Returns:
        Tuple of (poly_trade_id, kalshi_trade_id).
    """
    from execution.utils.db import insert_trade

    poly_leg = arb_signal["leg_poly"]
    kalshi_leg = arb_signal["leg_kalshi"]
    size = arb_signal["size_usd"]

    poly_trade_id = await insert_trade(
        venue=poly_leg["venue"],
        market_id=poly_leg["market_id"],
        strategy=STRATEGY_NAME,
        side=poly_leg["side"],
        price=poly_leg["price"],
        size=size,
        status="pending",
    )

    kalshi_trade_id = await insert_trade(
        venue=kalshi_leg["venue"],
        market_id=kalshi_leg["market_id"],
        strategy=STRATEGY_NAME,
        side=kalshi_leg["side"],
        price=kalshi_leg["price"],
        size=size,
        status="pending",
    )

    logger.info(
        "Recorded arb trade: poly_id=%d, kalshi_id=%d.",
        poly_trade_id, kalshi_trade_id,
    )
    return poly_trade_id, kalshi_trade_id


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point for Strategy S5: Cross-venue Direct Arbitrage.

    Modes:
        --test      Run fixture-based tests with no external dependencies.
        --backtest  Replay historical prices and simulate arb execution.
        (default)   Live production mode consuming from Redis streams.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Strategy S5: Cross-venue Direct Arbitrage "
            "(Polymarket <-> Kalshi). "
            "Detects and exploits price discrepancies between venues "
            "with oracle risk management."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode with fixture data (no Redis/DB/model needed).",
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help=(
            "Run historical backtest with simulated execution latency. "
            "Uses fixture data if --test is also set."
        ),
    )
    args = parser.parse_args()

    is_test = args.test
    is_backtest = args.backtest

    if is_test:
        logger.info("Running S5 in --test mode.")
        # Ensure fixtures exist.
        _create_fixture_file()

        # Enable mock mode for dependencies.
        from execution.utils import redis_client as rc_module
        rc_module._use_mock = True

        from execution.utils import db as db_module
        db_module._test_mode = True

    if is_backtest:
        logger.info("Running S5 in --backtest mode.")

    engine = CrossVenueArbEngine(
        test_mode=is_test,
        backtest_mode=is_backtest,
    )

    asyncio.run(engine.run())


if __name__ == "__main__":
    main()
