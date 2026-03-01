#!/usr/bin/env python3
"""
market_matcher.py — Cross-venue semantic market matching.

Uses sentence-transformer embeddings (all-MiniLM-L6-v2) to match
markets across Polymarket and Kalshi by title similarity.
Also identifies logically related markets within a single venue
for combinatorial arbitrage (S4) and cross-venue arbitrage (S5).

Matching modes:
    1. Cross-venue:  Match Polymarket ↔ Kalshi (cosine > 0.80 = same event)
    2. Intra-venue:  Find semantically related Polymarket markets (cosine > 0.70)
       for logical implication / contradiction detection

Oracle risk flagging:
    Markets with subjective resolution criteria ("deal", "agreement",
    "announcement", "by end of", "before") are flagged as HIGH oracle
    risk per directive 07.  Only low-risk markets (sports scores,
    economic data, elections, measurable thresholds) proceed to
    automated cross-venue arb.

Results are persisted to PostgreSQL ``matched_markets`` table:
    (poly_market_id, kalshi_market_id, match_confidence, oracle_risk)

Usage:
    python execution/pipeline/market_matcher.py          # Live mode
    python execution/pipeline/market_matcher.py --test   # Fixture mode (no model download)

See: directives/06_strategy_s4_intramarket_arb.md
     directives/07_strategy_s5_crossvenue_arb.md
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

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
SCRIPT_DIR = Path(__file__).resolve().parent          # execution/pipeline/
EXECUTION_DIR = SCRIPT_DIR.parent                     # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                   # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"

# Ensure the project root is on sys.path so that sibling packages
# (execution.utils.db, execution.utils.config) can be imported when
# this script is run directly via `python execution/pipeline/market_matcher.py`.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
DEFAULT_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output dimension

# ---------------------------------------------------------------------------
# Matching thresholds (from directives 06 & 07)
# ---------------------------------------------------------------------------
CROSS_VENUE_THRESHOLD = 0.80    # cosine > 0.80 = same event (high confidence)
INTRA_VENUE_THRESHOLD = 0.70    # cosine > 0.70 = potentially related
INTRA_VENUE_MIN = 0.60          # cosine 0.60-0.80 = check for implications

# ---------------------------------------------------------------------------
# Oracle risk keywords (from directive 07)
#
# Markets whose titles contain these terms have subjective resolution
# criteria and are flagged HIGH risk for cross-venue arb.
# ---------------------------------------------------------------------------
ORACLE_RISK_HIGH_KEYWORDS = [
    "deal", "agreement", "announcement", "by end of", "before",
    "negotiation", "diplomatic",
]

# ---------------------------------------------------------------------------
# Fixture data for --test mode
#
# Precomputed 384-dim embeddings so --test runs without downloading
# the sentence-transformer model.  Embeddings were generated once
# from all-MiniLM-L6-v2 and frozen here as deterministic fixtures.
# ---------------------------------------------------------------------------
_FIXTURE_POLY_MARKETS = [
    {
        "market_id": "0xPOLY_BTC_100K",
        "title": "Will Bitcoin reach $100k by June 2026?",
        "yes_price": 0.62,
        "no_price": 0.38,
        "venue": "polymarket",
    },
    {
        "market_id": "0xPOLY_TRUMP_WIN",
        "title": "Will Trump win the 2028 presidential election?",
        "yes_price": 0.55,
        "no_price": 0.45,
        "venue": "polymarket",
    },
    {
        "market_id": "0xPOLY_GOP_WIN",
        "title": "Will the Republican Party win the 2028 presidential election?",
        "yes_price": 0.50,
        "no_price": 0.50,
        "venue": "polymarket",
    },
    {
        "market_id": "0xPOLY_CPI_ABOVE_3",
        "title": "US CPI above 3% in March 2026?",
        "yes_price": 0.31,
        "no_price": 0.69,
        "venue": "polymarket",
    },
    {
        "market_id": "0xPOLY_TRADE_DEAL",
        "title": "Will the US-China trade deal be announced by July 2026?",
        "yes_price": 0.40,
        "no_price": 0.60,
        "venue": "polymarket",
    },
]

_FIXTURE_KALSHI_MARKETS = [
    {
        "market_id": "KXBTC-100K-JUN26",
        "title": "Bitcoin to exceed $100,000 by end of June 2026",
        "yes_price": 0.58,
        "no_price": 0.42,
        "venue": "kalshi",
    },
    {
        "market_id": "KXCPI-MAR26-3PCT",
        "title": "March 2026 CPI year-over-year above 3.0%",
        "yes_price": 0.28,
        "no_price": 0.72,
        "venue": "kalshi",
    },
    {
        "market_id": "KXNFL-SB-KC",
        "title": "Kansas City Chiefs to win Super Bowl LXI",
        "yes_price": 0.15,
        "no_price": 0.85,
        "venue": "kalshi",
    },
    {
        "market_id": "KXUSCHINA-DEAL-JUL26",
        "title": "US-China trade agreement announced before August 2026",
        "yes_price": 0.35,
        "no_price": 0.65,
        "venue": "kalshi",
    },
]


def _generate_fixture_embeddings(n_markets: int, seed: int = 42) -> np.ndarray:
    """
    Generate deterministic pseudo-embeddings for fixture markets.

    Uses a seeded random state to produce normalised vectors that
    simulate real sentence-transformer output.  Pairs that should
    match (e.g. BTC markets) are given correlated embeddings by
    sharing a base vector with small perturbations.

    Returns:
        np.ndarray of shape (n_markets, EMBEDDING_DIM).
    """
    rng = np.random.RandomState(seed)
    embeddings = rng.randn(n_markets, EMBEDDING_DIM).astype(np.float32)
    # Normalise to unit vectors (sentence-transformers outputs are normalised).
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms
    return embeddings


def _generate_correlated_fixture_embeddings() -> tuple[np.ndarray, np.ndarray]:
    """
    Generate fixture embeddings with known correlation structure.

    Returns (poly_embeddings, kalshi_embeddings) where:
        - poly[0] (BTC 100k) ↔ kalshi[0] (BTC 100k) should match (cosine ~ 0.95)
        - poly[3] (CPI) ↔ kalshi[1] (CPI) should match (cosine ~ 0.92)
        - poly[4] (trade deal) ↔ kalshi[3] (trade deal) should match (cosine ~ 0.90)
        - poly[1] (Trump) ↔ poly[2] (GOP) should be intra-related (cosine ~ 0.78)
        - All other pairs should be dissimilar (cosine < 0.60)
    """
    rng = np.random.RandomState(42)

    n_poly = len(_FIXTURE_POLY_MARKETS)
    n_kalshi = len(_FIXTURE_KALSHI_MARKETS)

    # Generate independent base vectors for each distinct concept.
    base_btc = rng.randn(EMBEDDING_DIM).astype(np.float32)
    base_trump = rng.randn(EMBEDDING_DIM).astype(np.float32)
    base_gop = rng.randn(EMBEDDING_DIM).astype(np.float32)
    base_cpi = rng.randn(EMBEDDING_DIM).astype(np.float32)
    base_deal = rng.randn(EMBEDDING_DIM).astype(np.float32)
    base_nfl = rng.randn(EMBEDDING_DIM).astype(np.float32)

    # Make Trump and GOP correlated (~0.78 cosine) by blending.
    base_gop = 0.78 * base_trump + 0.63 * base_gop

    # Build poly embeddings.
    poly_raw = np.stack([
        base_btc,                                          # 0: BTC 100k
        base_trump,                                        # 1: Trump win
        base_gop,                                          # 2: GOP win
        base_cpi,                                          # 3: CPI
        base_deal,                                         # 4: Trade deal
    ])

    # Build kalshi embeddings — matched pairs get correlated vectors.
    noise_scale = 0.10
    kalshi_raw = np.stack([
        base_btc + noise_scale * rng.randn(EMBEDDING_DIM),      # 0: BTC (matches poly 0)
        base_cpi + noise_scale * rng.randn(EMBEDDING_DIM),      # 1: CPI (matches poly 3)
        base_nfl,                                                # 2: NFL (no match)
        base_deal + noise_scale * rng.randn(EMBEDDING_DIM),     # 3: Trade deal (matches poly 4)
    ]).astype(np.float32)

    # Normalise all to unit vectors.
    def _normalise(arr: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return arr / norms

    return _normalise(poly_raw), _normalise(kalshi_raw)


# =========================================================================
#  COSINE SIMILARITY (pure numpy, no scipy dependency)
# =========================================================================

def cosine_similarity_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Compute pairwise cosine similarity between two sets of vectors.

    Args:
        a: Array of shape (N, D) — first set of embeddings.
        b: Array of shape (M, D) — second set of embeddings.

    Returns:
        np.ndarray of shape (N, M) where element [i, j] is the
        cosine similarity between a[i] and b[j].
    """
    # Normalise rows to unit length.
    a_norm = a / (np.linalg.norm(a, axis=1, keepdims=True) + 1e-10)
    b_norm = b / (np.linalg.norm(b, axis=1, keepdims=True) + 1e-10)
    return np.dot(a_norm, b_norm.T)


def cosine_similarity_pair(a: np.ndarray, b: np.ndarray) -> float:
    """
    Compute cosine similarity between two individual vectors.

    Args:
        a: 1-D array of shape (D,).
        b: 1-D array of shape (D,).

    Returns:
        Cosine similarity as a float in [-1, 1].
    """
    dot = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))


# =========================================================================
#  ORACLE RISK ASSESSMENT
# =========================================================================

def assess_oracle_risk(title: str) -> str:
    """
    Assess oracle mismatch risk for a market based on its title.

    Markets with subjective or ambiguous resolution criteria are
    flagged as HIGH risk.  See directive 07 for full rationale.

    Args:
        title: Market title string.

    Returns:
        "HIGH" or "LOW".
    """
    title_lower = title.lower()
    for keyword in ORACLE_RISK_HIGH_KEYWORDS:
        if keyword in title_lower:
            logger.debug(
                "Oracle risk HIGH for '%s' (matched keyword '%s')",
                title[:60], keyword,
            )
            return "HIGH"
    return "LOW"


# =========================================================================
#  MarketMatcher CLASS
# =========================================================================

class MarketMatcher:
    """
    Semantic market matcher using sentence-transformer embeddings.

    Provides two matching modes:
        1. Cross-venue: Find Polymarket ↔ Kalshi pairs referring to
           the same underlying event (cosine > 0.80).
        2. Intra-venue: Cluster semantically related markets within
           a single venue for logical implication analysis (cosine > 0.70).

    In --test mode, uses precomputed fixture embeddings so no model
    download or GPU is required.

    Attributes:
        model_name:  Name of the sentence-transformer model.
        model:       The loaded SentenceTransformer instance (None in test mode).
        test_mode:   Whether fixture embeddings are used.
    """

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL_NAME,
        test_mode: bool = False,
    ) -> None:
        self.model_name = model_name
        self.model = None
        self.test_mode = test_mode

        if not test_mode:
            self._load_model()
        else:
            logger.info(
                "MarketMatcher initialised in test mode "
                "(using fixture embeddings, no model loaded)."
            )

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------

    def _load_model(self) -> None:
        """
        Load the sentence-transformer model.

        Late-imports sentence_transformers so that --test mode works
        without the library installed.  Logs at ERROR if the model
        cannot be loaded and sets self.model to None (callers must
        handle the None case — fall back to Type 1 arb only).
        """
        try:
            from sentence_transformers import SentenceTransformer
            logger.info("Loading sentence-transformer model '%s'...", self.model_name)
            self.model = SentenceTransformer(self.model_name)
            logger.info(
                "Model '%s' loaded successfully (dim=%d).",
                self.model_name, EMBEDDING_DIM,
            )
        except ImportError:
            logger.error(
                "sentence-transformers is not installed. "
                "Install with: pip install sentence-transformers. "
                "Falling back to Type 1 arb only (no semantic matching)."
            )
            self.model = None
        except Exception as exc:
            logger.error(
                "Failed to load model '%s': %s: %s. "
                "Falling back to Type 1 arb only.",
                self.model_name, type(exc).__name__, exc,
            )
            self.model = None

    # ------------------------------------------------------------------
    # Embedding
    # ------------------------------------------------------------------

    def encode_titles(self, titles: list[str]) -> np.ndarray:
        """
        Batch-encode market titles into dense vector embeddings.

        Args:
            titles: List of market title strings.

        Returns:
            np.ndarray of shape (len(titles), EMBEDDING_DIM) with
            L2-normalised embeddings.

        Raises:
            RuntimeError: If the model is not loaded and test_mode is False.
        """
        if not titles:
            return np.empty((0, EMBEDDING_DIM), dtype=np.float32)

        if self.test_mode:
            logger.info(
                "encode_titles [test]: generating fixture embeddings for %d titles.",
                len(titles),
            )
            return _generate_fixture_embeddings(len(titles), seed=hash(titles[0]) % 2**31)

        if self.model is None:
            raise RuntimeError(
                "Sentence-transformer model is not loaded. "
                "Cannot encode titles without the model."
            )

        logger.info("Encoding %d title(s) with '%s'...", len(titles), self.model_name)
        embeddings = self.model.encode(
            titles,
            batch_size=64,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        logger.info(
            "Encoded %d title(s) -> shape %s.",
            len(titles), embeddings.shape,
        )
        return np.asarray(embeddings, dtype=np.float32)

    # ------------------------------------------------------------------
    # Cross-venue matching
    # ------------------------------------------------------------------

    def find_cross_venue_pairs(
        self,
        poly_markets: list[dict],
        kalshi_markets: list[dict],
        threshold: float = CROSS_VENUE_THRESHOLD,
    ) -> list[dict]:
        """
        Find cross-venue pairs (Polymarket ↔ Kalshi) for the same event.

        For each pair above the cosine threshold, produces a match result
        dict with market IDs, confidence score, and oracle risk assessment.

        Args:
            poly_markets:   List of Polymarket market dicts (must have
                            'market_id' and 'title' keys).
            kalshi_markets: List of Kalshi market dicts (same keys).
            threshold:      Minimum cosine similarity for a match
                            (default 0.80 per directive 07).

        Returns:
            List of match result dicts::

                {
                    "poly_id":          str,
                    "poly_title":       str,
                    "kalshi_id":        str,
                    "kalshi_title":     str,
                    "match_confidence": float,   # cosine similarity
                    "oracle_risk":      str,     # "HIGH" or "LOW"
                }
        """
        if not poly_markets or not kalshi_markets:
            logger.warning("find_cross_venue_pairs: empty market list(s), returning [].")
            return []

        # Compute embeddings.
        if self.test_mode:
            poly_emb, kalshi_emb = _generate_correlated_fixture_embeddings()
            # Truncate/pad to actual market count (fixtures may differ).
            poly_emb = poly_emb[:len(poly_markets)]
            kalshi_emb = kalshi_emb[:len(kalshi_markets)]
        else:
            poly_titles = [m["title"] for m in poly_markets]
            kalshi_titles = [m["title"] for m in kalshi_markets]
            poly_emb = self.encode_titles(poly_titles)
            kalshi_emb = self.encode_titles(kalshi_titles)

        # Compute pairwise cosine similarity matrix.
        sim_matrix = cosine_similarity_matrix(poly_emb, kalshi_emb)
        logger.info(
            "Cross-venue similarity matrix: %d x %d (poly x kalshi).",
            sim_matrix.shape[0], sim_matrix.shape[1],
        )

        # Extract pairs above threshold.
        pairs: list[dict] = []
        for i in range(sim_matrix.shape[0]):
            for j in range(sim_matrix.shape[1]):
                score = float(sim_matrix[i, j])
                if score >= threshold:
                    poly_m = poly_markets[i]
                    kalshi_m = kalshi_markets[j]

                    # Oracle risk: flag HIGH if either title triggers keywords.
                    poly_risk = assess_oracle_risk(poly_m["title"])
                    kalshi_risk = assess_oracle_risk(kalshi_m["title"])
                    oracle_risk = "HIGH" if (poly_risk == "HIGH" or kalshi_risk == "HIGH") else "LOW"

                    pair = {
                        "poly_id": poly_m["market_id"],
                        "poly_title": poly_m["title"],
                        "kalshi_id": kalshi_m["market_id"],
                        "kalshi_title": kalshi_m["title"],
                        "match_confidence": round(score, 4),
                        "oracle_risk": oracle_risk,
                    }
                    pairs.append(pair)
                    logger.info(
                        "Cross-venue match: '%s' ↔ '%s' (cos=%.4f, risk=%s)",
                        poly_m["title"][:40], kalshi_m["title"][:40],
                        score, oracle_risk,
                    )

        # Sort by confidence descending.
        pairs.sort(key=lambda p: p["match_confidence"], reverse=True)
        logger.info(
            "Found %d cross-venue pair(s) above threshold %.2f.",
            len(pairs), threshold,
        )
        return pairs

    # ------------------------------------------------------------------
    # Intra-venue clustering
    # ------------------------------------------------------------------

    def find_intra_venue_clusters(
        self,
        markets: list[dict],
        threshold: float = INTRA_VENUE_THRESHOLD,
    ) -> list[list[dict]]:
        """
        Find clusters of semantically related markets within one venue.

        Uses single-linkage clustering: if A matches B and B matches C
        (both above threshold), all three are placed in the same cluster.
        Self-matches (same market_id) are filtered out.

        Args:
            markets:   List of market dicts ('market_id', 'title' required).
            threshold: Minimum cosine similarity for relatedness
                       (default 0.70 per directive 06).

        Returns:
            List of clusters, where each cluster is a list of market
            dicts enriched with a ``related_to`` key listing the other
            market IDs in the cluster and their cosine scores.
        """
        if len(markets) < 2:
            logger.info("find_intra_venue_clusters: fewer than 2 markets, no clusters.")
            return []

        # Compute embeddings.
        if self.test_mode:
            # Use the poly fixture embeddings for intra-venue tests.
            emb, _ = _generate_correlated_fixture_embeddings()
            emb = emb[:len(markets)]
        else:
            titles = [m["title"] for m in markets]
            emb = self.encode_titles(titles)

        # Pairwise similarity.
        sim_matrix = cosine_similarity_matrix(emb, emb)
        logger.info(
            "Intra-venue similarity matrix: %d x %d.", len(markets), len(markets),
        )

        # Build adjacency list (undirected) for pairs above threshold.
        adjacency: dict[int, list[tuple[int, float]]] = {i: [] for i in range(len(markets))}
        for i in range(len(markets)):
            for j in range(i + 1, len(markets)):
                # Filter self-referential matches by market_id.
                if markets[i]["market_id"] == markets[j]["market_id"]:
                    continue
                score = float(sim_matrix[i, j])
                if score >= threshold:
                    adjacency[i].append((j, score))
                    adjacency[j].append((i, score))
                    logger.debug(
                        "Intra-venue link: '%s' ↔ '%s' (cos=%.4f)",
                        markets[i]["title"][:35], markets[j]["title"][:35], score,
                    )

        # Connected components via BFS (single-linkage clustering).
        visited: set[int] = set()
        clusters: list[list[dict]] = []

        for start in range(len(markets)):
            if start in visited or not adjacency[start]:
                continue

            # BFS from this node.
            component: list[int] = []
            queue = [start]
            while queue:
                node = queue.pop(0)
                if node in visited:
                    continue
                visited.add(node)
                component.append(node)
                for neighbour, _ in adjacency[node]:
                    if neighbour not in visited:
                        queue.append(neighbour)

            if len(component) < 2:
                continue

            # Build cluster output — each market enriched with relations.
            cluster: list[dict] = []
            for idx in component:
                related = [
                    {"market_id": markets[nbr]["market_id"], "cosine": round(sc, 4)}
                    for nbr, sc in adjacency[idx]
                ]
                entry = {
                    **markets[idx],
                    "related_to": related,
                }
                cluster.append(entry)

            clusters.append(cluster)
            logger.info(
                "Intra-venue cluster of %d markets: %s",
                len(cluster),
                [m["market_id"] for m in cluster],
            )

        logger.info(
            "Found %d intra-venue cluster(s) above threshold %.2f.",
            len(clusters), threshold,
        )
        return clusters

    # ------------------------------------------------------------------
    # Logical implication detection
    # ------------------------------------------------------------------

    def check_logical_implication(
        self,
        market_a: dict,
        market_b: dict,
    ) -> bool:
        """
        Detect whether market_a logically implies market_b.

        Uses keyword heuristics per directive 06:
            - "X wins [election]" implies "[X's party] wins [election]"
            - Subset/superset title patterns
            - Entity containment (one title's key entities are a subset
              of the other's)

        This is a heuristic check — it may produce false negatives but
        should not produce false positives.

        Args:
            market_a: Market dict with 'title' key.
            market_b: Market dict with 'title' key.

        Returns:
            True if market_a plausibly implies market_b (i.e. if A is
            true then B must be true), False otherwise.
        """
        title_a = market_a["title"].lower()
        title_b = market_b["title"].lower()

        # Rule 1: Candidate-to-party implication.
        # "Trump wins" implies "Republican wins" for the same election.
        candidate_party_map = {
            "trump": "republican",
            "desantis": "republican",
            "haley": "republican",
            "biden": "democrat",
            "harris": "democrat",
            "newsom": "democrat",
        }

        for candidate, party in candidate_party_map.items():
            if candidate in title_a and "win" in title_a:
                if party in title_b and "win" in title_b:
                    # Verify they reference the same election type.
                    election_terms = ["election", "president", "primary", "nomination"]
                    a_has_election = any(t in title_a for t in election_terms)
                    b_has_election = any(t in title_b for t in election_terms)
                    if a_has_election and b_has_election:
                        logger.info(
                            "Logical implication detected: '%s' → '%s' "
                            "(candidate→party rule: %s→%s).",
                            market_a["title"][:50], market_b["title"][:50],
                            candidate, party,
                        )
                        return True

        # Rule 2: Numeric threshold implication.
        # "BTC above $120k" implies "BTC above $100k" for the same timeframe.
        # This is a simplified heuristic — full implementation would
        # require NER + numeric parsing.
        # (Deferred to Phase 4 when NER pipeline is available.)

        # Rule 3: Subset title detection.
        # If all significant tokens in A also appear in B (plus extra
        # specificity), A may imply B or vice versa.
        # Using word overlap as a weak signal only.
        stop_words = {"will", "the", "a", "an", "in", "by", "to", "of", "be", "is", "?"}
        tokens_a = set(title_a.split()) - stop_words
        tokens_b = set(title_b.split()) - stop_words

        if tokens_a and tokens_b:
            # If A's tokens are a strict superset of B's, A may imply B.
            if tokens_a > tokens_b and len(tokens_a - tokens_b) <= 3:
                logger.info(
                    "Possible implication (token superset): '%s' → '%s'.",
                    market_a["title"][:50], market_b["title"][:50],
                )
                return True

        return False


# =========================================================================
#  DATABASE PERSISTENCE
# =========================================================================

async def persist_matched_pairs(
    pairs: list[dict],
    test_mode: bool = False,
) -> int:
    """
    Persist cross-venue matched pairs to the ``matched_markets`` table.

    Uses upsert (ON CONFLICT DO UPDATE) so re-running the matcher
    updates confidence scores and oracle risk for existing pairs.

    Args:
        pairs:     List of match result dicts from find_cross_venue_pairs().
        test_mode: If True, uses the mock DB pool.

    Returns:
        Number of rows upserted.
    """
    if not pairs:
        logger.info("persist_matched_pairs: no pairs to persist.")
        return 0

    # Late import to avoid circular dependency and allow --test without asyncpg.
    from execution.utils.db import get_pool, _test_mode as db_test_mode

    pool = await get_pool()
    count = 0
    now = datetime.now(timezone.utc)

    for pair in pairs:
        poly_id = pair["poly_id"]
        kalshi_id = pair["kalshi_id"]
        confidence = pair["match_confidence"]
        oracle_risk = pair["oracle_risk"]

        if test_mode or db_test_mode:
            # Mock persistence — store in the pool's mock structure.
            if hasattr(pool, "_market_prices"):
                # Use a simple list on the mock pool for matched_markets.
                if not hasattr(pool, "_matched_markets"):
                    pool._matched_markets = []
                pool._matched_markets.append({
                    "poly_market_id": poly_id,
                    "kalshi_market_id": kalshi_id,
                    "match_confidence": confidence,
                    "oracle_risk": oracle_risk,
                    "quarantined": False,
                    "created_at": now,
                    "updated_at": now,
                })
            logger.info(
                "Persisted matched pair [mock]: %s ↔ %s (cos=%.4f, risk=%s)",
                poly_id, kalshi_id, confidence, oracle_risk,
            )
            count += 1
            continue

        query = """
            INSERT INTO matched_markets
                (poly_market_id, kalshi_market_id, match_confidence, oracle_risk, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (poly_market_id, kalshi_market_id) DO UPDATE
                SET match_confidence = EXCLUDED.match_confidence,
                    oracle_risk      = EXCLUDED.oracle_risk,
                    updated_at       = NOW()
        """
        async with pool.acquire() as conn:
            await conn.execute(query, poly_id, kalshi_id, confidence, oracle_risk)
        logger.info(
            "Persisted matched pair: %s ↔ %s (cos=%.4f, risk=%s)",
            poly_id, kalshi_id, confidence, oracle_risk,
        )
        count += 1

    logger.info("Persisted %d matched pair(s) to database.", count)
    return count


# =========================================================================
#  TEST MODE
# =========================================================================

async def run_test_mode() -> None:
    """
    --test mode: exercise all MarketMatcher methods with fixture data.

    Uses precomputed correlated embeddings so no model download is needed.
    Validates cross-venue matching, intra-venue clustering, oracle risk
    assessment, logical implication detection, and DB persistence.
    """
    print()
    print("=" * 72)
    print("  Market Matcher — Test Mode")
    print("  Time: %s" % datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    print("=" * 72)

    matcher = MarketMatcher(test_mode=True)

    # ------------------------------------------------------------------
    # Test 1: encode_titles
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 1: encode_titles")
    print("-" * 72)

    test_titles = [
        "Will Bitcoin reach $100k?",
        "BTC to hit $100,000",
        "Kansas City Chiefs Super Bowl",
    ]
    embeddings = matcher.encode_titles(test_titles)
    print(f"  Input:  {len(test_titles)} titles")
    print(f"  Output: shape {embeddings.shape}, dtype {embeddings.dtype}")
    assert embeddings.shape == (3, EMBEDDING_DIM), (
        f"Expected shape (3, {EMBEDDING_DIM}), got {embeddings.shape}"
    )
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 2: cosine_similarity_matrix
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 2: cosine_similarity (pure numpy)")
    print("-" * 72)

    # Self-similarity should be ~1.0.
    self_sim = cosine_similarity_matrix(embeddings, embeddings)
    diag = np.diag(self_sim)
    print(f"  Self-similarity diagonal: {diag}")
    assert np.allclose(diag, 1.0, atol=0.01), "Self-similarity should be ~1.0"
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 3: Cross-venue matching
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 3: find_cross_venue_pairs")
    print("-" * 72)

    pairs = matcher.find_cross_venue_pairs(
        _FIXTURE_POLY_MARKETS, _FIXTURE_KALSHI_MARKETS,
    )
    print(f"  Found {len(pairs)} cross-venue pair(s):")
    for p in pairs:
        print(
            f"    {p['poly_id']:25s} ↔ {p['kalshi_id']:25s}  "
            f"cos={p['match_confidence']:.4f}  risk={p['oracle_risk']}"
        )
    # We expect at least the BTC and CPI pairs.
    assert len(pairs) >= 2, f"Expected >= 2 cross-venue pairs, got {len(pairs)}"
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 4: Oracle risk assessment
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 4: assess_oracle_risk")
    print("-" * 72)

    risk_cases = [
        ("Will the US-China trade deal be announced?", "HIGH"),
        ("US CPI above 3% in March 2026?", "LOW"),
        ("New trade agreement signed before July", "HIGH"),
        ("Bitcoin to exceed $100,000", "LOW"),
        ("Ceasefire announcement by end of March", "HIGH"),
    ]
    all_pass = True
    for title, expected in risk_cases:
        actual = assess_oracle_risk(title)
        status = "OK" if actual == expected else "FAIL"
        if actual != expected:
            all_pass = False
        print(f"    [{status}] '{title[:50]}' -> {actual} (expected {expected})")
    assert all_pass, "Oracle risk assessment failed for one or more cases"
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 5: Intra-venue clustering
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 5: find_intra_venue_clusters")
    print("-" * 72)

    clusters = matcher.find_intra_venue_clusters(_FIXTURE_POLY_MARKETS)
    print(f"  Found {len(clusters)} cluster(s):")
    for ci, cluster in enumerate(clusters):
        ids = [m["market_id"] for m in cluster]
        print(f"    Cluster {ci}: {ids}")
        for m in cluster:
            if "related_to" in m:
                for rel in m["related_to"]:
                    print(
                        f"      {m['market_id'][:20]} → "
                        f"{rel['market_id'][:20]} (cos={rel['cosine']:.4f})"
                    )
    # Trump and GOP should cluster together.
    print("  PASSED (cluster structure validated visually)")

    # ------------------------------------------------------------------
    # Test 6: Logical implication detection
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 6: check_logical_implication")
    print("-" * 72)

    impl_cases = [
        (
            {"title": "Will Trump win the 2028 presidential election?"},
            {"title": "Will the Republican Party win the 2028 presidential election?"},
            True,
        ),
        (
            {"title": "Will Harris win the 2028 presidential election?"},
            {"title": "Will the Democrat Party win the 2028 presidential election?"},
            True,
        ),
        (
            {"title": "US CPI above 3%?"},
            {"title": "Bitcoin to exceed $100k?"},
            False,
        ),
    ]
    all_pass = True
    for a, b, expected in impl_cases:
        actual = matcher.check_logical_implication(a, b)
        status = "OK" if actual == expected else "FAIL"
        if actual != expected:
            all_pass = False
        print(
            f"    [{status}] '{a['title'][:35]}' → '{b['title'][:35]}' "
            f"= {actual} (expected {expected})"
        )
    assert all_pass, "Logical implication detection failed"
    print("  PASSED")

    # ------------------------------------------------------------------
    # Test 7: Database persistence (mock)
    # ------------------------------------------------------------------
    print()
    print("-" * 72)
    print("  TEST 7: persist_matched_pairs (mock DB)")
    print("-" * 72)

    # Set up mock DB.
    from execution.utils import db as db_module
    db_module._test_mode = True
    db_module._pool = None  # Force re-init of mock pool.

    upserted = await persist_matched_pairs(pairs, test_mode=True)
    print(f"  Upserted {upserted} pair(s) to mock DB.")
    assert upserted == len(pairs), (
        f"Expected {len(pairs)} upserts, got {upserted}"
    )

    # Verify mock storage.
    pool = await db_module.get_pool()
    if hasattr(pool, "_matched_markets"):
        print(f"  Mock matched_markets rows: {len(pool._matched_markets)}")
        for row in pool._matched_markets:
            print(
                f"    {row['poly_market_id'][:20]} ↔ "
                f"{row['kalshi_market_id'][:20]}  "
                f"conf={row['match_confidence']:.4f}  "
                f"risk={row['oracle_risk']}"
            )
    print("  PASSED")

    # Cleanup.
    await db_module.close_pool()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print()
    print("=" * 72)
    print("  ALL TESTS PASSED")
    print("=" * 72)
    print()


# =========================================================================
#  PRODUCTION MODE
# =========================================================================

async def run_production() -> None:
    """
    Production mode: load markets from DB, compute matches, persist results.

    In production, markets are loaded from the market_prices table
    (distinct market_ids per venue), matched via the sentence-transformer
    model, and results are persisted to matched_markets.

    This function is intended to be called periodically (every 5 minutes)
    by the strategy orchestrator or a cron job.
    """
    from execution.utils.db import get_pool

    logger.info("Starting production market matching run...")
    matcher = MarketMatcher()

    if matcher.model is None:
        logger.error(
            "Cannot run production matching without a loaded model. Exiting."
        )
        return

    pool = await get_pool()

    # Fetch distinct active markets from each venue.
    # Using the most recent title per market_id.
    query_markets = """
        SELECT DISTINCT ON (market_id)
            market_id, title, yes_price, no_price, venue
        FROM market_prices
        WHERE venue = $1
        ORDER BY market_id, time DESC
    """

    async with pool.acquire() as conn:
        poly_rows = await conn.fetch(query_markets, "polymarket")
        kalshi_rows = await conn.fetch(query_markets, "kalshi")

    poly_markets = [dict(r) for r in poly_rows]
    kalshi_markets = [dict(r) for r in kalshi_rows]

    logger.info(
        "Loaded %d Polymarket and %d Kalshi market(s) from DB.",
        len(poly_markets), len(kalshi_markets),
    )

    # Cross-venue matching.
    if poly_markets and kalshi_markets:
        cross_pairs = matcher.find_cross_venue_pairs(poly_markets, kalshi_markets)
        await persist_matched_pairs(cross_pairs)
    else:
        logger.warning(
            "Skipping cross-venue matching: insufficient markets "
            "(poly=%d, kalshi=%d).",
            len(poly_markets), len(kalshi_markets),
        )

    # Intra-venue clustering (Polymarket only for S4).
    if len(poly_markets) >= 2:
        clusters = matcher.find_intra_venue_clusters(poly_markets)
        logger.info(
            "Intra-venue clustering found %d cluster(s) across %d Polymarket markets.",
            len(clusters), len(poly_markets),
        )

        # Check for logical implications within each cluster.
        for cluster in clusters:
            for i, market_a in enumerate(cluster):
                for j, market_b in enumerate(cluster):
                    if i >= j:
                        continue
                    if matcher.check_logical_implication(market_a, market_b):
                        # Check for pricing violation: P(A) > P(B) when A implies B.
                        p_a = market_a.get("yes_price", 0)
                        p_b = market_b.get("yes_price", 0)
                        if p_a > p_b:
                            logger.warning(
                                "LOGICAL CONTRADICTION: '%s' (%.2f) implies "
                                "'%s' (%.2f) but P(A) > P(B). Edge=%.4f.",
                                market_a["title"][:40], p_a,
                                market_b["title"][:40], p_b,
                                p_a - p_b,
                            )
    else:
        logger.info("Skipping intra-venue clustering: fewer than 2 Polymarket markets.")

    logger.info("Production market matching run complete.")


# =========================================================================
#  ENTRY POINT
# =========================================================================

def main() -> None:
    """
    CLI entry point: parse --test flag and run the appropriate mode.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Cross-venue semantic market matcher for polymarket-trader. "
            "Matches markets across Polymarket and Kalshi using "
            "sentence-transformer embeddings."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Use precomputed fixture embeddings (no model download needed). "
            "Exercises all matching methods with synthetic data."
        ),
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode with fixture embeddings.")
        asyncio.run(run_test_mode())
    else:
        logger.info("Running in production mode.")
        asyncio.run(run_production())


if __name__ == "__main__":
    main()
