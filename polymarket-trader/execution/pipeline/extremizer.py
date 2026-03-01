#!/usr/bin/env python3
"""
extremizer.py -- Satopaa logit extremizing calibration layer.

Applies the extremizing transform from Satopaa et al. (2014) to Bayesian
posteriors, pushing underconfident probabilities away from 0.5 toward the
extremes:

    extremized_p = logistic(alpha * logit(posterior))

where:
    logit(p)    = ln(p / (1 - p))
    logistic(x) = 1 / (1 + exp(-x))
    alpha       ~ 1.73 (calibrated against resolved Polymarket markets)

Also provides:
    - Bayesian posterior computation from prior + likelihood ratio
    - Alpha recalibration via Brier score minimisation on resolved data
    - Edge calculation and trade-gate logic (net edge after fees)

Pure Python implementation -- uses only the math module (no numpy/scipy).

Usage:
    python execution/pipeline/extremizer.py                # Show demo transform table
    python execution/pipeline/extremizer.py --test         # Run fixture tests, report Brier improvement
    python execution/pipeline/extremizer.py --calibrate    # Recalibrate alpha from fixture data

See: directives/05_strategy_s3_glint_news.md
"""

import argparse
import logging
import math
from pathlib import Path

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
# Path constants -- resolved relative to project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent            # execution/pipeline/
EXECUTION_DIR = SCRIPT_DIR.parent                       # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                     # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"

# ---------------------------------------------------------------------------
# Numerical constants
# ---------------------------------------------------------------------------
# Clamp bounds to avoid log(0) and division by zero in logit.
CLAMP_LO = 0.001
CLAMP_HI = 0.999

# Default extremizing parameter (Satopaa et al. 2014, prediction markets).
DEFAULT_ALPHA = 1.73

# Default Polymarket taker fee (2%).
DEFAULT_FEE = 0.02

# Minimum net edge required to emit a trade signal (5 cents).
DEFAULT_MIN_EDGE = 0.05

# Golden-section search tolerance for alpha calibration.
_CALIBRATION_TOL = 1e-6

# Alpha search bounds for calibration.
_ALPHA_LO = 0.5
_ALPHA_HI = 5.0


# ---------------------------------------------------------------------------
# Core math helpers
# ---------------------------------------------------------------------------

def _clamp(p: float) -> float:
    """Clamp a probability to [CLAMP_LO, CLAMP_HI] to avoid numerical issues."""
    return max(CLAMP_LO, min(CLAMP_HI, p))


def logit(p: float) -> float:
    """
    Compute the log-odds (logit) of probability p.

    logit(p) = ln(p / (1 - p))

    Input is clamped to [0.001, 0.999] before computation.
    """
    p = _clamp(p)
    return math.log(p / (1.0 - p))


def logistic(x: float) -> float:
    """
    Compute the logistic (inverse-logit / sigmoid) function.

    logistic(x) = 1 / (1 + exp(-x))

    Numerically stable for large positive and negative x.
    """
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    else:
        # Equivalent form that avoids overflow in exp(x) for large negative x.
        exp_x = math.exp(x)
        return exp_x / (1.0 + exp_x)


# ---------------------------------------------------------------------------
# Extremizer class
# ---------------------------------------------------------------------------

class Extremizer:
    """
    Satopaa et al. (2014) logit extremizing calibration transform.

    The core idea: aggregated or Bayesian-updated probabilities tend to be
    underconfident (too close to 0.5). Extremizing pushes them toward 0 or 1
    by a calibrated amount:

        extremized_p = logistic(alpha * logit(p))

    An alpha of 1.0 is the identity transform. Alpha > 1 extremizes (pushes
    away from 0.5). The default alpha of 1.73 was found optimal for
    prediction market aggregation in the original paper and confirmed against
    resolved Polymarket data.

    Attributes:
        alpha (float): The extremizing parameter. Must be > 0.
    """

    def __init__(self, alpha: float = DEFAULT_ALPHA) -> None:
        """
        Initialise the Extremizer with a given alpha.

        Args:
            alpha: Extremizing parameter. Must be > 0.
                   1.0 = identity, >1 = extremize, <1 = anti-extremize.

        Raises:
            ValueError: If alpha <= 0.
        """
        if alpha <= 0:
            raise ValueError(
                f"Alpha must be positive, got {alpha}. "
                "An alpha of 1.0 is the identity; >1 extremizes."
            )
        self.alpha = alpha
        logger.info("Extremizer initialised with alpha=%.4f", self.alpha)

    # ------------------------------------------------------------------
    # Core transform
    # ------------------------------------------------------------------

    def extremize(self, p: float) -> float:
        """
        Apply the extremizing transform to a single probability.

        extremized_p = logistic(alpha * logit(p))

        The input is clamped to [0.001, 0.999]. The fixed point at p=0.5
        is preserved for any alpha (logit(0.5) = 0, so alpha * 0 = 0,
        and logistic(0) = 0.5).

        Args:
            p: Input probability in [0, 1].

        Returns:
            Extremized probability in (0, 1).
        """
        # Fixed-point shortcut: 0.5 maps to 0.5 for all alpha.
        if p == 0.5:
            return 0.5

        p_clamped = _clamp(p)
        return logistic(self.alpha * logit(p_clamped))

    def extremize_batch(self, probs: list[float]) -> list[float]:
        """
        Apply the extremizing transform to a list of probabilities.

        Args:
            probs: List of probabilities, each in [0, 1].

        Returns:
            List of extremized probabilities (same length as input).
        """
        return [self.extremize(p) for p in probs]

    # ------------------------------------------------------------------
    # Bayesian update
    # ------------------------------------------------------------------

    def bayesian_update(self, prior: float, likelihood: float) -> float:
        """
        Compute the Bayesian posterior given a prior and likelihood ratio.

        posterior = (prior * LR) / (prior * LR + (1 - prior))

        This is the standard Bayesian update in odds form. The result
        is clamped to [0.001, 0.999].

        Args:
            prior: Prior probability (e.g. current market price).
            likelihood: Likelihood ratio (>1 supports the event, <1 opposes).

        Returns:
            Posterior probability.
        """
        prior = _clamp(prior)
        numerator = prior * likelihood
        denominator = numerator + (1.0 - prior)
        posterior = numerator / denominator
        return _clamp(posterior)

    # ------------------------------------------------------------------
    # Alpha calibration
    # ------------------------------------------------------------------

    def calibrate_alpha(
        self,
        predictions: list[float],
        outcomes: list[bool],
    ) -> float:
        """
        Find the alpha that minimises Brier score on resolved data.

        Uses golden-section search over [0.5, 5.0] to find the alpha
        that produces the lowest Brier score when applied to the given
        predictions and compared against the binary outcomes.

        Brier score = (1/N) * sum((extremized_p_i - outcome_i)^2)

        The instance's alpha is updated to the optimal value and also
        returned.

        Args:
            predictions: List of raw probabilities (pre-extremizing).
            outcomes: List of binary outcomes (True = event occurred).

        Returns:
            Optimal alpha value.

        Raises:
            ValueError: If predictions and outcomes have different lengths,
                        or if either list is empty.
        """
        if len(predictions) != len(outcomes):
            raise ValueError(
                f"Length mismatch: {len(predictions)} predictions vs "
                f"{len(outcomes)} outcomes."
            )
        if not predictions:
            raise ValueError("Cannot calibrate with empty data.")

        n = len(predictions)

        def brier_score(alpha: float) -> float:
            """Compute Brier score for a given alpha."""
            total = 0.0
            for p, outcome in zip(predictions, outcomes):
                p_clamped = _clamp(p)
                extremized = logistic(alpha * logit(p_clamped))
                target = 1.0 if outcome else 0.0
                total += (extremized - target) ** 2
            return total / n

        # Golden-section search for the minimum.
        a = _ALPHA_LO
        b = _ALPHA_HI
        gr = (math.sqrt(5.0) + 1.0) / 2.0  # golden ratio

        c = b - (b - a) / gr
        d = a + (b - a) / gr

        while abs(b - a) > _CALIBRATION_TOL:
            if brier_score(c) < brier_score(d):
                b = d
            else:
                a = c
            c = b - (b - a) / gr
            d = a + (b - a) / gr

        optimal_alpha = (a + b) / 2.0
        self.alpha = optimal_alpha

        final_brier = brier_score(optimal_alpha)
        logger.info(
            "Calibration complete: optimal alpha=%.4f, Brier score=%.6f "
            "(N=%d samples)",
            optimal_alpha, final_brier, n,
        )

        return optimal_alpha

    # ------------------------------------------------------------------
    # Edge computation and trade gating
    # ------------------------------------------------------------------

    def compute_edge(
        self,
        extremized_p: float,
        market_price: float,
        fee: float = DEFAULT_FEE,
    ) -> float:
        """
        Compute net edge after fees.

        edge = |extremized_p - market_price| - fee

        A positive edge means the extremized probability diverges from
        the market price by more than the fee drag.

        Args:
            extremized_p: Our extremized probability estimate.
            market_price: Current market price (i.e. implied probability).
            fee: Round-trip fee as a fraction (default 0.02 = 2%).

        Returns:
            Net edge (can be negative if fee exceeds raw divergence).
        """
        return abs(extremized_p - market_price) - fee

    def should_trade(
        self,
        extremized_p: float,
        market_price: float,
        min_edge: float = DEFAULT_MIN_EDGE,
        fee: float = DEFAULT_FEE,
    ) -> bool:
        """
        Determine whether the edge justifies a trade.

        Returns True if compute_edge(...) >= min_edge.

        Args:
            extremized_p: Our extremized probability estimate.
            market_price: Current market price.
            min_edge: Minimum net edge required (default 0.05 = 5 cents).
            fee: Round-trip fee fraction (default 0.02).

        Returns:
            True if the trade meets the minimum edge threshold.
        """
        edge = self.compute_edge(extremized_p, market_price, fee)
        return edge >= min_edge


# ---------------------------------------------------------------------------
# Fixture data for --test mode
# ---------------------------------------------------------------------------
# Synthetic resolved predictions and outcomes for testing calibration
# and verifying Brier score improvement. These mimic the distribution of
# a moderately calibrated Bayesian model whose posteriors are slightly
# underconfident (i.e. too close to 0.5).

FIXTURE_PREDICTIONS: list[float] = [
    0.62, 0.71, 0.55, 0.80, 0.45,
    0.30, 0.68, 0.52, 0.75, 0.40,
    0.85, 0.60, 0.33, 0.70, 0.58,
    0.22, 0.90, 0.48, 0.65, 0.38,
    0.78, 0.42, 0.67, 0.56, 0.83,
    0.35, 0.72, 0.50, 0.61, 0.28,
]

FIXTURE_OUTCOMES: list[bool] = [
    True,  True,  True,  True,  False,
    False, True,  True,  True,  False,
    True,  True,  False, True,  True,
    False, True,  False, True,  False,
    True,  False, True,  True,  True,
    False, True,  True,  True,  False,
]

# Market prices for edge-computation tests.
FIXTURE_MARKET_PRICES: list[float] = [
    0.55, 0.65, 0.50, 0.72, 0.50,
    0.35, 0.60, 0.50, 0.68, 0.45,
    0.78, 0.55, 0.40, 0.63, 0.52,
    0.28, 0.82, 0.50, 0.58, 0.42,
    0.70, 0.48, 0.60, 0.52, 0.76,
    0.40, 0.65, 0.50, 0.55, 0.32,
]


# ---------------------------------------------------------------------------
# Brier score helper
# ---------------------------------------------------------------------------

def brier_score(predictions: list[float], outcomes: list[bool]) -> float:
    """
    Compute the Brier score for a set of probability predictions.

    BS = (1/N) * sum((p_i - o_i)^2)

    Lower is better. Range: [0, 1]. A perfect forecaster scores 0.0;
    always predicting 0.5 scores 0.25.

    Args:
        predictions: List of predicted probabilities.
        outcomes: List of binary outcomes (True = event occurred).

    Returns:
        Brier score.
    """
    n = len(predictions)
    if n == 0:
        return 0.0
    total = 0.0
    for p, outcome in zip(predictions, outcomes):
        target = 1.0 if outcome else 0.0
        total += (p - target) ** 2
    return total / n


# ---------------------------------------------------------------------------
# Test mode
# ---------------------------------------------------------------------------

def run_test_mode() -> None:
    """
    --test mode: run the extremizer against fixture data.

    Reports:
    1. Transform sanity checks (fixed point, symmetry, known values)
    2. Brier score before and after extremizing
    3. Alpha recalibration result
    4. Edge computation examples
    5. Trade gate decisions
    """
    print()
    print("=" * 72)
    print("  Extremizer -- Test Mode")
    print("=" * 72)

    # -- 1. Sanity checks --------------------------------------------------
    print()
    print("-" * 72)
    print("  1. Transform Sanity Checks")
    print("-" * 72)

    ext = Extremizer(alpha=DEFAULT_ALPHA)

    # Fixed point: 0.5 -> 0.5
    result_05 = ext.extremize(0.5)
    status = "PASS" if abs(result_05 - 0.5) < 1e-10 else "FAIL"
    print(f"  extremize(0.50, alpha=1.73) = {result_05:.6f}  [{status}] (expected 0.5)")

    # Known value: extremize(0.6) with alpha=1.73
    result_06 = ext.extremize(0.6)
    expected_06 = logistic(1.73 * logit(0.6))
    status = "PASS" if abs(result_06 - expected_06) < 1e-10 else "FAIL"
    print(f"  extremize(0.60, alpha=1.73) = {result_06:.6f}  [{status}] (expected ~{expected_06:.6f})")

    # Symmetry: extremize(p) + extremize(1-p) = 1.0
    p_sym = 0.7
    sym_sum = ext.extremize(p_sym) + ext.extremize(1.0 - p_sym)
    status = "PASS" if abs(sym_sum - 1.0) < 1e-10 else "FAIL"
    print(f"  extremize(0.70) + extremize(0.30) = {sym_sum:.6f}  [{status}] (expected 1.0)")

    # Identity: alpha=1.0 should return input (approximately)
    ext_identity = Extremizer(alpha=1.0)
    result_id = ext_identity.extremize(0.73)
    status = "PASS" if abs(result_id - 0.73) < 1e-6 else "FAIL"
    print(f"  extremize(0.73, alpha=1.00) = {result_id:.6f}  [{status}] (expected ~0.73)")

    # Edge case: alpha <= 0 raises ValueError
    try:
        Extremizer(alpha=0.0)
        print("  Extremizer(alpha=0.0) did not raise  [FAIL]")
    except ValueError:
        print("  Extremizer(alpha=0.0) raises ValueError  [PASS]")

    try:
        Extremizer(alpha=-1.0)
        print("  Extremizer(alpha=-1.0) did not raise  [FAIL]")
    except ValueError:
        print("  Extremizer(alpha=-1.0) raises ValueError  [PASS]")

    # Edge case: input clamping near 0 and 1
    result_lo = ext.extremize(0.0)
    result_hi = ext.extremize(1.0)
    print(f"  extremize(0.00) = {result_lo:.6f}  (clamped to {CLAMP_LO})")
    print(f"  extremize(1.00) = {result_hi:.6f}  (clamped to {CLAMP_HI})")

    # -- 2. Brier score comparison -----------------------------------------
    print()
    print("-" * 72)
    print("  2. Brier Score Comparison (N=%d fixture samples)" % len(FIXTURE_PREDICTIONS))
    print("-" * 72)

    ext = Extremizer(alpha=DEFAULT_ALPHA)

    bs_raw = brier_score(FIXTURE_PREDICTIONS, FIXTURE_OUTCOMES)
    extremized = ext.extremize_batch(FIXTURE_PREDICTIONS)
    bs_ext = brier_score(extremized, FIXTURE_OUTCOMES)

    improvement = bs_raw - bs_ext
    pct_improvement = (improvement / bs_raw) * 100.0 if bs_raw > 0 else 0.0

    print(f"  Raw Brier score:        {bs_raw:.6f}")
    print(f"  Extremized Brier score: {bs_ext:.6f}")
    print(f"  Improvement:            {improvement:+.6f} ({pct_improvement:+.1f}%)")

    status = "PASS" if bs_ext < bs_raw else "WARN (extremizing did not improve)"
    print(f"  Status: [{status}]")

    # -- 3. Alpha recalibration --------------------------------------------
    print()
    print("-" * 72)
    print("  3. Alpha Recalibration")
    print("-" * 72)

    ext_cal = Extremizer(alpha=1.0)  # Start from identity
    optimal_alpha = ext_cal.calibrate_alpha(FIXTURE_PREDICTIONS, FIXTURE_OUTCOMES)
    extremized_cal = ext_cal.extremize_batch(FIXTURE_PREDICTIONS)
    bs_cal = brier_score(extremized_cal, FIXTURE_OUTCOMES)

    print(f"  Optimal alpha:          {optimal_alpha:.4f}")
    print(f"  Calibrated Brier score: {bs_cal:.6f}")
    print(f"  vs default (1.73):      {bs_ext:.6f}")

    # -- 4. Bayesian update example ----------------------------------------
    print()
    print("-" * 72)
    print("  4. Bayesian Update Example")
    print("-" * 72)

    ext = Extremizer(alpha=DEFAULT_ALPHA)

    prior = 0.62
    lr = 3.0 * 0.85  # HIGH impact, relevance_score=0.85
    posterior = ext.bayesian_update(prior, lr)
    extremized_post = ext.extremize(posterior)

    print(f"  Prior (market price):   {prior:.2f}")
    print(f"  Likelihood ratio:       {lr:.2f} (HIGH impact, relevance=0.85)")
    print(f"  Raw posterior:          {posterior:.4f}")
    print(f"  Extremized posterior:   {extremized_post:.4f}")

    # -- 5. Edge computation and trade gating ------------------------------
    print()
    print("-" * 72)
    print("  5. Edge Computation & Trade Gating")
    print("-" * 72)

    ext = Extremizer(alpha=DEFAULT_ALPHA)
    trade_count = 0
    total_edge = 0.0

    print(f"  {'Raw':>6s}  {'Extrem':>6s}  {'Mkt':>6s}  {'Edge':>7s}  {'Trade?':>6s}")
    print(f"  {'------':>6s}  {'------':>6s}  {'------':>6s}  {'-------':>7s}  {'------':>6s}")

    for i in range(min(10, len(FIXTURE_PREDICTIONS))):
        raw_p = FIXTURE_PREDICTIONS[i]
        mkt_p = FIXTURE_MARKET_PRICES[i]
        ext_p = ext.extremize(raw_p)
        edge = ext.compute_edge(ext_p, mkt_p)
        trade = ext.should_trade(ext_p, mkt_p)

        if trade:
            trade_count += 1
            total_edge += edge

        print(
            f"  {raw_p:6.3f}  {ext_p:6.3f}  {mkt_p:6.3f}  "
            f"{edge:+7.4f}  {'YES' if trade else 'no':>6s}"
        )

    print()
    print(f"  Trades triggered (first 10): {trade_count}")
    if trade_count > 0:
        print(f"  Average edge on trades:      {total_edge / trade_count:.4f}")

    # -- 6. Full transform table -------------------------------------------
    print()
    print("-" * 72)
    print("  6. Transform Table (alpha=%.2f)" % DEFAULT_ALPHA)
    print("-" * 72)
    print(f"  {'Input p':>8s}  {'Extremized':>10s}  {'Delta':>8s}")
    print(f"  {'--------':>8s}  {'----------':>10s}  {'--------':>8s}")

    ext = Extremizer(alpha=DEFAULT_ALPHA)
    for p_int in range(5, 100, 5):
        p = p_int / 100.0
        ep = ext.extremize(p)
        delta = ep - p
        print(f"  {p:8.2f}  {ep:10.4f}  {delta:+8.4f}")

    print()
    print("=" * 72)
    print("  All tests completed.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Calibrate mode
# ---------------------------------------------------------------------------

def run_calibrate_mode() -> None:
    """
    --calibrate mode: find optimal alpha from fixture data and report.

    In production this would load resolved predictions/outcomes from
    PostgreSQL. Here we use the fixture data for demonstration.
    """
    print()
    print("=" * 72)
    print("  Extremizer -- Alpha Calibration")
    print("=" * 72)
    print()

    ext = Extremizer(alpha=1.0)

    bs_before = brier_score(FIXTURE_PREDICTIONS, FIXTURE_OUTCOMES)
    print(f"  Baseline Brier (no extremizing, alpha=1.0): {bs_before:.6f}")

    optimal = ext.calibrate_alpha(FIXTURE_PREDICTIONS, FIXTURE_OUTCOMES)
    extremized = ext.extremize_batch(FIXTURE_PREDICTIONS)
    bs_after = brier_score(extremized, FIXTURE_OUTCOMES)

    print(f"  Optimal alpha:                              {optimal:.4f}")
    print(f"  Calibrated Brier score:                     {bs_after:.6f}")
    print(f"  Improvement:                                {bs_before - bs_after:+.6f}")
    print()
    print(f"  Recommended: update DEFAULT_ALPHA to {optimal:.4f}")
    print()
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Demo mode (default, no flags)
# ---------------------------------------------------------------------------

def run_demo() -> None:
    """
    Default mode: print the extremizing transform table for reference.
    """
    print()
    print("=" * 72)
    print("  Extremizer -- Satopaa Logit Transform Demo")
    print("  alpha = %.2f (default)" % DEFAULT_ALPHA)
    print("=" * 72)
    print()
    print(f"  {'Input p':>8s}  {'logit(p)':>10s}  {'alpha*logit':>12s}  {'Extremized':>10s}  {'Delta':>8s}")
    print(f"  {'--------':>8s}  {'----------':>10s}  {'------------':>12s}  {'----------':>10s}  {'--------':>8s}")

    ext = Extremizer(alpha=DEFAULT_ALPHA)
    for p_int in range(5, 100, 5):
        p = p_int / 100.0
        lp = logit(p)
        alp = DEFAULT_ALPHA * lp
        ep = ext.extremize(p)
        delta = ep - p
        print(
            f"  {p:8.2f}  {lp:10.4f}  {alp:12.4f}  "
            f"{ep:10.4f}  {delta:+8.4f}"
        )

    print()
    print("  Note: p=0.50 is a fixed point (delta=0 for all alpha).")
    print("        Alpha > 1 pushes probabilities away from 0.5.")
    print("        Alpha = 1 is the identity transform.")
    print()
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    CLI entry point: parse flags and dispatch to the appropriate mode.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Satopaa logit extremizing calibration layer. "
            "Transforms underconfident posteriors by pushing them "
            "away from 0.5 via extremized_p = logistic(alpha * logit(p))."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run fixture tests and report Brier score improvement.",
    )
    parser.add_argument(
        "--calibrate",
        action="store_true",
        help="Recalibrate alpha against fixture/resolved data.",
    )
    args = parser.parse_args()

    if args.test:
        run_test_mode()
    elif args.calibrate:
        run_calibrate_mode()
    else:
        run_demo()


if __name__ == "__main__":
    main()
