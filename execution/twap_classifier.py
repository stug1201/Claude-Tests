#!/usr/bin/env python3
"""
TWAP Classifier

Categorizes detected TWAP patterns by size, urgency, and other characteristics.

Usage:
    from twap_classifier import TWAPClassifier, classify_detections

    classifier = TWAPClassifier()
    classified = classifier.classify(detections)
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from twap_fourier_analyzer import TWAPDetection


class SizeCategory(Enum):
    """Classification by estimated total USD value."""
    SMALL = "Small"      # < $50K
    MEDIUM = "Medium"    # $50K - $500K
    LARGE = "Large"      # $500K - $5M
    WHALE = "Whale"      # > $5M


class UrgencyCategory(Enum):
    """Classification by execution interval."""
    AGGRESSIVE = "Aggressive"  # < 10s interval
    NORMAL = "Normal"          # 10-60s interval
    PASSIVE = "Passive"        # > 60s interval


class ConfidenceLevel(Enum):
    """Classification by detection confidence."""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


@dataclass
class ClassifiedTWAP:
    """A TWAP detection with additional classification metadata."""
    detection: TWAPDetection
    size_category: SizeCategory
    urgency_category: UrgencyCategory
    confidence_level: ConfidenceLevel
    risk_score: float  # 0-100, higher = more market impact risk
    description: str

    def __str__(self) -> str:
        return (
            f"{self.detection.side.upper()} TWAP [{self.size_category.value}] "
            f"({self.urgency_category.value})\n"
            f"  Interval: {self.detection.interval_seconds:.1f}s\n"
            f"  Per-exec: {self.detection.estimated_per_execution_size:.6f} "
            f"(${self.detection.estimated_per_execution_value:,.0f})\n"
            f"  Est. Total: {self.detection.estimated_total_size:.4f} "
            f"(${self.detection.estimated_total_value:,.0f})\n"
            f"  Confidence: {self.confidence_level.value} (SNR: {self.detection.snr:.1f})\n"
            f"  Risk Score: {self.risk_score:.0f}/100"
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "side": self.detection.side,
            "interval_seconds": self.detection.interval_seconds,
            "per_execution_size": self.detection.estimated_per_execution_size,
            "per_execution_value_usd": self.detection.estimated_per_execution_value,
            "total_size": self.detection.estimated_total_size,
            "total_value_usd": self.detection.estimated_total_value,
            "snr": self.detection.snr,
            "size_category": self.size_category.value,
            "urgency_category": self.urgency_category.value,
            "confidence": self.confidence_level.value,
            "risk_score": self.risk_score,
            "description": self.description,
        }


class TWAPClassifier:
    """
    Classifies TWAP detections into categories.
    """

    # Size thresholds (USD)
    SIZE_THRESHOLDS = {
        SizeCategory.SMALL: 50_000,
        SizeCategory.MEDIUM: 500_000,
        SizeCategory.LARGE: 5_000_000,
        # Anything above LARGE threshold is WHALE
    }

    # Urgency thresholds (seconds)
    URGENCY_THRESHOLDS = {
        UrgencyCategory.AGGRESSIVE: 10,
        UrgencyCategory.NORMAL: 60,
        # Anything above NORMAL threshold is PASSIVE
    }

    def __init__(
        self,
        size_thresholds: Optional[dict] = None,
        urgency_thresholds: Optional[dict] = None,
    ):
        """
        Initialize classifier with optional custom thresholds.

        Args:
            size_thresholds: Custom USD thresholds for size categories
            urgency_thresholds: Custom interval thresholds for urgency
        """
        self.size_thresholds = size_thresholds or self.SIZE_THRESHOLDS
        self.urgency_thresholds = urgency_thresholds or self.URGENCY_THRESHOLDS

    def _classify_size(self, value_usd: float) -> SizeCategory:
        """Classify by total USD value."""
        if value_usd < self.size_thresholds[SizeCategory.SMALL]:
            return SizeCategory.SMALL
        elif value_usd < self.size_thresholds[SizeCategory.MEDIUM]:
            return SizeCategory.MEDIUM
        elif value_usd < self.size_thresholds[SizeCategory.LARGE]:
            return SizeCategory.LARGE
        else:
            return SizeCategory.WHALE

    def _classify_urgency(self, interval_sec: float) -> UrgencyCategory:
        """Classify by execution interval."""
        if interval_sec < self.urgency_thresholds[UrgencyCategory.AGGRESSIVE]:
            return UrgencyCategory.AGGRESSIVE
        elif interval_sec < self.urgency_thresholds[UrgencyCategory.NORMAL]:
            return UrgencyCategory.NORMAL
        else:
            return UrgencyCategory.PASSIVE

    def _classify_confidence(self, detection: TWAPDetection) -> ConfidenceLevel:
        """Classify confidence level."""
        conf_str = detection.confidence.upper()
        if conf_str == "HIGH":
            return ConfidenceLevel.HIGH
        elif conf_str == "MEDIUM":
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW

    def _calculate_risk_score(
        self,
        detection: TWAPDetection,
        size_cat: SizeCategory,
        urgency_cat: UrgencyCategory,
    ) -> float:
        """
        Calculate market impact risk score (0-100).

        Higher score = more potential market impact.
        Factors:
        - Size (larger = higher risk)
        - Urgency (more aggressive = higher risk)
        - Confidence (higher confidence = more certain risk)
        """
        # Base score from size
        size_scores = {
            SizeCategory.SMALL: 10,
            SizeCategory.MEDIUM: 30,
            SizeCategory.LARGE: 60,
            SizeCategory.WHALE: 90,
        }
        base_score = size_scores[size_cat]

        # Urgency modifier
        urgency_modifiers = {
            UrgencyCategory.AGGRESSIVE: 1.3,
            UrgencyCategory.NORMAL: 1.0,
            UrgencyCategory.PASSIVE: 0.7,
        }
        score = base_score * urgency_modifiers[urgency_cat]

        # Confidence modifier (uncertain detections = lower risk certainty)
        confidence_modifiers = {
            "HIGH": 1.0,
            "MEDIUM": 0.85,
            "LOW": 0.6,
        }
        score *= confidence_modifiers.get(detection.confidence.upper(), 0.7)

        return min(100, max(0, score))

    def _generate_description(
        self,
        detection: TWAPDetection,
        size_cat: SizeCategory,
        urgency_cat: UrgencyCategory,
        confidence: ConfidenceLevel,
        risk_score: float,
    ) -> str:
        """Generate human-readable description of the TWAP with full context."""
        side_verb = "buying" if detection.side == "buy" else "selling"
        side_noun = "accumulation" if detection.side == "buy" else "distribution"

        urgency_desc = {
            UrgencyCategory.AGGRESSIVE: "aggressively",
            UrgencyCategory.NORMAL: "steadily",
            UrgencyCategory.PASSIVE: "patiently",
        }

        size_desc = {
            SizeCategory.SMALL: "A small player",
            SizeCategory.MEDIUM: "An institutional trader",
            SizeCategory.LARGE: "A major fund",
            SizeCategory.WHALE: "A whale",
        }

        # Confidence context
        confidence_context = {
            ConfidenceLevel.HIGH: f"Signal strength is strong (SNR: {detection.snr:.1f}), making this a reliable detection.",
            ConfidenceLevel.MEDIUM: f"Signal is moderate (SNR: {detection.snr:.1f}); pattern is likely real but monitor for confirmation.",
            ConfidenceLevel.LOW: f"Signal is weak (SNR: {detection.snr:.1f}); treat as tentative - could be noise or early-stage TWAP.",
        }

        # Risk context
        if risk_score >= 60:
            risk_context = f"Risk score {risk_score:.0f}/100 suggests significant potential market impact."
        elif risk_score >= 30:
            risk_context = f"Risk score {risk_score:.0f}/100 indicates moderate market influence."
        else:
            risk_context = f"Risk score {risk_score:.0f}/100 implies limited market impact."

        return (
            f"{size_desc[size_cat]} is {urgency_desc[urgency_cat]} {side_verb}, "
            f"executing ~${detection.estimated_per_execution_value:,.0f} every "
            f"{detection.interval_seconds:.0f} seconds. "
            f"{confidence_context[confidence]} {risk_context}"
        )

    def classify(self, detection: TWAPDetection) -> ClassifiedTWAP:
        """
        Classify a single TWAP detection.

        Args:
            detection: TWAPDetection to classify

        Returns:
            ClassifiedTWAP with categories and metadata
        """
        size_cat = self._classify_size(detection.estimated_total_value)
        urgency_cat = self._classify_urgency(detection.interval_seconds)
        confidence = self._classify_confidence(detection)
        risk_score = self._calculate_risk_score(detection, size_cat, urgency_cat)
        description = self._generate_description(
            detection, size_cat, urgency_cat, confidence, risk_score
        )

        return ClassifiedTWAP(
            detection=detection,
            size_category=size_cat,
            urgency_category=urgency_cat,
            confidence_level=confidence,
            risk_score=risk_score,
            description=description,
        )

    def classify_all(self, detections: List[TWAPDetection]) -> List[ClassifiedTWAP]:
        """
        Classify multiple detections.

        Args:
            detections: List of TWAPDetection objects

        Returns:
            List of ClassifiedTWAP objects, sorted by risk score descending
        """
        classified = [self.classify(d) for d in detections]
        classified.sort(key=lambda c: c.risk_score, reverse=True)
        return classified


def classify_detections(detections: List[TWAPDetection]) -> List[ClassifiedTWAP]:
    """
    Convenience function to classify detections.

    Args:
        detections: List of TWAPDetection objects

    Returns:
        List of ClassifiedTWAP objects
    """
    classifier = TWAPClassifier()
    return classifier.classify_all(detections)


def generate_summary_report(classified: List[ClassifiedTWAP]) -> str:
    """
    Generate a summary report of all classified TWAPs.

    Args:
        classified: List of ClassifiedTWAP objects

    Returns:
        Formatted string report
    """
    if not classified:
        return "No TWAP patterns detected."

    lines = [
        "=" * 60,
        "TWAP DETECTION SUMMARY REPORT",
        "=" * 60,
        "",
    ]

    # Summary stats
    buy_twaps = [c for c in classified if c.detection.side == "buy"]
    sell_twaps = [c for c in classified if c.detection.side == "sell"]

    total_buy_value = sum(c.detection.estimated_total_value for c in buy_twaps)
    total_sell_value = sum(c.detection.estimated_total_value for c in sell_twaps)

    lines.extend([
        f"Total Patterns Detected: {len(classified)}",
        f"  Buy TWAPs:  {len(buy_twaps)} (Est. ${total_buy_value:,.0f})",
        f"  Sell TWAPs: {len(sell_twaps)} (Est. ${total_sell_value:,.0f})",
        "",
        "Net Flow Bias: " + (
            f"BUYING (+${total_buy_value - total_sell_value:,.0f})"
            if total_buy_value > total_sell_value
            else f"SELLING (-${total_sell_value - total_buy_value:,.0f})"
        ),
        "",
        "-" * 60,
        "DETECTED PATTERNS (sorted by risk score)",
        "-" * 60,
        "",
    ])

    # Individual patterns
    for i, c in enumerate(classified, 1):
        lines.extend([
            f"[{i}] {c.detection.side.upper()} TWAP - {c.size_category.value} ({c.urgency_category.value})",
            f"    Interval: {c.detection.interval_seconds:.1f}s",
            f"    Per-execution: {c.detection.estimated_per_execution_size:.6f} (${c.detection.estimated_per_execution_value:,.0f})",
            f"    Estimated Total: {c.detection.estimated_total_size:.4f} (${c.detection.estimated_total_value:,.0f})",
            f"    Confidence: {c.confidence_level.value} (SNR: {c.detection.snr:.1f})",
            f"    Risk Score: {c.risk_score:.0f}/100",
            f"    Analysis: {c.description}",
            "",
        ])

    lines.append("=" * 60)

    return "\n".join(lines)


def main():
    """Test classifier with sample data."""
    from twap_fourier_analyzer import TWAPDetection

    # Create sample detections
    detections = [
        TWAPDetection(
            side="sell",
            interval_seconds=30,
            frequency_hz=1/30,
            amplitude=0.5,
            snr=8.5,
            confidence="HIGH",
            estimated_per_execution_size=0.5,
            estimated_per_execution_value=25000,
            duration_detected_seconds=600,
            estimated_total_size=10,
            estimated_total_value=500000,
        ),
        TWAPDetection(
            side="buy",
            interval_seconds=15,
            frequency_hz=1/15,
            amplitude=0.1,
            snr=4.2,
            confidence="MEDIUM",
            estimated_per_execution_size=0.1,
            estimated_per_execution_value=5000,
            duration_detected_seconds=600,
            estimated_total_size=4,
            estimated_total_value=200000,
        ),
    ]

    classifier = TWAPClassifier()
    classified = classifier.classify_all(detections)

    report = generate_summary_report(classified)
    print(report)


if __name__ == "__main__":
    main()
