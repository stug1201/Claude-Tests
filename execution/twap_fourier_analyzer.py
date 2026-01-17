#!/usr/bin/env python3
"""
TWAP Fourier Analyzer

Analyzes trade data using FFT to detect periodic execution patterns
characteristic of TWAP algorithmic orders.

Usage:
    from twap_fourier_analyzer import TWAPAnalyzer, analyze_trades

    analyzer = TWAPAnalyzer(trades, bucket_size_ms=1000)
    detections = analyzer.detect_twaps()
"""

import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Tuple
from enum import Enum


class TradeSide(Enum):
    BUY = "buy"
    SELL = "sell"
    NET = "net"  # Buy - Sell


@dataclass
class TWAPDetection:
    """Represents a detected TWAP pattern."""
    side: str  # "buy" or "sell"
    interval_seconds: float
    frequency_hz: float
    amplitude: float  # Volume per execution
    snr: float  # Signal-to-noise ratio
    confidence: str  # "LOW", "MEDIUM", "HIGH"
    estimated_per_execution_size: float
    estimated_per_execution_value: float  # USD
    duration_detected_seconds: float
    estimated_total_size: float
    estimated_total_value: float

    def __str__(self) -> str:
        return (
            f"TWAP {self.side.upper()}: "
            f"interval={self.interval_seconds:.1f}s, "
            f"size/exec={self.estimated_per_execution_size:.6f}, "
            f"SNR={self.snr:.1f}, "
            f"confidence={self.confidence}"
        )


@dataclass
class TimeBucket:
    """Aggregated trade data for a time bucket."""
    timestamp_ms: int
    buy_volume: float
    sell_volume: float
    buy_count: int
    sell_count: int

    @property
    def net_volume(self) -> float:
        return self.buy_volume - self.sell_volume

    @property
    def total_volume(self) -> float:
        return self.buy_volume + self.sell_volume


class TWAPAnalyzer:
    """
    Analyzes trade data for TWAP patterns using FFT.
    """

    # Detection thresholds
    MIN_SNR = 3.0  # Minimum signal-to-noise ratio
    MIN_INTERVAL_SEC = 5.0  # Minimum TWAP interval (ignore faster patterns)
    MAX_INTERVAL_SEC = 300.0  # Maximum TWAP interval (5 minutes)
    MIN_CYCLES = 5  # Minimum number of cycles to consider valid

    def __init__(
        self,
        trades: list,
        bucket_size_ms: int = 1000,
        avg_price: Optional[float] = None,
    ):
        """
        Initialize analyzer with trade data.

        Args:
            trades: List of Trade objects from data collector
            bucket_size_ms: Time bucket size for aggregation (default 1 second)
            avg_price: Average price for USD calculations (auto-calculated if None)
        """
        self.trades = trades
        self.bucket_size_ms = bucket_size_ms
        self.buckets: List[TimeBucket] = []
        self.avg_price = avg_price

        if trades:
            self._aggregate_trades()
            if self.avg_price is None:
                self.avg_price = sum(t.price for t in trades) / len(trades)

    def _aggregate_trades(self) -> None:
        """Aggregate trades into time buckets."""
        if not self.trades:
            return

        # Find time range
        min_ts = min(t.timestamp_ms for t in self.trades)
        max_ts = max(t.timestamp_ms for t in self.trades)

        # Create buckets
        bucket_count = (max_ts - min_ts) // self.bucket_size_ms + 1
        buckets_dict = {}

        for i in range(bucket_count):
            ts = min_ts + i * self.bucket_size_ms
            buckets_dict[ts] = TimeBucket(
                timestamp_ms=ts,
                buy_volume=0.0,
                sell_volume=0.0,
                buy_count=0,
                sell_count=0,
            )

        # Fill buckets
        for trade in self.trades:
            bucket_ts = min_ts + ((trade.timestamp_ms - min_ts) // self.bucket_size_ms) * self.bucket_size_ms
            bucket = buckets_dict.get(bucket_ts)
            if bucket:
                if trade.side == "buy":
                    bucket.buy_volume += trade.size
                    bucket.buy_count += 1
                else:
                    bucket.sell_volume += trade.size
                    bucket.sell_count += 1

        self.buckets = list(buckets_dict.values())

    def _get_signal(self, side: TradeSide) -> np.ndarray:
        """Extract signal array for specified side."""
        if side == TradeSide.BUY:
            return np.array([b.buy_volume for b in self.buckets])
        elif side == TradeSide.SELL:
            return np.array([b.sell_volume for b in self.buckets])
        else:
            return np.array([b.net_volume for b in self.buckets])

    def _apply_window(self, signal: np.ndarray) -> np.ndarray:
        """Apply Hann window to reduce spectral leakage."""
        window = np.hanning(len(signal))
        return signal * window

    def _compute_fft(self, signal: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Compute FFT and return frequencies and power spectrum.

        Returns:
            Tuple of (frequencies in Hz, power spectrum)
        """
        n = len(signal)
        if n < 2:
            return np.array([]), np.array([])

        # Apply window
        windowed = self._apply_window(signal)

        # Compute FFT
        fft_result = np.fft.rfft(windowed)
        power = np.abs(fft_result) ** 2

        # Compute frequencies
        sample_rate = 1000 / self.bucket_size_ms  # Samples per second
        frequencies = np.fft.rfftfreq(n, d=1/sample_rate)

        return frequencies, power

    def _find_peaks(
        self,
        frequencies: np.ndarray,
        power: np.ndarray,
        min_snr: float = MIN_SNR,
    ) -> List[Tuple[float, float, float]]:
        """
        Find significant peaks in power spectrum.

        Returns:
            List of (frequency, power, snr) tuples for detected peaks
        """
        if len(power) < 10:
            return []

        # Calculate noise floor (median of power spectrum, excluding DC)
        noise_floor = np.median(power[1:])
        noise_std = np.std(power[1:])

        if noise_floor == 0 or noise_std == 0:
            return []

        peaks = []

        # Find local maxima
        for i in range(2, len(power) - 2):
            freq = frequencies[i]

            # Check frequency bounds (convert to interval)
            if freq <= 0:
                continue
            interval = 1 / freq
            if interval < self.MIN_INTERVAL_SEC or interval > self.MAX_INTERVAL_SEC:
                continue

            # Check if local maximum
            if power[i] > power[i-1] and power[i] > power[i+1]:
                if power[i] > power[i-2] and power[i] > power[i+2]:
                    # Calculate SNR
                    snr = (power[i] - noise_floor) / noise_std

                    if snr >= min_snr:
                        peaks.append((freq, power[i], snr))

        # Sort by SNR descending
        peaks.sort(key=lambda x: x[2], reverse=True)

        return peaks

    def _estimate_amplitude(self, frequency: float, power: float) -> float:
        """
        Estimate per-execution volume from FFT amplitude.

        The amplitude in the FFT corresponds roughly to the periodic
        component's contribution. We scale by the number of samples
        and account for the windowing.
        """
        n = len(self.buckets)
        if n == 0:
            return 0.0

        # FFT amplitude to signal amplitude conversion
        # Factor of 2 for single-sided spectrum, sqrt for power->amplitude
        amplitude = np.sqrt(power) * 2 / n

        # Correct for Hann window (reduces amplitude by ~0.5)
        amplitude *= 2

        return amplitude

    def _calculate_confidence(self, snr: float, cycles: float) -> str:
        """Determine confidence level based on SNR and number of cycles."""
        if snr >= 8 and cycles >= 20:
            return "HIGH"
        elif snr >= 5 and cycles >= 10:
            return "MEDIUM"
        else:
            return "LOW"

    def detect_twaps(
        self,
        min_snr: float = MIN_SNR,
        analyze_sides: Optional[List[str]] = None,
    ) -> List[TWAPDetection]:
        """
        Detect TWAP patterns in the trade data.

        Args:
            min_snr: Minimum signal-to-noise ratio for detection
            analyze_sides: List of sides to analyze ("buy", "sell", or both)

        Returns:
            List of TWAPDetection objects
        """
        if not self.buckets:
            return []

        if analyze_sides is None:
            analyze_sides = ["buy", "sell"]

        detections = []
        duration_sec = len(self.buckets) * self.bucket_size_ms / 1000

        for side_str in analyze_sides:
            side = TradeSide.BUY if side_str == "buy" else TradeSide.SELL
            signal = self._get_signal(side)

            # Skip if no activity
            if np.sum(signal) == 0:
                continue

            frequencies, power = self._compute_fft(signal)
            peaks = self._find_peaks(frequencies, power, min_snr)

            for freq, pwr, snr in peaks:
                interval = 1 / freq
                cycles = duration_sec / interval

                # Must have minimum cycles
                if cycles < self.MIN_CYCLES:
                    continue

                amplitude = self._estimate_amplitude(freq, pwr)
                confidence = self._calculate_confidence(snr, cycles)

                # Estimate totals
                num_executions = duration_sec / interval
                total_size = amplitude * num_executions
                per_exec_value = amplitude * (self.avg_price or 0)
                total_value = total_size * (self.avg_price or 0)

                detection = TWAPDetection(
                    side=side_str,
                    interval_seconds=interval,
                    frequency_hz=freq,
                    amplitude=amplitude,
                    snr=snr,
                    confidence=confidence,
                    estimated_per_execution_size=amplitude,
                    estimated_per_execution_value=per_exec_value,
                    duration_detected_seconds=duration_sec,
                    estimated_total_size=total_size,
                    estimated_total_value=total_value,
                )
                detections.append(detection)

        # Sort by estimated total value descending
        detections.sort(key=lambda d: d.estimated_total_value, reverse=True)

        return detections

    def get_spectrum_data(self, side: str = "buy") -> dict:
        """
        Get FFT spectrum data for visualization.

        Returns dict with frequencies, power, and detected peaks.
        """
        trade_side = TradeSide.BUY if side == "buy" else TradeSide.SELL
        signal = self._get_signal(trade_side)
        frequencies, power = self._compute_fft(signal)
        peaks = self._find_peaks(frequencies, power)

        return {
            "frequencies": frequencies.tolist(),
            "power": power.tolist(),
            "peaks": [(f, p, s) for f, p, s in peaks],
            "bucket_size_ms": self.bucket_size_ms,
            "total_buckets": len(self.buckets),
        }


def analyze_trades(
    trades: list,
    bucket_size_ms: int = 1000,
    min_snr: float = 3.0,
) -> List[TWAPDetection]:
    """
    Convenience function to analyze trades for TWAP patterns.

    Args:
        trades: List of Trade objects
        bucket_size_ms: Aggregation bucket size
        min_snr: Minimum signal-to-noise ratio

    Returns:
        List of detected TWAP patterns
    """
    analyzer = TWAPAnalyzer(trades, bucket_size_ms)
    return analyzer.detect_twaps(min_snr=min_snr)


# Test with synthetic data
def _generate_synthetic_twap(
    duration_sec: int = 600,
    interval_sec: float = 30,
    size_per_exec: float = 0.5,
    noise_level: float = 0.3,
    side: str = "sell",
) -> list:
    """Generate synthetic trade data with a TWAP pattern for testing."""
    from dataclasses import dataclass as dc
    import random

    @dc
    class FakeTrade:
        timestamp_ms: int
        price: float
        size: float
        side: str

    trades = []
    base_time = 1700000000000  # Arbitrary start

    # Generate TWAP executions
    num_executions = int(duration_sec / interval_sec)
    for i in range(num_executions):
        ts = base_time + int(i * interval_sec * 1000)
        # Add some jitter to execution time
        ts += random.randint(-500, 500)
        # Add some variation to size
        size = size_per_exec * (1 + random.uniform(-0.1, 0.1))
        trades.append(FakeTrade(
            timestamp_ms=ts,
            price=50000,
            size=size,
            side=side,
        ))

    # Add random noise trades
    num_noise = int(duration_sec * noise_level)
    for _ in range(num_noise):
        ts = base_time + random.randint(0, duration_sec * 1000)
        trades.append(FakeTrade(
            timestamp_ms=ts,
            price=50000,
            size=random.uniform(0.01, 0.2),
            side=random.choice(["buy", "sell"]),
        ))

    # Sort by timestamp
    trades.sort(key=lambda t: t.timestamp_ms)
    return trades


def main():
    """Test the analyzer with synthetic data."""
    print("Generating synthetic TWAP data...")
    print("  - 10 minute duration")
    print("  - 30 second interval SELL TWAP")
    print("  - 0.5 BTC per execution")
    print()

    trades = _generate_synthetic_twap(
        duration_sec=600,
        interval_sec=30,
        size_per_exec=0.5,
        noise_level=0.5,
        side="sell",
    )

    print(f"Generated {len(trades)} trades")
    print()

    analyzer = TWAPAnalyzer(trades, bucket_size_ms=1000, avg_price=50000)
    detections = analyzer.detect_twaps()

    print(f"Detected {len(detections)} TWAP pattern(s):")
    print()

    for det in detections:
        print(f"  {det.side.upper()} TWAP")
        print(f"    Interval: {det.interval_seconds:.1f} seconds")
        print(f"    Per-execution: {det.estimated_per_execution_size:.4f} BTC (${det.estimated_per_execution_value:,.0f})")
        print(f"    SNR: {det.snr:.1f}")
        print(f"    Confidence: {det.confidence}")
        print(f"    Est. total: {det.estimated_total_size:.2f} BTC (${det.estimated_total_value:,.0f})")
        print()


if __name__ == "__main__":
    main()
