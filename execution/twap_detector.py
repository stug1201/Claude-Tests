#!/usr/bin/env python3
"""
TWAP Detector - Main CLI Interface

Real-time detection of TWAP algorithmic orders on Binance Perpetual Futures
using Fourier Transform analysis of trade flow patterns.

Usage:
    python execution/twap_detector.py
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import List, Optional, Tuple

# Add execution directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from twap_data_collector import TradeCollector, Trade, get_available_pairs
from twap_fourier_analyzer import TWAPAnalyzer
from twap_classifier import TWAPClassifier, generate_summary_report


class TrackedTWAP:
    """Represents a tracked TWAP with a persistent name."""

    def __init__(self, name: str, side: str, frequency_hz: float, first_seen: datetime):
        self.name = name
        self.side = side
        self.frequency_hz = frequency_hz
        self.first_seen = first_seen
        self.last_seen = first_seen
        self.detection_count = 1

    @property
    def interval_seconds(self) -> float:
        return 1.0 / self.frequency_hz if self.frequency_hz > 0 else 0

    def matches(self, side: str, frequency_hz: float, tolerance: float = 0.15) -> bool:
        """Check if a detection matches this tracked TWAP."""
        if side != self.side:
            return False
        freq_diff = abs(frequency_hz - self.frequency_hz) / self.frequency_hz
        return freq_diff <= tolerance

    def update(self, detection_time: datetime) -> None:
        """Update tracking when TWAP is re-detected."""
        self.last_seen = detection_time
        self.detection_count += 1


class TWAPDetectorCLI:
    """
    Interactive CLI for TWAP detection on Binance Perpetual Futures.
    """

    # Greek letters for naming TWAPs
    TWAP_NAMES = [
        "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta",
        "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi",
        "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi", "Psi", "Omega"
    ]

    def __init__(self):
        self.collector: Optional[TradeCollector] = None
        self.classifier = TWAPClassifier()
        self.analysis_interval_sec = 30
        self.min_buffer_sec = 120
        self.buffer_minutes = 30

        # Detection state
        self.all_detections = []
        self.running = False

        # TWAP tracking
        self.tracked_twaps: List[TrackedTWAP] = []
        self.next_name_index = 0

    def clear_screen(self) -> None:
        """Clear terminal screen."""
        os.system('cls' if os.name == 'nt' else 'clear')

    def print_header(self) -> None:
        """Print application header."""
        print("=" * 60)
        print("  TWAP DETECTOR - Binance Perpetual Futures")
        print("  Using Fourier Transform Analysis")
        print("=" * 60)
        print()

    def get_or_create_tracked_twap(
        self, side: str, frequency_hz: float
    ) -> Tuple[TrackedTWAP, bool]:
        """Find existing tracked TWAP or create a new one."""
        now = datetime.now()

        for tracked in self.tracked_twaps:
            if tracked.matches(side, frequency_hz):
                tracked.update(now)
                return tracked, False

        if self.next_name_index < len(self.TWAP_NAMES):
            name = self.TWAP_NAMES[self.next_name_index]
        else:
            name = f"TWAP-{self.next_name_index + 1}"

        self.next_name_index += 1
        new_twap = TrackedTWAP(name, side, frequency_hz, now)
        self.tracked_twaps.append(new_twap)
        return new_twap, True

    def select_symbol(self) -> str:
        """Menu to select trading pair."""
        pairs = get_available_pairs()
        print("\nAvailable Binance Perpetual pairs:")

        cols = 3
        for i in range(0, len(pairs), cols):
            row = pairs[i:i+cols]
            print("  " + "  ".join(f"[{j+1:2}] {p:<12}" for j, p in enumerate(row, i)))

        print()
        print(f"  [{len(pairs)+1}] Enter custom symbol")
        print()

        while True:
            try:
                choice = input("Enter selection: ").strip()
                idx = int(choice) - 1

                if idx == len(pairs):
                    custom = input("Enter symbol (e.g., BTCUSDT): ").strip().upper()
                    if custom:
                        return custom
                elif 0 <= idx < len(pairs):
                    return pairs[idx]
                else:
                    print(f"Please enter a number between 1 and {len(pairs)+1}")
            except ValueError:
                print("Invalid input. Please enter a number.")
            except KeyboardInterrupt:
                print("\nExiting...")
                sys.exit(0)

    def format_detection_alert(
        self, classified_twap, tracked: TrackedTWAP, is_new: bool
    ) -> str:
        """Format a detection for display with tracking info."""
        d = classified_twap.detection
        c = classified_twap

        if is_new:
            status_line = f"🎯 NEW TWAP DETECTED: \"{tracked.name}\""
        else:
            duration = (tracked.last_seen - tracked.first_seen).total_seconds()
            duration_str = f"{duration:.0f}s" if duration < 60 else f"{duration/60:.1f}min"
            status_line = (
                f"🔄 TWAP UPDATE: \"{tracked.name}\" "
                f"(seen {tracked.detection_count}x over {duration_str})"
            )

        return (
            f"\n{'='*60}\n"
            f"{status_line}\n"
            f"{'='*60}\n"
            f"  Name:          {tracked.name}\n"
            f"  Side:          {d.side.upper()}\n"
            f"  Category:      {c.size_category.value} ({c.urgency_category.value})\n"
            f"  Interval:      {d.interval_seconds:.1f} seconds\n"
            f"  Per-execution: {d.estimated_per_execution_size:.6f} "
            f"(~${d.estimated_per_execution_value:,.0f})\n"
            f"  Est. Total:    {d.estimated_total_size:.4f} "
            f"(~${d.estimated_total_value:,.0f})\n"
            f"  Confidence:    {c.confidence_level.value} (SNR: {d.snr:.1f})\n"
            f"  Risk Score:    {c.risk_score:.0f}/100\n"
            f"{'='*60}\n"
            f"  {c.description}\n"
        )

    def on_trade(self, trade: Trade) -> None:
        """Callback for each trade received."""
        pass

    async def run_analysis_loop(self) -> None:
        """Periodically analyze collected data for TWAPs."""
        last_analysis = datetime.now()

        while self.running:
            await asyncio.sleep(5)

            stats = self.collector.get_stats()
            buffer_sec = stats.get("buffer_duration_sec", 0)

            now = datetime.now().strftime("%H:%M:%S")
            active_count = len(self.tracked_twaps)
            active_str = f" | Active TWAPs: {active_count}" if active_count > 0 else ""
            print(
                f"\r[{now}] Trades: {stats.get('trades_in_buffer', 0):,} | "
                f"Buffer: {buffer_sec:.0f}s | "
                f"Vol: {stats.get('total_volume', 0):.4f} "
                f"(B:{stats.get('buy_volume', 0):.4f} S:{stats.get('sell_volume', 0):.4f})"
                f"{active_str}",
                end="",
                flush=True,
            )

            if buffer_sec < self.min_buffer_sec:
                continue

            elapsed = (datetime.now() - last_analysis).total_seconds()
            if elapsed < self.analysis_interval_sec:
                continue

            last_analysis = datetime.now()
            print()

            trades = self.collector.get_trades()
            if not trades:
                continue

            analyzer = TWAPAnalyzer(trades, bucket_size_ms=1000)
            detections = analyzer.detect_twaps()

            alerts_shown = 0
            for det in detections:
                tracked, is_new = self.get_or_create_tracked_twap(
                    det.side, det.frequency_hz
                )

                if is_new:
                    self.all_detections.append(det)

                classified = self.classifier.classify(det)
                print(self.format_detection_alert(classified, tracked, is_new))
                alerts_shown += 1

            if alerts_shown == 0:
                print(f"[{now}] Analysis complete - no patterns detected")

    async def monitor(self, symbol: str) -> None:
        """Start monitoring for TWAPs."""
        self.clear_screen()
        self.print_header()

        print(f"Configuration:")
        print(f"  Symbol:      {symbol}")
        print(f"  Market:      Binance Perpetual Futures")
        print(f"  Buffer:      {self.buffer_minutes} minutes")
        print(f"  Analysis:    Every {self.analysis_interval_sec} seconds")
        print()
        print("Starting data collection...")
        print("(Press Ctrl+C to stop and see summary)\n")

        self.collector = TradeCollector(
            symbol=symbol,
            buffer_minutes=self.buffer_minutes,
            on_trade=self.on_trade,
        )

        self.running = True
        self.all_detections = []
        self.tracked_twaps = []
        self.next_name_index = 0

        try:
            await asyncio.gather(
                self.collector.start(),
                self.run_analysis_loop(),
            )
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            await self.collector.stop()

            print("\n\n")
            if self.tracked_twaps:
                print("=" * 60)
                print("SESSION SUMMARY")
                print("=" * 60)
                print(f"\nTotal unique TWAPs detected: {len(self.tracked_twaps)}\n")

                for tracked in self.tracked_twaps:
                    duration = (tracked.last_seen - tracked.first_seen).total_seconds()
                    duration_str = f"{duration:.0f} seconds" if duration < 60 else f"{duration/60:.1f} minutes"

                    print(f"  [{tracked.name}] {tracked.side.upper()} TWAP")
                    print(f"      Interval: {tracked.interval_seconds:.1f}s")
                    print(f"      Detected: {tracked.detection_count} times over {duration_str}")
                    print()

                if self.all_detections:
                    classified = self.classifier.classify_all(self.all_detections)
                    report = generate_summary_report(classified)
                    print(report)
            else:
                print("No TWAP patterns were detected during this session.")

    def run(self) -> None:
        """Main entry point."""
        self.clear_screen()
        self.print_header()

        symbol = self.select_symbol()

        try:
            asyncio.run(self.monitor(symbol))
        except KeyboardInterrupt:
            print("\nExiting...")


def main():
    """Entry point."""
    cli = TWAPDetectorCLI()
    cli.run()


if __name__ == "__main__":
    main()
