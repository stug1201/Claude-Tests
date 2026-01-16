#!/usr/bin/env python3
"""
TWAP Detector - Main CLI Interface

Real-time detection of TWAP algorithmic orders on cryptocurrency exchanges
using Fourier Transform analysis of trade flow patterns.

Usage:
    python execution/twap_detector.py

Supports:
    - Binance Spot & Perpetual Futures
    - Coinbase Spot & Perpetual Futures
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Optional

# Add execution directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from twap_data_collector import (
    TradeCollector,
    Exchange,
    MarketType,
    get_available_pairs,
    Trade,
)
from twap_fourier_analyzer import TWAPAnalyzer
from twap_classifier import TWAPClassifier, generate_summary_report


class TWAPDetectorCLI:
    """
    Interactive CLI for TWAP detection.
    """

    def __init__(self):
        self.collector: Optional[TradeCollector] = None
        self.classifier = TWAPClassifier()
        self.analysis_interval_sec = 30  # Run analysis every N seconds
        self.min_buffer_sec = 120  # Minimum data before first analysis
        self.buffer_minutes = 30  # Total buffer size

        # Detection state
        self.all_detections = []
        self.running = False

    def clear_screen(self) -> None:
        """Clear terminal screen."""
        os.system('cls' if os.name == 'nt' else 'clear')

    def print_header(self) -> None:
        """Print application header."""
        print("=" * 60)
        print("  TWAP DETECTOR - Algorithmic Order Detection")
        print("  Using Fourier Transform Analysis")
        print("=" * 60)
        print()

    def print_menu(self, title: str, options: list[str]) -> int:
        """
        Display a numbered menu and get user selection.

        Args:
            title: Menu title
            options: List of option strings

        Returns:
            Selected index (0-based)
        """
        print(f"\n{title}")
        print("-" * 40)
        for i, opt in enumerate(options, 1):
            print(f"  [{i}] {opt}")
        print()

        while True:
            try:
                choice = input("Enter selection: ").strip()
                idx = int(choice) - 1
                if 0 <= idx < len(options):
                    return idx
                print(f"Please enter a number between 1 and {len(options)}")
            except ValueError:
                print("Invalid input. Please enter a number.")
            except KeyboardInterrupt:
                print("\nExiting...")
                sys.exit(0)

    def select_exchange(self) -> Exchange:
        """Menu to select exchange."""
        options = ["Binance", "Coinbase"]
        idx = self.print_menu("SELECT EXCHANGE", options)
        return Exchange.BINANCE if idx == 0 else Exchange.COINBASE

    def select_market_type(self) -> MarketType:
        """Menu to select market type."""
        options = ["Spot Market", "Perpetual Futures"]
        idx = self.print_menu("SELECT MARKET TYPE", options)
        return MarketType.SPOT if idx == 0 else MarketType.PERPETUAL

    def select_symbol(self, exchange: Exchange, market_type: MarketType) -> str:
        """Menu to select trading pair."""
        pairs = get_available_pairs(exchange, market_type)
        print(f"\nAvailable pairs on {exchange.value} {market_type.value}:")

        # Show in columns
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
                    # Custom symbol
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

    def format_detection_alert(self, classified_twap) -> str:
        """Format a detection for display."""
        d = classified_twap.detection
        c = classified_twap

        return (
            f"\n{'='*60}\n"
            f"🎯 TWAP DETECTED!\n"
            f"{'='*60}\n"
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
        # Optionally print trade ticks (commented out for cleaner output)
        # side_char = "↑" if trade.side == "buy" else "↓"
        # print(f"  {side_char} {trade.size:.6f} @ ${trade.price:.2f}")
        pass

    async def run_analysis_loop(self) -> None:
        """Periodically analyze collected data for TWAPs."""
        last_analysis = datetime.now()
        detected_frequencies = set()  # Track already-detected patterns

        while self.running:
            await asyncio.sleep(5)  # Check every 5 seconds

            stats = self.collector.get_stats()
            buffer_sec = stats.get("buffer_duration_sec", 0)

            # Status update
            now = datetime.now().strftime("%H:%M:%S")
            print(
                f"\r[{now}] Trades: {stats.get('trades_in_buffer', 0):,} | "
                f"Buffer: {buffer_sec:.0f}s | "
                f"Vol: {stats.get('total_volume', 0):.4f} "
                f"(B:{stats.get('buy_volume', 0):.4f} S:{stats.get('sell_volume', 0):.4f})",
                end="",
                flush=True,
            )

            # Wait for minimum buffer before analysis
            if buffer_sec < self.min_buffer_sec:
                continue

            # Run analysis at interval
            elapsed = (datetime.now() - last_analysis).total_seconds()
            if elapsed < self.analysis_interval_sec:
                continue

            last_analysis = datetime.now()
            print()  # Newline before analysis output

            # Get trades and analyze
            trades = self.collector.get_trades()
            if not trades:
                continue

            analyzer = TWAPAnalyzer(trades, bucket_size_ms=1000)
            detections = analyzer.detect_twaps()

            # Filter new detections (avoid duplicate alerts)
            new_detections = []
            for det in detections:
                # Use rounded frequency as signature
                freq_sig = round(det.frequency_hz, 3)
                side_sig = det.side
                key = (side_sig, freq_sig)

                if key not in detected_frequencies:
                    detected_frequencies.add(key)
                    new_detections.append(det)
                    self.all_detections.append(det)

            # Display new detections
            if new_detections:
                classified = self.classifier.classify_all(new_detections)
                for c in classified:
                    print(self.format_detection_alert(c))
            else:
                print(f"[{now}] Analysis complete - no new patterns detected")

    async def monitor(
        self,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
    ) -> None:
        """
        Start monitoring for TWAPs.

        Args:
            exchange: Exchange to monitor
            market_type: Spot or Perpetual
            symbol: Trading pair symbol
        """
        self.clear_screen()
        self.print_header()

        print(f"Configuration:")
        print(f"  Exchange:    {exchange.value.title()}")
        print(f"  Market:      {market_type.value.title()}")
        print(f"  Symbol:      {symbol}")
        print(f"  Buffer:      {self.buffer_minutes} minutes")
        print(f"  Analysis:    Every {self.analysis_interval_sec} seconds")
        print()
        print("Starting data collection...")
        print("(Press Ctrl+C to stop and see summary)\n")

        # Create collector
        self.collector = TradeCollector(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            buffer_minutes=self.buffer_minutes,
            on_trade=self.on_trade,
        )

        self.running = True
        self.all_detections = []

        # Run collector and analysis loop concurrently
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

            # Print final summary
            print("\n\n")
            if self.all_detections:
                classified = self.classifier.classify_all(self.all_detections)
                report = generate_summary_report(classified)
                print(report)
            else:
                print("No TWAP patterns were detected during this session.")

    def run(self) -> None:
        """Main entry point - run the interactive CLI."""
        self.clear_screen()
        self.print_header()

        # Get user selections
        exchange = self.select_exchange()
        market_type = self.select_market_type()
        symbol = self.select_symbol(exchange, market_type)

        # Start monitoring
        try:
            asyncio.run(self.monitor(exchange, market_type, symbol))
        except KeyboardInterrupt:
            print("\nExiting...")


def main():
    """Entry point."""
    cli = TWAPDetectorCLI()
    cli.run()


if __name__ == "__main__":
    main()
