#!/usr/bin/env python3
"""
Spread Anomaly Monitor

Monitors bid-ask spread for statistical anomalies using exponential moving averages.
Optimized for minimal memory overhead - uses O(1) space per ticker.

Usage:
    from spread_monitor import SpreadMonitor

    monitor = SpreadMonitor(symbol="BTCUSDT", z_threshold=3.0)
    await monitor.start()
"""

import asyncio
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional, List

import websockets

from twap_data_collector import parse_symbol, format_symbol, BINANCE_PERP_WS, BINANCE_SPOT_WS


@dataclass
class SpreadAnomaly:
    """Represents a detected spread anomaly."""
    symbol: str
    timestamp_ms: int
    spread_bps: float  # Spread in basis points
    mean_bps: float
    std_bps: float
    z_score: float
    multiplier: float  # How many times normal spread (e.g., 3.5 = 3.5x normal)
    bid: float
    ask: float
    mid: float

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000)

    @property
    def is_wide(self) -> bool:
        """True if spread is abnormally wide."""
        return self.multiplier > 1.0

    @property
    def severity(self) -> str:
        """Severity based on multiplier (how many times normal)."""
        m = self.multiplier
        if m >= 10:
            return "EXTREME"
        elif m >= 5:
            return "SEVERE"
        elif m >= 3:
            return "HIGH"
        else:
            return "MODERATE"


class ExponentialStats:
    """
    Maintains exponential moving average and variance with O(1) space.

    Uses Welford's online algorithm adapted for exponential weighting.
    Includes minimum std floor to prevent low variance from causing huge z-scores.
    """

    def __init__(self, alpha: float = 0.01, warmup_samples: int = 100, min_std_pct: float = 0.5):
        """
        Args:
            alpha: Smoothing factor (0 < alpha < 1). Lower = slower adaptation.
                   Effective window ~ 2/alpha samples.
            warmup_samples: Minimum samples before stats are considered valid.
            min_std_pct: Minimum std as percentage of mean (0.5 = 50%).
                         This prevents low variance periods from causing huge z-scores.
                         With 0.5, a 3x spread change = z-score of ~4.
        """
        self.alpha = alpha
        self.warmup_samples = warmup_samples
        self.min_std_pct = min_std_pct
        self.mean = 0.0
        self.variance = 0.0
        self.count = 0
        self._initialized = False

    def update(self, value: float) -> None:
        """Update statistics with new value."""
        self.count += 1

        if not self._initialized:
            # First value - initialize
            self.mean = value
            self.variance = 0.0
            self._initialized = True
            return

        # Exponential moving average
        delta = value - self.mean
        self.mean += self.alpha * delta

        # Exponential moving variance (biased but stable)
        self.variance = (1 - self.alpha) * (self.variance + self.alpha * delta * delta)

    @property
    def std(self) -> float:
        """Raw standard deviation."""
        return math.sqrt(max(0, self.variance))

    @property
    def effective_std(self) -> float:
        """Standard deviation with minimum floor based on mean.

        This ensures z-scores are meaningful even during low variance periods.
        With min_std_pct=0.5, we need at least a 50% change from mean to start
        accumulating z-score, making it correlate better with actual magnitude changes.
        """
        min_std = abs(self.mean) * self.min_std_pct
        return max(self.std, min_std)

    @property
    def is_valid(self) -> bool:
        """Whether we have enough samples for reliable stats."""
        return self.count >= self.warmup_samples

    def z_score(self, value: float) -> float:
        """Calculate z-score using effective_std (with floor)."""
        eff_std = self.effective_std
        if eff_std < 1e-10:
            return 0.0
        return (value - self.mean) / eff_std

    def multiplier(self, value: float) -> float:
        """Calculate how many times the mean this value is."""
        if self.mean < 1e-10:
            return 1.0
        return value / self.mean


class SpreadMonitor:
    """
    Monitors bid-ask spread for anomalies using book ticker stream.

    Uses exponential moving statistics for O(1) memory per ticker.
    Detects both abnormally wide and narrow spreads.

    Has a warmup period where it collects data to establish a baseline
    before triggering any anomaly alerts (similar to TWAP buffer approach).
    """

    def __init__(
        self,
        symbol: str,
        z_threshold: float = 3.0,
        alpha: float = 0.01,
        warmup_seconds: float = 300.0,  # 5 minutes warmup by default
        cooldown_sec: float = 60.0,
        on_anomaly: Optional[Callable[[SpreadAnomaly], None]] = None,
    ):
        """
        Args:
            symbol: Trading pair (e.g., "BTCUSDT" or "BTCUSDT.S")
            z_threshold: Z-score threshold for anomaly detection
            alpha: EMA smoothing factor (lower = slower adaptation)
            warmup_seconds: Seconds to collect data before detection is active (default 5 min)
            cooldown_sec: Minimum seconds between alerts for same symbol
            on_anomaly: Callback for detected anomalies
        """
        self.original_symbol = symbol
        self.base_symbol, self.is_spot = parse_symbol(symbol)
        self.symbol = format_symbol(self.base_symbol, self.is_spot)

        self.z_threshold = z_threshold
        self.warmup_seconds = warmup_seconds
        self.cooldown_sec = cooldown_sec
        self.on_anomaly = on_anomaly

        # Statistics tracker (warmup based on time, not samples)
        self.stats = ExponentialStats(alpha=alpha, warmup_samples=1)  # No sample-based warmup

        # Connection state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False

        # Tracking
        self.updates_received = 0
        self.anomalies_detected = 0
        self.last_anomaly_time = 0.0
        self.last_spread_bps = 0.0
        self.last_bid = 0.0
        self.last_ask = 0.0

        # Time-based warmup
        self._start_time: Optional[float] = None
        self._is_warmed_up = False

    @property
    def market_type(self) -> str:
        return "Spot" if self.is_spot else "Perp"

    @property
    def ws_url(self) -> str:
        """Get WebSocket URL for book ticker stream."""
        symbol_lower = self.base_symbol.lower()
        if self.is_spot:
            return f"{BINANCE_SPOT_WS}/{symbol_lower}@bookTicker"
        return f"{BINANCE_PERP_WS}/{symbol_lower}@bookTicker"

    def _parse_book_ticker(self, message: str) -> Optional[dict]:
        """Parse book ticker message."""
        try:
            data = json.loads(message)
            # Binance book ticker format:
            # {"u":update_id,"s":"BTCUSDT","b":"bid_price","B":"bid_qty",
            #  "a":"ask_price","A":"ask_qty"}
            return {
                "bid": float(data["b"]),
                "ask": float(data["a"]),
                "timestamp_ms": int(time.time() * 1000),
            }
        except (KeyError, ValueError, json.JSONDecodeError):
            return None

    def _calculate_spread_bps(self, bid: float, ask: float) -> float:
        """Calculate spread in basis points."""
        if bid <= 0:
            return 0.0
        mid = (bid + ask) / 2
        spread = ask - bid
        return (spread / mid) * 10000  # Convert to basis points

    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        self._ws = await websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
        )

    @property
    def is_warmed_up(self) -> bool:
        """Check if warmup period has elapsed."""
        if self._is_warmed_up:
            return True
        if self._start_time is None:
            return False
        elapsed = time.time() - self._start_time
        if elapsed >= self.warmup_seconds:
            self._is_warmed_up = True
            return True
        return False

    @property
    def warmup_remaining_sec(self) -> float:
        """Seconds remaining in warmup period."""
        if self._is_warmed_up or self._start_time is None:
            return 0.0
        elapsed = time.time() - self._start_time
        return max(0.0, self.warmup_seconds - elapsed)

    async def _listen(self) -> None:
        """Listen for book ticker updates."""
        # Start warmup timer on first listen
        if self._start_time is None:
            self._start_time = time.time()

        async for message in self._ws:
            if not self._running:
                break

            data = self._parse_book_ticker(message)
            if not data:
                continue

            self.updates_received += 1
            bid, ask = data["bid"], data["ask"]
            spread_bps = self._calculate_spread_bps(bid, ask)

            self.last_spread_bps = spread_bps
            self.last_bid = bid
            self.last_ask = ask

            # Update statistics (always update, even during warmup)
            self.stats.update(spread_bps)

            # Check for anomaly only after time-based warmup complete
            if self.is_warmed_up:
                z = self.stats.z_score(spread_bps)

                if abs(z) >= self.z_threshold:
                    # Check cooldown
                    now = time.time()
                    if now - self.last_anomaly_time >= self.cooldown_sec:
                        self.last_anomaly_time = now
                        self.anomalies_detected += 1

                        anomaly = SpreadAnomaly(
                            symbol=self.symbol,
                            timestamp_ms=data["timestamp_ms"],
                            spread_bps=spread_bps,
                            mean_bps=self.stats.mean,
                            std_bps=self.stats.std,
                            z_score=z,
                            multiplier=self.stats.multiplier(spread_bps),
                            bid=bid,
                            ask=ask,
                            mid=(bid + ask) / 2,
                        )

                        if self.on_anomaly:
                            self.on_anomaly(anomaly)

    async def start(self) -> None:
        """Start monitoring spread."""
        if self._running:
            return

        self._running = True
        print(f"Connecting to Binance {self.market_type} book ticker for {self.symbol}...")
        print(f"  Spread warmup period: {self.warmup_seconds:.0f}s (collecting baseline data)")

        while self._running:
            try:
                await self._connect()
                print(f"Connected! Monitoring spread (z-threshold: {self.z_threshold})...")
                await self._listen()
            except websockets.ConnectionClosed as e:
                if self._running:
                    print(f"Spread monitor connection closed: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)
            except Exception as e:
                if self._running:
                    print(f"Spread monitor error: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        if self._ws:
            await self._ws.close()

    def get_stats(self) -> dict:
        """Get current spread statistics."""
        return {
            "symbol": self.symbol,
            "market_type": self.market_type,
            "updates_received": self.updates_received,
            "anomalies_detected": self.anomalies_detected,
            "current_spread_bps": self.last_spread_bps,
            "mean_spread_bps": self.stats.mean,
            "std_spread_bps": self.stats.std,
            "is_warmed_up": self.is_warmed_up,
            "warmup_remaining_sec": self.warmup_remaining_sec,
            "samples": self.stats.count,
        }


class MultiSpreadMonitor:
    """
    Monitors spread for multiple symbols concurrently.
    """

    def __init__(
        self,
        symbols: List[str],
        z_threshold: float = 3.0,
        alpha: float = 0.01,
        warmup_seconds: float = 300.0,
        cooldown_sec: float = 60.0,
        on_anomaly: Optional[Callable[[SpreadAnomaly], None]] = None,
    ):
        self.symbols = symbols
        self.on_anomaly = on_anomaly
        self.monitors: dict[str, SpreadMonitor] = {}

        for symbol in symbols:
            self.monitors[symbol] = SpreadMonitor(
                symbol=symbol,
                z_threshold=z_threshold,
                alpha=alpha,
                warmup_seconds=warmup_seconds,
                cooldown_sec=cooldown_sec,
                on_anomaly=on_anomaly,
            )

    async def start(self) -> None:
        """Start all monitors."""
        tasks = [monitor.start() for monitor in self.monitors.values()]
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        """Stop all monitors."""
        for monitor in self.monitors.values():
            await monitor.stop()

    def add_symbol(self, symbol: str) -> SpreadMonitor:
        """Add a new symbol to monitor."""
        if symbol not in self.monitors:
            monitor = SpreadMonitor(
                symbol=symbol,
                z_threshold=self.monitors[list(self.monitors.keys())[0]].z_threshold if self.monitors else 3.0,
                on_anomaly=self.on_anomaly,
            )
            self.monitors[symbol] = monitor
        return self.monitors[symbol]

    def remove_symbol(self, symbol: str) -> bool:
        """Remove a symbol from monitoring."""
        if symbol in self.monitors:
            del self.monitors[symbol]
            return True
        return False

    def get_all_stats(self) -> dict:
        """Get stats for all monitors."""
        return {symbol: monitor.get_stats() for symbol, monitor in self.monitors.items()}


async def main():
    """Test the spread monitor."""
    def on_anomaly(anomaly: SpreadAnomaly):
        print(f"\n⚠️  SPREAD ANOMALY: {anomaly.symbol}")
        print(f"    Spread: {anomaly.spread_bps:.2f} bps ({anomaly.multiplier:.1f}x normal)")
        print(f"    Mean: {anomaly.mean_bps:.2f} bps, Std: {anomaly.std_bps:.2f} bps")
        print(f"    Z-score: {anomaly.z_score:.2f} ({anomaly.severity})")
        print(f"    Bid: {anomaly.bid:.2f}, Ask: {anomaly.ask:.2f}")

    monitor = SpreadMonitor(
        symbol="BTCUSDT",
        z_threshold=3.0,
        on_anomaly=on_anomaly,
    )

    try:
        await monitor.start()
    except KeyboardInterrupt:
        print("\nStopping...")
        await monitor.stop()

        stats = monitor.get_stats()
        print(f"\nStats: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
