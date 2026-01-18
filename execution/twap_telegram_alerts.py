#!/usr/bin/env python3
"""
TWAP Telegram Alerts

Multi-ticker TWAP detection on Binance (Perpetual & Spot) with Telegram alerts.

Features:
- Monitors multiple tickers simultaneously (Perp: BTCUSDT, Spot: BTCUSDT.S)
- Sends alerts to a Telegram channel
- Admin commands via DM (authorized user only)
- Per-ticker USD thresholds for filtering
- Per-ticker TWAP naming (e.g., BTC-B1, ETH-S2)
- Low confidence TWAP confirmation tracking
- Spread anomaly detection

Setup:
1. Create bot via @BotFather -> get TELEGRAM_BOT_TOKEN
2. Create channel -> add bot as admin -> get TELEGRAM_CHANNEL_ID
3. Get your user ID via @userinfobot -> set as TELEGRAM_ADMIN_ID
4. Configure in config.json

Usage:
    python twap_telegram_alerts.py
"""

import asyncio
import json
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

# Add execution directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from twap_data_collector import TradeCollector, Trade, get_available_pairs, parse_symbol
from twap_fourier_analyzer import TWAPAnalyzer
from twap_classifier import TWAPClassifier, SizeCategory
from spread_monitor import SpreadMonitor, SpreadAnomaly

# Major tickers with higher thresholds (include both perp and spot)
MAJOR_TICKERS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BTCUSDT.S", "ETHUSDT.S", "SOLUSDT.S"]


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class TickerConfig:
    """Configuration for a single ticker to monitor."""
    symbol: str
    enabled: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TickerConfig":
        return cls(
            symbol=data["symbol"],
            enabled=data.get("enabled", True)
        )


@dataclass
class Config:
    """Main configuration."""
    telegram_bot_token: str
    telegram_channel_id: str
    telegram_admin_id: int
    tickers: List[TickerConfig]
    analysis_interval_sec: int = 30
    min_buffer_sec: int = 120
    buffer_minutes: int = 30
    min_confidence: str = "LOW"
    min_value_major: int = 80000  # USD threshold for BTC/ETH/SOL
    min_value_other: int = 40000  # USD threshold for other tickers
    alert_on_updates: bool = False
    # Spread monitoring settings
    spread_monitoring: bool = True
    spread_z_threshold: float = 6.0  # Z-score threshold for spread anomalies (higher = less sensitive)
    spread_cooldown_sec: float = 300.0  # Minimum seconds between spread alerts (5 min)
    # Low confidence confirmation tracking
    track_low_confidence: bool = True
    confirmation_checks: int = 3  # Number of confirmations needed

    def to_dict(self) -> dict:
        return {
            "telegram_bot_token": self.telegram_bot_token,
            "telegram_channel_id": self.telegram_channel_id,
            "telegram_admin_id": self.telegram_admin_id,
            "tickers": [t.to_dict() for t in self.tickers],
            "analysis_interval_sec": self.analysis_interval_sec,
            "min_buffer_sec": self.min_buffer_sec,
            "buffer_minutes": self.buffer_minutes,
            "min_confidence": self.min_confidence,
            "min_value_major": self.min_value_major,
            "min_value_other": self.min_value_other,
            "alert_on_updates": self.alert_on_updates,
            "spread_monitoring": self.spread_monitoring,
            "spread_z_threshold": self.spread_z_threshold,
            "spread_cooldown_sec": self.spread_cooldown_sec,
            "track_low_confidence": self.track_low_confidence,
            "confirmation_checks": self.confirmation_checks,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        tickers = [TickerConfig.from_dict(t) for t in data.get("tickers", [])]
        return cls(
            telegram_bot_token=data["telegram_bot_token"],
            telegram_channel_id=data["telegram_channel_id"],
            telegram_admin_id=data["telegram_admin_id"],
            tickers=tickers,
            analysis_interval_sec=data.get("analysis_interval_sec", 30),
            min_buffer_sec=data.get("min_buffer_sec", 120),
            buffer_minutes=data.get("buffer_minutes", 30),
            min_confidence=data.get("min_confidence", "LOW"),
            min_value_major=data.get("min_value_major", 80000),
            min_value_other=data.get("min_value_other", 40000),
            alert_on_updates=data.get("alert_on_updates", False),
            spread_monitoring=data.get("spread_monitoring", True),
            spread_z_threshold=data.get("spread_z_threshold", 6.0),
            spread_cooldown_sec=data.get("spread_cooldown_sec", 300.0),
            track_low_confidence=data.get("track_low_confidence", True),
            confirmation_checks=data.get("confirmation_checks", 3),
        )

    def save(self, path: str = "config.json") -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str = "config.json") -> "Config":
        with open(path, "r") as f:
            return cls.from_dict(json.load(f))

    def get_min_value_for_ticker(self, symbol: str) -> int:
        """Get the minimum USD value threshold for a specific ticker."""
        # Handle both perp (BTCUSDT) and spot (BTCUSDT.S)
        base_symbol, _ = parse_symbol(symbol)
        if symbol.upper() in MAJOR_TICKERS or base_symbol.upper() in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            return self.min_value_major
        return self.min_value_other


# =============================================================================
# Telegram API
# =============================================================================

class TelegramBot:
    """Simple Telegram bot using HTTP API."""

    BASE_URL = "https://api.telegram.org/bot{token}/{method}"

    def __init__(self, token: str, channel_id: str, admin_id: int):
        self.token = token
        self.channel_id = channel_id
        self.admin_id = admin_id
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_update_id = 0

    async def start(self) -> None:
        self.session = aiohttp.ClientSession()
        # Clear any pending updates to avoid processing old commands
        await self._clear_pending_updates()

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    async def _clear_pending_updates(self) -> None:
        """Clear pending updates to avoid processing old commands on restart."""
        try:
            result = await self._call("getUpdates", offset=-1, timeout=1)
            updates = result.get("result", [])
            if updates:
                self.last_update_id = updates[-1]["update_id"]
        except Exception:
            pass

    async def _call(self, method: str, **params) -> dict:
        """Call Telegram API method."""
        url = self.BASE_URL.format(token=self.token, method=method)
        try:
            async with self.session.post(url, json=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                return await resp.json()
        except Exception as e:
            print(f"Telegram API error: {e}")
            return {"ok": False, "error": str(e)}

    async def send_channel_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send message to the alerts channel."""
        result = await self._call(
            "sendMessage",
            chat_id=self.channel_id,
            text=text,
            parse_mode=parse_mode,
        )
        return result.get("ok", False)

    async def send_admin_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send message to admin via DM."""
        result = await self._call(
            "sendMessage",
            chat_id=self.admin_id,
            text=text,
            parse_mode=parse_mode,
        )
        return result.get("ok", False)

    async def get_updates(self, timeout: int = 30) -> List[dict]:
        """Get new messages (long polling)."""
        result = await self._call(
            "getUpdates",
            offset=self.last_update_id + 1,
            timeout=timeout,
        )
        updates = result.get("result", [])
        if updates:
            self.last_update_id = updates[-1]["update_id"]
        return updates

    def is_admin(self, user_id: int) -> bool:
        """Check if user is the admin."""
        return user_id == self.admin_id


# =============================================================================
# TWAP Tracking with Per-Ticker Naming
# =============================================================================

@dataclass
class TrackedTWAP:
    """Tracked TWAP with persistent name."""
    name: str
    ticker: str
    side: str
    frequency_hz: float
    first_seen: datetime
    last_seen: datetime
    detection_count: int = 1
    best_confidence: str = "LOW"

    @property
    def interval_seconds(self) -> float:
        return 1.0 / self.frequency_hz if self.frequency_hz > 0 else 0

    def matches(self, side: str, frequency_hz: float, tolerance: float = 0.15) -> bool:
        if side != self.side:
            return False
        freq_diff = abs(frequency_hz - self.frequency_hz) / self.frequency_hz
        return freq_diff <= tolerance


@dataclass
class PendingTWAP:
    """Low confidence TWAP being tracked for confirmation."""
    ticker: str
    side: str
    frequency_hz: float
    first_seen: datetime
    last_seen: datetime
    checks: int = 1
    best_snr: float = 0.0
    best_confidence: str = "LOW"
    last_classified: object = None  # Store last classification for alert

    @property
    def interval_seconds(self) -> float:
        return 1.0 / self.frequency_hz if self.frequency_hz > 0 else 0

    def matches(self, side: str, frequency_hz: float, tolerance: float = 0.15) -> bool:
        if side != self.side:
            return False
        freq_diff = abs(frequency_hz - self.frequency_hz) / self.frequency_hz
        return freq_diff <= tolerance


class TWAPTracker:
    """Tracks TWAPs with per-ticker naming convention."""

    def __init__(self, confirmation_checks: int = 3):
        # Per-ticker tracking: {ticker: {side: [TWAPs]}}
        self.tracked: Dict[str, Dict[str, List[TrackedTWAP]]] = {}
        # Per-ticker counters: {ticker: {side: count}}
        self.counters: Dict[str, Dict[str, int]] = {}
        # Pending low-confidence TWAPs awaiting confirmation
        self.pending: Dict[str, Dict[str, List[PendingTWAP]]] = {}
        self.confirmation_checks = confirmation_checks

    def _get_ticker_code(self, ticker: str) -> str:
        """Get 3-4 char ticker code. E.g., BTCUSDT -> BTC, ETHUSDT -> ETH"""
        # Remove common suffixes
        for suffix in ["USDT", "USD", "BUSD", "USDC"]:
            if ticker.endswith(suffix):
                return ticker[:-len(suffix)][:4]
        return ticker[:4]

    def _get_side_code(self, side: str) -> str:
        """Get 1 char side code. B=Buy, S=Sell"""
        return "B" if side.lower() == "buy" else "S"

    def _generate_name(self, ticker: str, side: str) -> str:
        """Generate name like BTC-B1, ETH-S2"""
        ticker_code = self._get_ticker_code(ticker)
        side_code = self._get_side_code(side)

        # Initialize counters if needed
        if ticker not in self.counters:
            self.counters[ticker] = {"buy": 0, "sell": 0}

        # Increment counter
        self.counters[ticker][side.lower()] += 1
        count = self.counters[ticker][side.lower()]

        return f"{ticker_code}-{side_code}{count}"

    def get_or_create(self, ticker: str, side: str, frequency_hz: float) -> tuple:
        """Get existing or create new tracked TWAP. Returns (TrackedTWAP, is_new)."""
        now = datetime.now()
        side_lower = side.lower()

        # Initialize tracking structures
        if ticker not in self.tracked:
            self.tracked[ticker] = {"buy": [], "sell": []}

        # Check for existing match
        for twap in self.tracked[ticker][side_lower]:
            if twap.matches(side, frequency_hz):
                twap.last_seen = now
                twap.detection_count += 1
                return twap, False

        # Create new TWAP with per-ticker name
        name = self._generate_name(ticker, side)

        new_twap = TrackedTWAP(
            name=name,
            ticker=ticker,
            side=side,
            frequency_hz=frequency_hz,
            first_seen=now,
            last_seen=now,
        )
        self.tracked[ticker][side_lower].append(new_twap)
        return new_twap, True

    def get_all_active(self) -> List[TrackedTWAP]:
        """Get all active TWAPs across all tickers."""
        result = []
        for ticker_twaps in self.tracked.values():
            for side_twaps in ticker_twaps.values():
                result.extend(side_twaps)
        return result

    def get_ticker_active(self, ticker: str) -> List[TrackedTWAP]:
        """Get active TWAPs for a specific ticker."""
        if ticker not in self.tracked:
            return []
        result = []
        for side_twaps in self.tracked[ticker].values():
            result.extend(side_twaps)
        return result

    def clear_ticker(self, ticker: str) -> None:
        """Clear tracked TWAPs for a ticker."""
        self.tracked[ticker] = {"buy": [], "sell": []}
        self.counters[ticker] = {"buy": 0, "sell": 0}
        self.pending[ticker] = {"buy": [], "sell": []}

    def track_pending(self, ticker: str, side: str, frequency_hz: float, snr: float,
                      confidence: str, classified) -> tuple:
        """
        Track a pending low-confidence TWAP for confirmation.
        Returns (pending_twap, is_confirmed, should_alert) tuple.
        - is_confirmed: True if this check confirms the TWAP
        - should_alert: True if confidence improved and should alert
        """
        now = datetime.now()
        side_lower = side.lower()

        # Initialize if needed
        if ticker not in self.pending:
            self.pending[ticker] = {"buy": [], "sell": []}

        # Check for existing pending match
        for pending in self.pending[ticker][side_lower]:
            if pending.matches(side, frequency_hz):
                pending.last_seen = now
                pending.checks += 1
                pending.last_classified = classified

                # Track best SNR/confidence seen
                old_confidence = pending.best_confidence
                confidence_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
                if snr > pending.best_snr:
                    pending.best_snr = snr
                if confidence_order.get(confidence.upper(), 0) > confidence_order.get(pending.best_confidence, 0):
                    pending.best_confidence = confidence

                # Check if confirmed (enough checks or confidence improved to MEDIUM+)
                is_confirmed = pending.checks >= self.confirmation_checks
                should_alert = (pending.best_confidence in ["MEDIUM", "HIGH"] and
                               old_confidence == "LOW")

                if is_confirmed or should_alert:
                    # Remove from pending
                    self.pending[ticker][side_lower].remove(pending)

                return pending, is_confirmed, should_alert

        # Create new pending TWAP
        new_pending = PendingTWAP(
            ticker=ticker,
            side=side,
            frequency_hz=frequency_hz,
            first_seen=now,
            last_seen=now,
            checks=1,
            best_snr=snr,
            best_confidence=confidence,
            last_classified=classified,
        )
        self.pending[ticker][side_lower].append(new_pending)
        return new_pending, False, False

    def get_pending_count(self) -> int:
        """Get total number of pending TWAPs across all tickers."""
        count = 0
        for ticker_pending in self.pending.values():
            for side_pending in ticker_pending.values():
                count += len(side_pending)
        return count


# =============================================================================
# Ticker Monitor
# =============================================================================

class TickerMonitor:
    """Monitors a single ticker for TWAPs."""

    def __init__(
        self,
        symbol: str,
        buffer_minutes: int,
        analysis_interval_sec: int,
        min_buffer_sec: int,
    ):
        self.symbol = symbol
        self.buffer_minutes = buffer_minutes
        self.analysis_interval_sec = analysis_interval_sec
        self.min_buffer_sec = min_buffer_sec

        self.collector: Optional[TradeCollector] = None
        self.classifier = TWAPClassifier()
        self.running = False
        self.last_analysis = datetime.now()

    async def start(self) -> None:
        """Start the collector."""
        self.collector = TradeCollector(
            symbol=self.symbol,
            buffer_minutes=self.buffer_minutes,
        )
        self.running = True
        await self.collector.start()

    async def stop(self) -> None:
        """Stop the collector."""
        self.running = False
        if self.collector:
            await self.collector.stop()

    def should_analyze(self) -> bool:
        """Check if it's time to run analysis."""
        if not self.collector:
            return False

        stats = self.collector.get_stats()
        buffer_sec = stats.get("buffer_duration_sec", 0)

        if buffer_sec < self.min_buffer_sec:
            return False

        elapsed = (datetime.now() - self.last_analysis).total_seconds()
        return elapsed >= self.analysis_interval_sec

    def analyze(self) -> List[tuple]:
        """Run analysis and return detections. Returns list of (detection, classified)."""
        if not self.collector:
            return []

        self.last_analysis = datetime.now()
        trades = self.collector.get_trades()

        if not trades:
            return []

        analyzer = TWAPAnalyzer(trades, bucket_size_ms=1000)
        detections = analyzer.detect_twaps()

        results = []
        for det in detections:
            classified = self.classifier.classify(det)
            results.append((det, classified))

        return results

    def get_stats(self) -> dict:
        """Get collector stats."""
        if self.collector:
            return self.collector.get_stats()
        return {}


# =============================================================================
# Alert Formatter
# =============================================================================

class AlertFormatter:
    """Formats TWAP alerts for Telegram."""

    @staticmethod
    def _get_market_tag(ticker: str) -> str:
        """Get market type tag for ticker."""
        if ticker.endswith(".S"):
            return " [SPOT]"
        return ""

    @staticmethod
    def format_new_twap(ticker: str, tracked: TrackedTWAP, classified) -> str:
        """Format a new TWAP detection alert."""
        d = classified.detection
        market_tag = AlertFormatter._get_market_tag(ticker)

        return (
            f"🎯 <b>NEW TWAP: {tracked.name}</b>{market_tag}\n"
            f"\n"
            f"<b>Ticker:</b> {ticker}\n"
            f"<b>Side:</b> {d.side.upper()}\n"
            f"<b>Category:</b> {classified.size_category.value} ({classified.urgency_category.value})\n"
            f"<b>Interval:</b> {d.interval_seconds:.1f}s\n"
            f"<b>Per-exec:</b> {d.estimated_per_execution_size:.6f} (~${d.estimated_per_execution_value:,.0f})\n"
            f"<b>Est. Total:</b> ~${d.estimated_total_value:,.0f}\n"
            f"<b>Confidence:</b> {classified.confidence_level.value} (SNR: {d.snr:.1f})\n"
            f"<b>Risk:</b> {classified.risk_score:.0f}/100\n"
            f"\n"
            f"<i>{classified.description}</i>"
        )

    @staticmethod
    def format_confirmed_twap(ticker: str, tracked: TrackedTWAP, pending: PendingTWAP, classified) -> str:
        """Format a confirmed TWAP alert (was pending, now confirmed)."""
        d = classified.detection
        market_tag = AlertFormatter._get_market_tag(ticker)
        duration = (pending.last_seen - pending.first_seen).total_seconds()
        duration_str = f"{duration:.0f}s" if duration < 60 else f"{duration/60:.1f}min"

        return (
            f"✅ <b>CONFIRMED TWAP: {tracked.name}</b>{market_tag}\n"
            f"\n"
            f"<b>Ticker:</b> {ticker}\n"
            f"<b>Side:</b> {d.side.upper()}\n"
            f"<b>Tracked for:</b> {duration_str} ({pending.checks} checks)\n"
            f"<b>Confidence:</b> {pending.best_confidence} (SNR: {pending.best_snr:.1f})\n"
            f"<b>Interval:</b> {d.interval_seconds:.1f}s\n"
            f"<b>Est. Total:</b> ~${d.estimated_total_value:,.0f}\n"
            f"\n"
            f"<i>Low confidence signal confirmed through persistence</i>"
        )

    @staticmethod
    def format_spread_anomaly(anomaly: SpreadAnomaly) -> str:
        """Format a spread anomaly alert."""
        direction = "WIDENED" if anomaly.is_wide else "NARROWED"
        market_tag = " [SPOT]" if anomaly.symbol.endswith(".S") else ""

        # Format multiplier for context (e.g., "3.2x normal")
        multiplier_str = f"{anomaly.multiplier:.1f}x normal" if anomaly.multiplier >= 1.5 else f"{anomaly.multiplier:.2f}x"

        return (
            f"📊 <b>SPREAD {direction}: {anomaly.symbol}</b>{market_tag}\n"
            f"\n"
            f"<b>Current:</b> {anomaly.spread_bps:.2f} bps ({multiplier_str})\n"
            f"<b>Normal:</b> {anomaly.mean_bps:.2f} ± {anomaly.std_bps:.2f} bps\n"
            f"<b>Z-score:</b> {anomaly.z_score:.1f} ({anomaly.severity})\n"
            f"<b>Bid/Ask:</b> {anomaly.bid:.2f} / {anomaly.ask:.2f}\n"
            f"\n"
            f"<i>Spread is {multiplier_str} - may indicate liquidity change or large order</i>"
        )

    @staticmethod
    def format_twap_update(ticker: str, tracked: TrackedTWAP, classified) -> str:
        """Format a TWAP update alert."""
        d = classified.detection
        market_tag = AlertFormatter._get_market_tag(ticker)
        duration = (tracked.last_seen - tracked.first_seen).total_seconds()
        duration_str = f"{duration:.0f}s" if duration < 60 else f"{duration/60:.1f}min"

        return (
            f"🔄 <b>UPDATE: {tracked.name}</b>{market_tag}\n"
            f"\n"
            f"<b>Ticker:</b> {ticker}\n"
            f"<b>Side:</b> {d.side.upper()}\n"
            f"<b>Seen:</b> {tracked.detection_count}x over {duration_str}\n"
            f"<b>Est. Total:</b> ~${d.estimated_total_value:,.0f}\n"
            f"<b>Confidence:</b> {classified.confidence_level.value}"
        )

    @staticmethod
    def format_status(monitors: Dict[str, TickerMonitor], tracker: TWAPTracker, paused: bool,
                      spread_monitors: Dict[str, SpreadMonitor] = None) -> str:
        """Format status message."""
        lines = []

        if paused:
            lines.append("⏸️ <b>STATUS: PAUSED</b>\n")
        else:
            lines.append("📊 <b>TWAP Monitor Status</b>\n")

        # Count perp and spot
        perp_count = sum(1 for s in monitors if not s.endswith(".S"))
        spot_count = sum(1 for s in monitors if s.endswith(".S"))

        lines.append(f"<b>Monitoring {len(monitors)} tickers:</b> ({perp_count} perp, {spot_count} spot)")
        for symbol, monitor in monitors.items():
            stats = monitor.get_stats()
            trades = stats.get("trades_in_buffer", 0)
            buffer = stats.get("buffer_duration_sec", 0)
            status = "🟢" if monitor.running and buffer > 60 else "🟡"
            market = "[S]" if symbol.endswith(".S") else "[P]"
            lines.append(f"  {status} {market} {symbol}: {trades} trades, {buffer:.0f}s buffer")

        active = tracker.get_all_active()
        pending_count = tracker.get_pending_count()
        if active:
            lines.append(f"\n<b>Active TWAPs ({len(active)}):</b>")
            for twap in active:
                duration = (twap.last_seen - twap.first_seen).total_seconds()
                duration_str = f"{duration:.0f}s" if duration < 60 else f"{duration/60:.1f}min"
                lines.append(f"  • {twap.name}: {twap.interval_seconds:.1f}s interval, {duration_str}")
        else:
            lines.append("\n<i>No active TWAPs detected</i>")

        if pending_count > 0:
            lines.append(f"\n<i>{pending_count} low-confidence TWAPs being tracked</i>")

        if spread_monitors:
            lines.append(f"\n<b>Spread Monitors:</b> {len(spread_monitors)} active")

        return "\n".join(lines)


# =============================================================================
# Main Application
# =============================================================================

class TWAPAlertService:
    """Main service that coordinates everything."""

    def __init__(self, config: Config):
        self.config = config
        self.bot = TelegramBot(
            config.telegram_bot_token,
            config.telegram_channel_id,
            config.telegram_admin_id,
        )
        self.tracker = TWAPTracker(confirmation_checks=config.confirmation_checks)
        self.monitors: Dict[str, TickerMonitor] = {}
        self.spread_monitors: Dict[str, SpreadMonitor] = {}
        self.formatter = AlertFormatter()
        self.running = False
        self.paused = False

    def _confidence_meets_threshold(self, confidence: str) -> bool:
        """Check if confidence meets minimum threshold."""
        levels = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
        return levels.get(confidence.upper(), 0) >= levels.get(self.config.min_confidence.upper(), 0)

    def _value_meets_threshold(self, symbol: str, value_usd: float) -> bool:
        """Check if value meets minimum threshold for this ticker."""
        min_value = self.config.get_min_value_for_ticker(symbol)
        return value_usd >= min_value

    async def start(self) -> None:
        """Start the service."""
        await self.bot.start()
        self.running = True

        for ticker_config in self.config.tickers:
            if ticker_config.enabled:
                await self._add_monitor(ticker_config.symbol)

        # Count perp and spot
        perp_list = [t.symbol for t in self.config.tickers if t.enabled and not t.symbol.endswith(".S")]
        spot_list = [t.symbol for t in self.config.tickers if t.enabled and t.symbol.endswith(".S")]

        tickers_summary = f"{len(perp_list)} perp, {len(spot_list)} spot"
        spread_status = "enabled" if self.config.spread_monitoring else "disabled"
        confirm_status = f"tracking ({self.config.confirmation_checks} checks)" if self.config.track_low_confidence else "disabled"

        # Only send startup message to admin DM, not channel
        await self.bot.send_admin_message(
            f"🟢 <b>TWAP Alert Service started!</b>\n\n"
            f"<b>Tickers:</b> {tickers_summary}\n"
            f"<b>Major threshold:</b> ${self.config.min_value_major:,}\n"
            f"<b>Other threshold:</b> ${self.config.min_value_other:,}\n"
            f"<b>Min confidence:</b> {self.config.min_confidence}\n"
            f"<b>Spread monitoring:</b> {spread_status}\n"
            f"<b>Low-conf TWAPs:</b> {confirm_status}\n\n"
            f"Send /help for commands"
        )

        await asyncio.gather(
            self._analysis_loop(),
            self._command_loop(),
        )

    async def stop(self) -> None:
        """Stop the service."""
        self.running = False
        for monitor in self.monitors.values():
            await monitor.stop()
        for spread_monitor in self.spread_monitors.values():
            await spread_monitor.stop()
        await self.bot.stop()

    async def _add_monitor(self, symbol: str) -> None:
        """Add a ticker monitor and optionally a spread monitor."""
        monitor = TickerMonitor(
            symbol=symbol,
            buffer_minutes=self.config.buffer_minutes,
            analysis_interval_sec=self.config.analysis_interval_sec,
            min_buffer_sec=self.config.min_buffer_sec,
        )
        self.monitors[symbol] = monitor
        asyncio.create_task(monitor.start())

        # Add spread monitor if enabled
        if self.config.spread_monitoring:
            spread_monitor = SpreadMonitor(
                symbol=symbol,
                z_threshold=self.config.spread_z_threshold,
                cooldown_sec=self.config.spread_cooldown_sec,
                on_anomaly=lambda a: asyncio.create_task(self._handle_spread_anomaly(a)),
            )
            self.spread_monitors[symbol] = spread_monitor
            asyncio.create_task(spread_monitor.start())

    async def _handle_spread_anomaly(self, anomaly: SpreadAnomaly) -> None:
        """Handle a detected spread anomaly."""
        if self.paused:
            return
        msg = self.formatter.format_spread_anomaly(anomaly)
        await self.bot.send_channel_message(msg)

    async def _remove_monitor(self, symbol: str) -> bool:
        """Remove a ticker monitor and spread monitor."""
        removed = False
        if symbol in self.monitors:
            await self.monitors[symbol].stop()
            del self.monitors[symbol]
            self.tracker.clear_ticker(symbol)
            removed = True
        if symbol in self.spread_monitors:
            await self.spread_monitors[symbol].stop()
            del self.spread_monitors[symbol]
        return removed

    async def _analysis_loop(self) -> None:
        """Main analysis loop."""
        while self.running:
            await asyncio.sleep(5)

            if self.paused:
                continue

            for symbol, monitor in list(self.monitors.items()):
                if not monitor.should_analyze():
                    continue

                try:
                    results = monitor.analyze()

                    for detection, classified in results:
                        confidence = classified.confidence_level.value
                        snr = detection.snr

                        # Skip if doesn't meet USD value threshold for this ticker
                        if not self._value_meets_threshold(symbol, classified.detection.estimated_total_value):
                            continue

                        # Check if confidence meets threshold
                        if self._confidence_meets_threshold(confidence):
                            # Meets threshold - track normally
                            tracked, is_new = self.tracker.get_or_create(
                                symbol,
                                detection.side,
                                detection.frequency_hz,
                            )

                            if is_new:
                                msg = self.formatter.format_new_twap(symbol, tracked, classified)
                                await self.bot.send_channel_message(msg)
                            elif self.config.alert_on_updates:
                                msg = self.formatter.format_twap_update(symbol, tracked, classified)
                                await self.bot.send_channel_message(msg)

                        elif self.config.track_low_confidence:
                            # Low confidence - track for confirmation
                            pending, is_confirmed, should_alert = self.tracker.track_pending(
                                symbol,
                                detection.side,
                                detection.frequency_hz,
                                snr,
                                confidence,
                                classified,
                            )

                            if is_confirmed or should_alert:
                                # Promote to tracked TWAP
                                tracked, _ = self.tracker.get_or_create(
                                    symbol,
                                    detection.side,
                                    detection.frequency_hz,
                                )
                                msg = self.formatter.format_confirmed_twap(
                                    symbol, tracked, pending, classified
                                )
                                await self.bot.send_channel_message(msg)

                except Exception as e:
                    print(f"Analysis error for {symbol}: {e}")

    async def _command_loop(self) -> None:
        """Listen for admin commands."""
        while self.running:
            try:
                updates = await self.bot.get_updates(timeout=10)

                for update in updates:
                    message = update.get("message", {})
                    user_id = message.get("from", {}).get("id")
                    text = message.get("text", "")

                    if not user_id or not text:
                        continue

                    if not self.bot.is_admin(user_id):
                        continue

                    await self._handle_command(text)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Command loop error: {e}")
                await asyncio.sleep(5)

    async def _handle_command(self, text: str) -> None:
        """Handle admin command."""
        parts = text.strip().split()
        if not parts:
            return

        cmd = parts[0].lower()

        if cmd == "/help":
            help_text = (
                "📖 <b>Admin Commands</b>\n\n"
                "<b>Monitoring:</b>\n"
                "/status - Show monitoring status\n"
                "/pause - Pause monitoring\n"
                "/resume - Resume monitoring\n"
                "\n<b>Tickers:</b>\n"
                "/list - List monitored tickers\n"
                "/add SYMBOL - Add ticker\n"
                "/remove SYMBOL - Remove ticker\n"
                "\n<b>Thresholds:</b>\n"
                "/config - Show current config\n"
                "/setmajor VALUE - Set BTC/ETH/SOL min USD (e.g., /setmajor 80000)\n"
                "/setother VALUE - Set other tickers min USD (e.g., /setother 40000)\n"
                "/setconf LEVEL - Set min confidence (LOW/MEDIUM/HIGH)\n"
                f"\n<b>Major tickers:</b> {', '.join(MAJOR_TICKERS)}"
            )
            await self.bot.send_admin_message(help_text)

        elif cmd == "/status":
            msg = self.formatter.format_status(self.monitors, self.tracker, self.paused, self.spread_monitors)
            await self.bot.send_admin_message(msg)

        elif cmd == "/list":
            if not self.monitors:
                await self.bot.send_admin_message("No tickers being monitored.")
            else:
                lines = ["<b>Monitored Tickers:</b>"]
                for symbol in self.monitors:
                    twaps = self.tracker.get_ticker_active(symbol)
                    min_val = self.config.get_min_value_for_ticker(symbol)
                    twap_str = f" ({len(twaps)} active)" if twaps else ""
                    lines.append(f"• {symbol} [>${min_val/1000:.0f}k]{twap_str}")
                await self.bot.send_admin_message("\n".join(lines))

        elif cmd == "/add" and len(parts) >= 2:
            symbol = parts[1].upper()
            if symbol in self.monitors:
                await self.bot.send_admin_message(f"❌ {symbol} already being monitored")
            else:
                await self._add_monitor(symbol)
                self.config.tickers.append(TickerConfig(symbol=symbol))
                self.config.save()
                min_val = self.config.get_min_value_for_ticker(symbol)
                await self.bot.send_admin_message(f"✅ Added {symbol} (min: ${min_val:,})")

        elif cmd == "/remove" and len(parts) >= 2:
            symbol = parts[1].upper()
            if await self._remove_monitor(symbol):
                self.config.tickers = [t for t in self.config.tickers if t.symbol != symbol]
                self.config.save()
                await self.bot.send_admin_message(f"✅ Removed {symbol}")
            else:
                await self.bot.send_admin_message(f"❌ Ticker {symbol} not found")

        elif cmd == "/pause":
            self.paused = True
            await self.bot.send_admin_message("⏸️ Monitoring paused. Send /resume to continue.")

        elif cmd == "/resume":
            self.paused = False
            await self.bot.send_admin_message("▶️ Monitoring resumed")

        elif cmd == "/config":
            msg = (
                f"<b>Configuration:</b>\n"
                f"Analysis interval: {self.config.analysis_interval_sec}s\n"
                f"Buffer size: {self.config.buffer_minutes}min\n"
                f"Min confidence: {self.config.min_confidence}\n"
                f"Major threshold (BTC/ETH/SOL): ${self.config.min_value_major:,}\n"
                f"Other threshold: ${self.config.min_value_other:,}\n"
                f"Alert on updates: {self.config.alert_on_updates}"
            )
            await self.bot.send_admin_message(msg)

        elif cmd == "/setmajor" and len(parts) >= 2:
            try:
                value = int(parts[1].replace(",", "").replace("$", ""))
                if value < 0:
                    raise ValueError("Negative value")
                self.config.min_value_major = value
                self.config.save()
                await self.bot.send_admin_message(f"✅ Major threshold (BTC/ETH/SOL) set to ${value:,}")
            except ValueError:
                await self.bot.send_admin_message("❌ Invalid value. Use a number (e.g., /setmajor 80000)")

        elif cmd == "/setother" and len(parts) >= 2:
            try:
                value = int(parts[1].replace(",", "").replace("$", ""))
                if value < 0:
                    raise ValueError("Negative value")
                self.config.min_value_other = value
                self.config.save()
                await self.bot.send_admin_message(f"✅ Other threshold set to ${value:,}")
            except ValueError:
                await self.bot.send_admin_message("❌ Invalid value. Use a number (e.g., /setother 40000)")

        elif cmd == "/setconf" and len(parts) >= 2:
            level = parts[1].upper()
            if level in ["LOW", "MEDIUM", "HIGH"]:
                self.config.min_confidence = level
                self.config.save()
                await self.bot.send_admin_message(f"✅ Min confidence set to {level}")
            else:
                await self.bot.send_admin_message("❌ Invalid level. Use: LOW, MEDIUM, or HIGH")


# =============================================================================
# Entry Point
# =============================================================================

def create_default_config() -> None:
    """Create default config file."""
    config = Config(
        telegram_bot_token="YOUR_BOT_TOKEN",
        telegram_channel_id="YOUR_CHANNEL_ID",
        telegram_admin_id=0,
        tickers=[
            TickerConfig("BTCUSDT"),
            TickerConfig("ETHUSDT"),
        ],
    )
    config.save("config.json")
    print("Created default config.json - please edit with your values")


async def main():
    config_path = "config.json"

    if not Path(config_path).exists():
        create_default_config()
        return

    config = Config.load(config_path)

    if config.telegram_bot_token == "YOUR_BOT_TOKEN":
        print("Please configure config.json with your Telegram credentials")
        return

    print(f"Starting TWAP Alert Service...")
    print(f"Major threshold (BTC/ETH/SOL): ${config.min_value_major:,}")
    print(f"Other threshold: ${config.min_value_other:,}")

    service = TWAPAlertService(config)

    try:
        await service.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        await service.stop()
    except Exception as e:
        print(f"Service error: {e}")
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
