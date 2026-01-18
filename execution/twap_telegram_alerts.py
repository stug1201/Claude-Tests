#!/usr/bin/env python3
"""
TWAP Telegram Alerts

Multi-ticker TWAP detection on Binance Perpetual Futures with Telegram alerts.

Features:
- Monitors multiple tickers simultaneously
- Sends alerts to a Telegram channel
- Admin commands via DM (authorized user only)
- Persistent configuration
- Per-ticker TWAP naming (e.g., BTC-B1, ETH-S2)

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

from twap_data_collector import TradeCollector, Trade, get_available_pairs
from twap_fourier_analyzer import TWAPAnalyzer
from twap_classifier import TWAPClassifier, SizeCategory


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
    min_size: str = "MEDIUM"  # Filter out SMALL TWAPs by default
    alert_on_updates: bool = False

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
            "min_size": self.min_size,
            "alert_on_updates": self.alert_on_updates,
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
            min_size=data.get("min_size", "MEDIUM"),
            alert_on_updates=data.get("alert_on_updates", False),
        )

    def save(self, path: str = "config.json") -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str = "config.json") -> "Config":
        with open(path, "r") as f:
            return cls.from_dict(json.load(f))


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

    async def stop(self) -> None:
        if self.session:
            await self.session.close()

    async def _call(self, method: str, **params) -> dict:
        """Call Telegram API method."""
        url = self.BASE_URL.format(token=self.token, method=method)
        async with self.session.post(url, json=params) as resp:
            return await resp.json()

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

    def __init__(self):
        # Per-ticker tracking: {ticker: {side: [TWAPs]}}
        self.tracked: Dict[str, Dict[str, List[TrackedTWAP]]] = {}
        # Per-ticker counters: {ticker: {side: count}}
        self.counters: Dict[str, Dict[str, int]] = {}

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
    def format_new_twap(ticker: str, tracked: TrackedTWAP, classified) -> str:
        """Format a new TWAP detection alert."""
        d = classified.detection

        return (
            f"🎯 <b>NEW TWAP: {tracked.name}</b>\n"
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
    def format_twap_update(ticker: str, tracked: TrackedTWAP, classified) -> str:
        """Format a TWAP update alert."""
        d = classified.detection
        duration = (tracked.last_seen - tracked.first_seen).total_seconds()
        duration_str = f"{duration:.0f}s" if duration < 60 else f"{duration/60:.1f}min"

        return (
            f"🔄 <b>UPDATE: {tracked.name}</b>\n"
            f"\n"
            f"<b>Ticker:</b> {ticker}\n"
            f"<b>Side:</b> {d.side.upper()}\n"
            f"<b>Seen:</b> {tracked.detection_count}x over {duration_str}\n"
            f"<b>Est. Total:</b> ~${d.estimated_total_value:,.0f}\n"
            f"<b>Confidence:</b> {classified.confidence_level.value}"
        )

    @staticmethod
    def format_status(monitors: Dict[str, TickerMonitor], tracker: TWAPTracker, paused: bool) -> str:
        """Format status message."""
        lines = []

        if paused:
            lines.append("⏸️ <b>STATUS: PAUSED</b>\n")
        else:
            lines.append("📊 <b>TWAP Monitor Status</b>\n")

        lines.append(f"<b>Monitoring {len(monitors)} tickers:</b>")
        for symbol, monitor in monitors.items():
            stats = monitor.get_stats()
            trades = stats.get("trades_in_buffer", 0)
            buffer = stats.get("buffer_duration_sec", 0)
            status = "🟢" if monitor.running and buffer > 60 else "🟡"
            lines.append(f"  {status} {symbol}: {trades} trades, {buffer:.0f}s buffer")

        active = tracker.get_all_active()
        if active:
            lines.append(f"\n<b>Active TWAPs ({len(active)}):</b>")
            for twap in active:
                duration = (twap.last_seen - twap.first_seen).total_seconds()
                duration_str = f"{duration:.0f}s" if duration < 60 else f"{duration/60:.1f}min"
                lines.append(f"  • {twap.name}: {twap.interval_seconds:.1f}s interval, {duration_str}")
        else:
            lines.append("\n<i>No active TWAPs detected</i>")

        return "\n".join(lines)


# =============================================================================
# Main Application
# =============================================================================

class TWAPAlertService:
    """Main service that coordinates everything."""

    # Size level ordering for threshold checking
    SIZE_LEVELS = {"SMALL": 0, "MEDIUM": 1, "LARGE": 2, "WHALE": 3}

    def __init__(self, config: Config):
        self.config = config
        self.bot = TelegramBot(
            config.telegram_bot_token,
            config.telegram_channel_id,
            config.telegram_admin_id,
        )
        self.tracker = TWAPTracker()
        self.monitors: Dict[str, TickerMonitor] = {}
        self.formatter = AlertFormatter()
        self.running = False
        self.paused = False

    def _confidence_meets_threshold(self, confidence: str) -> bool:
        """Check if confidence meets minimum threshold."""
        levels = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
        return levels.get(confidence.upper(), 0) >= levels.get(self.config.min_confidence.upper(), 0)

    def _size_meets_threshold(self, size_category: SizeCategory) -> bool:
        """Check if size meets minimum threshold."""
        size_value = self.SIZE_LEVELS.get(size_category.value.upper(), 0)
        threshold = self.SIZE_LEVELS.get(self.config.min_size.upper(), 1)
        return size_value >= threshold

    async def start(self) -> None:
        """Start the service."""
        await self.bot.start()
        self.running = True

        for ticker_config in self.config.tickers:
            if ticker_config.enabled:
                await self._add_monitor(ticker_config.symbol)

        tickers_list = ", ".join([t.symbol for t in self.config.tickers if t.enabled])
        await self.bot.send_admin_message(
            f"🟢 <b>TWAP Alert Service started!</b>\n\n"
            f"Monitoring: {tickers_list}\n"
            f"Min size: {self.config.min_size}\n"
            f"Min confidence: {self.config.min_confidence}\n\n"
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
        await self.bot.send_admin_message("🔴 TWAP Alert Service stopped.")
        await self.bot.stop()

    async def _add_monitor(self, symbol: str) -> None:
        """Add a ticker monitor."""
        monitor = TickerMonitor(
            symbol=symbol,
            buffer_minutes=self.config.buffer_minutes,
            analysis_interval_sec=self.config.analysis_interval_sec,
            min_buffer_sec=self.config.min_buffer_sec,
        )
        self.monitors[symbol] = monitor
        asyncio.create_task(monitor.start())

    async def _remove_monitor(self, symbol: str) -> bool:
        """Remove a ticker monitor."""
        if symbol in self.monitors:
            await self.monitors[symbol].stop()
            del self.monitors[symbol]
            self.tracker.clear_ticker(symbol)
            return True
        return False

    async def _analysis_loop(self) -> None:
        """Main analysis loop."""
        while self.running:
            await asyncio.sleep(5)

            if self.paused:
                continue

            for symbol, monitor in list(self.monitors.items()):
                if not monitor.should_analyze():
                    continue

                results = monitor.analyze()

                for detection, classified in results:
                    # Skip if doesn't meet confidence threshold
                    if not self._confidence_meets_threshold(classified.confidence_level.value):
                        continue

                    # Skip if doesn't meet size threshold (filters out small TWAPs)
                    if not self._size_meets_threshold(classified.size_category):
                        continue

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

    async def _command_loop(self) -> None:
        """Listen for admin commands."""
        while self.running:
            try:
                updates = await self.bot.get_updates(timeout=10)

                for update in updates:
                    message = update.get("message", {})
                    user_id = message.get("from", {}).get("id")
                    text = message.get("text", "")

                    if not self.bot.is_admin(user_id):
                        continue

                    await self._handle_command(text)

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
            available = ", ".join(get_available_pairs()[:10]) + "..."
            help_text = (
                "📖 <b>Admin Commands</b>\n\n"
                "<b>Monitoring:</b>\n"
                "/status - Show monitoring status\n"
                "/pause - Pause monitoring\n"
                "/resume - Resume monitoring\n"
                "/shutdown - Stop service (requires systemctl to restart)\n"
                "\n<b>Tickers:</b>\n"
                "/list - List monitored tickers\n"
                "/add SYMBOL - Add ticker (e.g., /add SOLUSDT)\n"
                "/remove SYMBOL - Remove ticker\n"
                "\n<b>Config:</b>\n"
                "/config - Show current config\n"
                "/setsize LEVEL - Set min size (SMALL/MEDIUM/LARGE/WHALE)\n"
                "/setconf LEVEL - Set min confidence (LOW/MEDIUM/HIGH)\n"
                f"\n<b>Available pairs:</b> {available}"
            )
            await self.bot.send_admin_message(help_text)

        elif cmd == "/status":
            msg = self.formatter.format_status(self.monitors, self.tracker, self.paused)
            await self.bot.send_admin_message(msg)

        elif cmd == "/list":
            if not self.monitors:
                await self.bot.send_admin_message("No tickers being monitored.")
            else:
                lines = ["<b>Monitored Tickers:</b>"]
                for symbol in self.monitors:
                    twaps = self.tracker.get_ticker_active(symbol)
                    twap_str = f" ({len(twaps)} active)" if twaps else ""
                    lines.append(f"• {symbol}{twap_str}")
                await self.bot.send_admin_message("\n".join(lines))

        elif cmd == "/add" and len(parts) >= 2:
            symbol = parts[1].upper()
            if symbol in self.monitors:
                await self.bot.send_admin_message(f"❌ {symbol} already being monitored")
            else:
                await self._add_monitor(symbol)
                self.config.tickers.append(TickerConfig(symbol=symbol))
                self.config.save()
                await self.bot.send_admin_message(f"✅ Added {symbol}")

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
            await self.bot.send_channel_message("⏸️ <i>Monitoring paused by admin</i>")

        elif cmd == "/resume":
            self.paused = False
            await self.bot.send_admin_message("▶️ Monitoring resumed")
            await self.bot.send_channel_message("▶️ <i>Monitoring resumed</i>")

        elif cmd == "/shutdown":
            await self.bot.send_admin_message("🔴 Shutting down... Use systemctl to restart.")
            await self.bot.send_channel_message("🔴 <i>Service stopped by admin</i>")
            await self.stop()
            sys.exit(0)

        elif cmd == "/config":
            msg = (
                f"<b>Configuration:</b>\n"
                f"Analysis interval: {self.config.analysis_interval_sec}s\n"
                f"Buffer size: {self.config.buffer_minutes}min\n"
                f"Min confidence: {self.config.min_confidence}\n"
                f"Min size: {self.config.min_size}\n"
                f"Alert on updates: {self.config.alert_on_updates}"
            )
            await self.bot.send_admin_message(msg)

        elif cmd == "/setsize" and len(parts) >= 2:
            level = parts[1].upper()
            if level in self.SIZE_LEVELS:
                self.config.min_size = level
                self.config.save()
                await self.bot.send_admin_message(f"✅ Min size set to {level}")
            else:
                await self.bot.send_admin_message("❌ Invalid level. Use: SMALL, MEDIUM, LARGE, or WHALE")

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

    service = TWAPAlertService(config)

    try:
        await service.start()
    except KeyboardInterrupt:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
