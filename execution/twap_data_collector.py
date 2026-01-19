#!/usr/bin/env python3
"""
TWAP Data Collector

Collects real-time trade data from Binance Spot and Perpetual Futures via WebSocket.
Maintains a rolling buffer of trades for FFT analysis.

Symbol convention:
- BTCUSDT = Perpetual futures
- BTCUSDT.S = Spot market

Usage:
    from twap_data_collector import TradeCollector

    # Perpetual
    collector = TradeCollector(symbol="BTCUSDT", buffer_minutes=30)

    # Spot
    collector = TradeCollector(symbol="BTCUSDT.S", buffer_minutes=30)

    await collector.start()
"""

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional, Tuple

import websockets


@dataclass
class Trade:
    """Represents a single trade."""
    timestamp_ms: int
    price: float
    size: float
    side: str  # "buy" or "sell"

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000)

    @property
    def value_usd(self) -> float:
        return self.price * self.size


@dataclass
class TradeBuffer:
    """Rolling buffer of trades with time-based expiration."""
    max_age_ms: int
    trades: deque = field(default_factory=deque)

    def add(self, trade: Trade) -> None:
        """Add a trade and remove expired trades."""
        self.trades.append(trade)
        self._cleanup()

    def _cleanup(self) -> None:
        """Remove trades older than max_age_ms."""
        cutoff = int(time.time() * 1000) - self.max_age_ms
        while self.trades and self.trades[0].timestamp_ms < cutoff:
            self.trades.popleft()

    def get_trades(self) -> List[Trade]:
        """Get all trades in buffer."""
        self._cleanup()
        return list(self.trades)

    def get_trades_since(self, since_ms: int) -> List[Trade]:
        """Get trades since a specific timestamp."""
        self._cleanup()
        return [t for t in self.trades if t.timestamp_ms >= since_ms]

    @property
    def count(self) -> int:
        return len(self.trades)

    @property
    def duration_ms(self) -> int:
        """Duration covered by trades in buffer."""
        if len(self.trades) < 2:
            return 0
        return self.trades[-1].timestamp_ms - self.trades[0].timestamp_ms


# WebSocket endpoints
BINANCE_PERP_WS = "wss://fstream.binance.com/ws"
BINANCE_SPOT_WS = "wss://stream.binance.com:9443/ws"


def parse_symbol(symbol: str) -> Tuple[str, bool]:
    """
    Parse symbol to extract base symbol and market type.

    Returns:
        (base_symbol, is_spot)

    Examples:
        "BTCUSDT" -> ("BTCUSDT", False)  # Perpetual
        "BTCUSDT.S" -> ("BTCUSDT", True)  # Spot
    """
    if symbol.endswith(".S"):
        return symbol[:-2].upper(), True
    return symbol.upper(), False


def format_symbol(base_symbol: str, is_spot: bool) -> str:
    """Format symbol with market type suffix."""
    if is_spot:
        return f"{base_symbol.upper()}.S"
    return base_symbol.upper()


class TradeCollector:
    """
    Collects trades from Binance WebSocket (Spot or Perpetual).

    Symbol convention:
    - "BTCUSDT" = Perpetual futures
    - "BTCUSDT.S" = Spot market
    """

    def __init__(
        self,
        symbol: str,
        buffer_minutes: int = 30,
        on_trade: Optional[Callable[[Trade], None]] = None,
    ):
        self.original_symbol = symbol
        self.base_symbol, self.is_spot = parse_symbol(symbol)
        self.symbol = format_symbol(self.base_symbol, self.is_spot)
        self.buffer_minutes = buffer_minutes
        self.on_trade = on_trade

        # Initialize buffer
        self.buffer = TradeBuffer(max_age_ms=buffer_minutes * 60 * 1000)

        # Connection state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Stats
        self.trades_received = 0
        self.start_time: Optional[datetime] = None

    @property
    def market_type(self) -> str:
        """Get market type string."""
        return "Spot" if self.is_spot else "Perp"

    @property
    def ws_url(self) -> str:
        """Get WebSocket URL for the symbol."""
        symbol_lower = self.base_symbol.lower()
        if self.is_spot:
            return f"{BINANCE_SPOT_WS}/{symbol_lower}@trade"
        return f"{BINANCE_PERP_WS}/{symbol_lower}@trade"

    def _parse_trade(self, message: str) -> Optional[Trade]:
        """Parse trade from WebSocket message."""
        try:
            data = json.loads(message)
            # Binance trade format (same for spot and perp):
            # {"e":"trade","E":timestamp,"s":"BTCUSDT","t":trade_id,
            #  "p":"price","q":"quantity","b":buyer_order_id,
            #  "a":seller_order_id,"T":trade_time,"m":is_buyer_maker}
            return Trade(
                timestamp_ms=data["T"],
                price=float(data["p"]),
                size=float(data["q"]),
                side="sell" if data["m"] else "buy"  # m=true means buyer is maker, so taker sold
            )
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            print(f"Error parsing trade: {e}")
            return None

    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        self._ws = await websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
        )

    async def _listen(self) -> None:
        """Listen for trades and add to buffer."""
        async for message in self._ws:
            if not self._running:
                break

            trade = self._parse_trade(message)
            if trade:
                self.buffer.add(trade)
                self.trades_received += 1

                if self.on_trade:
                    self.on_trade(trade)

    async def start(self) -> None:
        """Start collecting trades."""
        if self._running:
            return

        self._running = True
        self.start_time = datetime.now()

        print(f"Connecting to Binance {self.market_type} for {self.symbol}...")

        while self._running:
            try:
                await self._connect()
                print(f"Connected! Collecting trades (buffer: {self.buffer_minutes} min)...")
                await self._listen()
            except websockets.ConnectionClosed as e:
                if self._running:
                    print(f"Connection closed: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)
            except Exception as e:
                if self._running:
                    print(f"Error: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop collecting trades."""
        self._running = False
        if self._ws:
            await self._ws.close()

    def get_trades(self) -> List[Trade]:
        """Get all trades in buffer."""
        return self.buffer.get_trades()

    def get_stats(self) -> dict:
        """Get collection statistics."""
        trades = self.buffer.get_trades()

        if not trades:
            return {
                "trades_in_buffer": 0,
                "buffer_duration_sec": 0,
                "trades_per_second": 0,
                "total_volume": 0,
                "buy_volume": 0,
                "sell_volume": 0,
            }

        duration_sec = self.buffer.duration_ms / 1000
        buy_trades = [t for t in trades if t.side == "buy"]
        sell_trades = [t for t in trades if t.side == "sell"]

        return {
            "trades_in_buffer": len(trades),
            "buffer_duration_sec": duration_sec,
            "trades_per_second": len(trades) / max(duration_sec, 1),
            "total_volume": sum(t.size for t in trades),
            "buy_volume": sum(t.size for t in buy_trades),
            "sell_volume": sum(t.size for t in sell_trades),
            "avg_trade_size": sum(t.size for t in trades) / len(trades),
            "avg_price": sum(t.price for t in trades) / len(trades),
        }


# Available trading pairs
PERPETUAL_PAIRS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "ARBUSDT",
    "OPUSDT", "APTUSDT", "NEARUSDT", "FTMUSDT", "SANDUSDT",
]

SPOT_PAIRS = [
    "BTCUSDT.S", "ETHUSDT.S", "BNBUSDT.S", "SOLUSDT.S", "XRPUSDT.S",
    "DOGEUSDT.S", "ADAUSDT.S", "AVAXUSDT.S", "DOTUSDT.S", "MATICUSDT.S",
    "LINKUSDT.S", "LTCUSDT.S", "ATOMUSDT.S", "UNIUSDT.S", "ARBUSDT.S",
]


def get_available_pairs(include_spot: bool = True) -> List[str]:
    """Get available trading pairs."""
    pairs = list(PERPETUAL_PAIRS)
    if include_spot:
        pairs.extend(SPOT_PAIRS)
    return pairs


def get_perpetual_pairs() -> List[str]:
    """Get available perpetual pairs only."""
    return list(PERPETUAL_PAIRS)


def get_spot_pairs() -> List[str]:
    """Get available spot pairs only."""
    return list(SPOT_PAIRS)


async def main():
    """Test the collector."""
    def on_trade(trade: Trade):
        print(f"  {trade.side.upper():4} {trade.size:.6f} @ ${trade.price:.2f}")

    # Test perpetual
    collector = TradeCollector(
        symbol="BTCUSDT",
        buffer_minutes=5,
        on_trade=on_trade,
    )

    try:
        await collector.start()
    except KeyboardInterrupt:
        print("\nStopping...")
        await collector.stop()

        stats = collector.get_stats()
        print(f"\nStats: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
