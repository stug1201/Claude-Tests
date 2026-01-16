#!/usr/bin/env python3
"""
TWAP Data Collector

Collects real-time trade data from Binance and Coinbase via WebSocket.
Maintains a rolling buffer of trades for FFT analysis.

Usage:
    from twap_data_collector import TradeCollector

    collector = TradeCollector(
        exchange="binance",
        market_type="perpetual",
        symbol="BTCUSDT",
        buffer_minutes=30
    )
    await collector.start()
"""

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Optional

import websockets


class Exchange(Enum):
    BINANCE = "binance"
    COINBASE = "coinbase"


class MarketType(Enum):
    SPOT = "spot"
    PERPETUAL = "perpetual"


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

    def get_trades(self) -> list[Trade]:
        """Get all trades in buffer."""
        self._cleanup()
        return list(self.trades)

    def get_trades_since(self, since_ms: int) -> list[Trade]:
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


class TradeCollector:
    """
    Collects trades from exchange WebSocket and maintains rolling buffer.
    """

    # WebSocket endpoints
    WS_ENDPOINTS = {
        (Exchange.BINANCE, MarketType.SPOT): "wss://stream.binance.com:9443/ws",
        (Exchange.BINANCE, MarketType.PERPETUAL): "wss://fstream.binance.com/ws",
        (Exchange.COINBASE, MarketType.SPOT): "wss://ws-feed.exchange.coinbase.com",
        (Exchange.COINBASE, MarketType.PERPETUAL): "wss://ws-feed.exchange.coinbase.com",
    }

    def __init__(
        self,
        exchange: str | Exchange,
        market_type: str | MarketType,
        symbol: str,
        buffer_minutes: int = 30,
        on_trade: Optional[Callable[[Trade], None]] = None,
    ):
        self.exchange = Exchange(exchange) if isinstance(exchange, str) else exchange
        self.market_type = MarketType(market_type) if isinstance(market_type, str) else market_type
        self.symbol = symbol.upper()
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
    def ws_url(self) -> str:
        """Get WebSocket URL for current exchange/market."""
        base_url = self.WS_ENDPOINTS[(self.exchange, self.market_type)]

        if self.exchange == Exchange.BINANCE:
            # Binance uses stream-specific URLs
            symbol_lower = self.symbol.lower()
            return f"{base_url}/{symbol_lower}@trade"
        else:
            # Coinbase uses a single endpoint with subscription
            return base_url

    def _format_coinbase_symbol(self) -> str:
        """Convert symbol to Coinbase format (e.g., BTCUSDT -> BTC-USD)."""
        symbol = self.symbol
        # Handle common conversions
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USD"
        elif symbol.endswith("USD"):
            base = symbol[:-3]
            return f"{base}-USD"
        elif symbol.endswith("PERP"):
            base = symbol[:-4]
            return f"{base}-PERP"
        return symbol

    async def _subscribe_coinbase(self, ws) -> None:
        """Send subscription message for Coinbase."""
        product_id = self._format_coinbase_symbol()

        subscribe_msg = {
            "type": "subscribe",
            "product_ids": [product_id],
            "channels": ["matches"]
        }
        await ws.send(json.dumps(subscribe_msg))

    def _parse_binance_trade(self, data: dict) -> Optional[Trade]:
        """Parse Binance trade message."""
        try:
            # Binance trade format:
            # {"e":"trade","E":timestamp,"s":"BTCUSDT","t":trade_id,
            #  "p":"price","q":"quantity","b":buyer_order_id,
            #  "a":seller_order_id,"T":trade_time,"m":is_buyer_maker}
            return Trade(
                timestamp_ms=data["T"],
                price=float(data["p"]),
                size=float(data["q"]),
                side="sell" if data["m"] else "buy"  # m=true means buyer is maker, so taker sold
            )
        except (KeyError, ValueError) as e:
            print(f"Error parsing Binance trade: {e}")
            return None

    def _parse_coinbase_trade(self, data: dict) -> Optional[Trade]:
        """Parse Coinbase trade message."""
        try:
            if data.get("type") != "match":
                return None

            # Coinbase match format:
            # {"type":"match","trade_id":123,"maker_order_id":"...",
            #  "taker_order_id":"...","side":"buy","size":"0.1",
            #  "price":"50000","product_id":"BTC-USD","time":"..."}
            timestamp = datetime.fromisoformat(data["time"].replace("Z", "+00:00"))
            return Trade(
                timestamp_ms=int(timestamp.timestamp() * 1000),
                price=float(data["price"]),
                size=float(data["size"]),
                side=data["side"]
            )
        except (KeyError, ValueError) as e:
            print(f"Error parsing Coinbase trade: {e}")
            return None

    def _parse_trade(self, message: str) -> Optional[Trade]:
        """Parse trade from WebSocket message."""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return None

        if self.exchange == Exchange.BINANCE:
            return self._parse_binance_trade(data)
        else:
            return self._parse_coinbase_trade(data)

    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        self._ws = await websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
        )

        # Coinbase requires explicit subscription
        if self.exchange == Exchange.COINBASE:
            await self._subscribe_coinbase(self._ws)

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

        print(f"Connecting to {self.exchange.value} {self.market_type.value} for {self.symbol}...")

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

    def get_trades(self) -> list[Trade]:
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


# Available trading pairs (commonly traded)
BINANCE_SPOT_PAIRS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "ARBUSDT",
]

BINANCE_PERP_PAIRS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "ARBUSDT",
]

COINBASE_SPOT_PAIRS = [
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD",
    "ADA-USD", "AVAX-USD", "DOT-USD", "MATIC-USD", "LINK-USD",
    "LTC-USD", "ATOM-USD", "UNI-USD", "ARB-USD", "OP-USD",
]

COINBASE_PERP_PAIRS = [
    "BTC-PERP", "ETH-PERP", "SOL-PERP", "XRP-PERP", "DOGE-PERP",
    "AVAX-PERP", "MATIC-PERP", "LINK-PERP", "LTC-PERP", "ARB-PERP",
]


def get_available_pairs(exchange: Exchange, market_type: MarketType) -> list[str]:
    """Get available trading pairs for exchange/market combination."""
    if exchange == Exchange.BINANCE:
        if market_type == MarketType.SPOT:
            return BINANCE_SPOT_PAIRS
        else:
            return BINANCE_PERP_PAIRS
    else:
        if market_type == MarketType.SPOT:
            return COINBASE_SPOT_PAIRS
        else:
            return COINBASE_PERP_PAIRS


async def main():
    """Test the collector."""
    def on_trade(trade: Trade):
        print(f"  {trade.side.upper():4} {trade.size:.6f} @ ${trade.price:.2f}")

    collector = TradeCollector(
        exchange="binance",
        market_type="perpetual",
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
