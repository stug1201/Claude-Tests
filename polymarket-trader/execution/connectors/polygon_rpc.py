#!/usr/bin/env python3
"""
polygon_rpc.py -- Polygon RPC connector via Alchemy endpoint.

Provides async access to the Polygon blockchain via Alchemy's JSON-RPC API.
Used by the wallet profiler for on-chain transaction history and IPS scoring
in the insider detection strategy.

Auth: Alchemy API key embedded in the endpoint URL.

Capabilities:
    - MATIC balance lookups (eth_getBalance)
    - ERC-20 token balance queries (eth_call to balanceOf)
    - Transaction history via eth_getLogs and alchemy_getAssetTransfers
    - Individual transaction details (eth_getTransactionByHash)
    - Current block number (eth_blockNumber)
    - Event log filtering (eth_getLogs)

Rate limiting:
    Alchemy free tier allows 330 requests/sec. A simple token-bucket rate
    limiter is included to stay within that budget.

Usage:
    python execution/connectors/polygon_rpc.py          # Live mode
    python execution/connectors/polygon_rpc.py --test   # Fixture mode

Environment variables:
    ALCHEMY_API_KEY -- Alchemy API key for Polygon RPC

See: directives/01_data_infrastructure.md
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Optional

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
# Project paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent              # execution/connectors/
EXECUTION_DIR = SCRIPT_DIR.parent                         # execution/
PROJECT_ROOT = EXECUTION_DIR.parent                       # polymarket-trader/
TMP_DIR = PROJECT_ROOT / ".tmp"
FIXTURES_DIR = TMP_DIR / "fixtures"
FIXTURE_FILE = FIXTURES_DIR / "polygon_rpc_fixtures.json"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALCHEMY_BASE_URL = "https://polygon-mainnet.g.alchemy.com/v2"
REQUEST_TIMEOUT_SECONDS = 30
RATE_LIMIT_RPS = 330  # Alchemy free tier: 330 requests/sec

# ERC-20 ABI fragment for balanceOf(address) -> uint256
# Function selector: 0x70a08231
BALANCE_OF_SELECTOR = "0x70a08231"

# USDC contract on Polygon (for demo/test convenience)
USDC_POLYGON = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RPCError(Exception):
    """Raised when the JSON-RPC endpoint returns an error object."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"RPC error {code}: {message}")


class RateLimitError(RPCError):
    """Raised when the RPC endpoint returns a rate-limit error (HTTP 429)."""
    pass


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class TokenBucketRateLimiter:
    """
    Simple async token-bucket rate limiter.

    Allows up to *rate* requests per second. If the bucket is empty, callers
    are delayed until a token becomes available. This keeps Alchemy traffic
    below the free-tier ceiling of 330 req/s.
    """

    def __init__(self, rate: float = RATE_LIMIT_RPS) -> None:
        self._rate = rate
        self._max_tokens = rate
        self._tokens = rate
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available, then consume one."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self._max_tokens,
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

            if self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / self._rate
                logger.debug("Rate limiter: sleeping %.4fs", wait_time)
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
                self._last_refill = time.monotonic()
            else:
                self._tokens -= 1.0


# ---------------------------------------------------------------------------
# Polygon RPC Client
# ---------------------------------------------------------------------------

class PolygonRPCClient:
    """
    Async Polygon blockchain client via Alchemy JSON-RPC endpoint.

    Provides typed methods for common on-chain queries: balances,
    transactions, logs, and Alchemy-specific asset transfer lookups.

    Usage::

        async with PolygonRPCClient(api_key="...") as client:
            balance = await client.get_balance("0xABC...")
            block = await client.get_block_number()

    Or without context manager::

        client = PolygonRPCClient(api_key="...")
        await client.connect()
        try:
            balance = await client.get_balance("0xABC...")
        finally:
            await client.close()
    """

    def __init__(self, api_key: str) -> None:
        """
        Initialise the client.

        Args:
            api_key: Alchemy API key. Appended to the base URL to form the
                     full JSON-RPC endpoint.
        """
        self._api_key = api_key
        self._endpoint = f"{ALCHEMY_BASE_URL}/{api_key}"
        self._session: Optional[Any] = None  # aiohttp.ClientSession
        self._rate_limiter = TokenBucketRateLimiter(rate=RATE_LIMIT_RPS)
        self._request_id = 0
        logger.info(
            "PolygonRPCClient initialised (endpoint=%s/****)",
            ALCHEMY_BASE_URL,
        )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "PolygonRPCClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        """Create the underlying aiohttp session."""
        import aiohttp

        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            self._session = aiohttp.ClientSession(timeout=timeout)
            logger.info("aiohttp session created.")

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("aiohttp session closed.")

    # ------------------------------------------------------------------
    # Core RPC helper
    # ------------------------------------------------------------------

    async def _rpc_call(
        self,
        method: str,
        params: Optional[list[Any]] = None,
    ) -> Any:
        """
        Execute a JSON-RPC 2.0 call against the Alchemy endpoint.

        Args:
            method: The RPC method name (e.g. ``eth_getBalance``).
            params: Positional parameters for the RPC method.

        Returns:
            The ``result`` field from the JSON-RPC response.

        Raises:
            RPCError: If the response contains an ``error`` object.
            RateLimitError: If the server returns HTTP 429.
            aiohttp.ClientError: On network-level failures.
        """
        import aiohttp

        if self._session is None or self._session.closed:
            await self.connect()

        self._request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params or [],
        }

        await self._rate_limiter.acquire()

        logger.debug("RPC -> %s (id=%d)", method, self._request_id)

        # Retry loop for transient failures (rate limits, timeouts)
        max_retries = 4
        backoff = 1.0

        for attempt in range(1, max_retries + 1):
            try:
                async with self._session.post(
                    self._endpoint,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    # Handle HTTP-level rate limiting
                    if resp.status == 429:
                        if attempt < max_retries:
                            logger.warning(
                                "Rate limited (HTTP 429) on %s — "
                                "backing off %.1fs (attempt %d/%d)",
                                method, backoff, attempt, max_retries,
                            )
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 60.0)
                            continue
                        raise RateLimitError(
                            code=429,
                            message="Rate limited by Alchemy after retries",
                        )

                    resp.raise_for_status()
                    data = await resp.json()

                # Check for JSON-RPC error object
                if "error" in data and data["error"]:
                    err = data["error"]
                    code = err.get("code", -1)
                    message = err.get("message", "Unknown RPC error")
                    err_data = err.get("data")

                    # Rate-limit errors sometimes come as RPC errors
                    if code == -32005 or "rate" in message.lower():
                        if attempt < max_retries:
                            logger.warning(
                                "RPC rate limit error on %s — "
                                "backing off %.1fs (attempt %d/%d)",
                                method, backoff, attempt, max_retries,
                            )
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 60.0)
                            continue
                        raise RateLimitError(code=code, message=message, data=err_data)

                    raise RPCError(code=code, message=message, data=err_data)

                logger.debug(
                    "RPC <- %s (id=%d) OK", method, self._request_id
                )
                return data.get("result")

            except aiohttp.ClientError as exc:
                if attempt < max_retries:
                    logger.warning(
                        "Network error on %s: %s — retrying in %.1fs "
                        "(attempt %d/%d)",
                        method, exc, backoff, attempt, max_retries,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
                    continue
                logger.error(
                    "Network error on %s after %d attempts: %s",
                    method, max_retries, exc,
                )
                raise

        # Should not reach here, but satisfy type checker
        raise RPCError(code=-1, message="Exhausted retries")  # pragma: no cover

    # ------------------------------------------------------------------
    # Public methods — standard Ethereum JSON-RPC
    # ------------------------------------------------------------------

    async def get_balance(self, wallet_address: str) -> float:
        """
        Get the native MATIC balance for a wallet.

        Args:
            wallet_address: The 0x-prefixed wallet address.

        Returns:
            Balance in MATIC (float, 18-decimal conversion).
        """
        result = await self._rpc_call(
            "eth_getBalance",
            [wallet_address, "latest"],
        )
        wei = int(result, 16)
        matic = wei / 1e18
        logger.info(
            "Balance for %s: %.6f MATIC",
            wallet_address, matic,
        )
        return matic

    async def get_token_balance(
        self,
        wallet_address: str,
        token_contract: str,
        decimals: int = 6,
    ) -> float:
        """
        Get the ERC-20 token balance for a wallet.

        Calls the ``balanceOf(address)`` function on the token contract
        using ``eth_call``.

        Args:
            wallet_address: The 0x-prefixed wallet address.
            token_contract: The 0x-prefixed ERC-20 contract address.
            decimals: Token decimal places (default 6 for USDC).

        Returns:
            Token balance as a float, adjusted for decimals.
        """
        # Encode balanceOf(address) call data:
        # 4-byte selector + 32-byte zero-padded address
        address_padded = wallet_address.lower().replace("0x", "").zfill(64)
        call_data = BALANCE_OF_SELECTOR + address_padded

        result = await self._rpc_call(
            "eth_call",
            [
                {"to": token_contract, "data": call_data},
                "latest",
            ],
        )

        raw_balance = int(result, 16)
        balance = raw_balance / (10 ** decimals)
        logger.info(
            "Token balance for %s on %s: %.6f (decimals=%d)",
            wallet_address, token_contract, balance, decimals,
        )
        return balance

    async def get_block_number(self) -> int:
        """
        Get the current block number on Polygon.

        Returns:
            The latest block number as an integer.
        """
        result = await self._rpc_call("eth_blockNumber")
        block_number = int(result, 16)
        logger.info("Current block number: %d", block_number)
        return block_number

    async def get_transaction(self, tx_hash: str) -> dict:
        """
        Get full transaction details by hash.

        Args:
            tx_hash: The 0x-prefixed transaction hash.

        Returns:
            Transaction object as a dict with fields like ``from``,
            ``to``, ``value``, ``blockNumber``, ``gas``, etc.
            Returns an empty dict if the transaction is not found.
        """
        result = await self._rpc_call(
            "eth_getTransactionByHash",
            [tx_hash],
        )
        if result is None:
            logger.warning("Transaction not found: %s", tx_hash)
            return {}
        logger.info(
            "Transaction %s: block=%s, from=%s, to=%s",
            tx_hash,
            result.get("blockNumber"),
            result.get("from"),
            result.get("to"),
        )
        return result

    async def get_transaction_history(
        self,
        wallet_address: str,
        from_block: int = 0,
        to_block: str | int = "latest",
    ) -> list[dict]:
        """
        Get transaction history for a wallet using eth_getLogs.

        Filters for Transfer events where the wallet appears as sender
        or receiver. This captures ERC-20 transfers; for native MATIC
        transfers, use ``get_asset_transfers`` instead.

        Args:
            wallet_address: The 0x-prefixed wallet address.
            from_block: Starting block number (default 0).
            to_block: Ending block number or 'latest'.

        Returns:
            List of log entries matching the wallet address.
        """
        # ERC-20 Transfer event topic
        # keccak256("Transfer(address,address,uint256)")
        transfer_topic = (
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        )
        address_padded = "0x" + wallet_address.lower().replace("0x", "").zfill(64)

        from_block_hex = hex(from_block)
        to_block_hex = hex(to_block) if isinstance(to_block, int) else to_block

        # Query for logs where wallet is sender (topic[1])
        logs_sent = await self._rpc_call(
            "eth_getLogs",
            [{
                "fromBlock": from_block_hex,
                "toBlock": to_block_hex,
                "topics": [transfer_topic, address_padded],
            }],
        )

        # Query for logs where wallet is receiver (topic[2])
        logs_received = await self._rpc_call(
            "eth_getLogs",
            [{
                "fromBlock": from_block_hex,
                "toBlock": to_block_hex,
                "topics": [transfer_topic, None, address_padded],
            }],
        )

        all_logs = (logs_sent or []) + (logs_received or [])
        logger.info(
            "Transaction history for %s: %d logs (sent=%d, received=%d, "
            "blocks %s..%s)",
            wallet_address,
            len(all_logs),
            len(logs_sent or []),
            len(logs_received or []),
            from_block_hex,
            to_block_hex,
        )
        return all_logs

    async def get_logs(
        self,
        contract_address: str,
        topics: list[Optional[str]],
        from_block: int | str = 0,
        to_block: int | str = "latest",
    ) -> list[dict]:
        """
        Get event logs filtered by contract and topics.

        Args:
            contract_address: The 0x-prefixed contract address.
            topics: List of topic filters (position-indexed). Use None
                    for wildcard positions.
            from_block: Starting block (int or hex string).
            to_block: Ending block (int, hex string, or 'latest').

        Returns:
            List of matching log entries.
        """
        from_hex = hex(from_block) if isinstance(from_block, int) else from_block
        to_hex = hex(to_block) if isinstance(to_block, int) else to_block

        result = await self._rpc_call(
            "eth_getLogs",
            [{
                "address": contract_address,
                "topics": topics,
                "fromBlock": from_hex,
                "toBlock": to_hex,
            }],
        )
        logs = result or []
        logger.info(
            "getLogs for %s: %d entries (blocks %s..%s)",
            contract_address, len(logs), from_hex, to_hex,
        )
        return logs

    # ------------------------------------------------------------------
    # Alchemy-specific methods
    # ------------------------------------------------------------------

    async def get_asset_transfers(
        self,
        wallet_address: str,
        category: str | list[str] = "erc20",
        max_count: int = 100,
        direction: str = "both",
    ) -> list[dict]:
        """
        Get asset transfers using Alchemy's alchemy_getAssetTransfers.

        This is an Alchemy-enhanced method that provides richer transfer
        data than raw eth_getLogs, including decoded token values, asset
        names, and native MATIC transfers.

        Args:
            wallet_address: The 0x-prefixed wallet address.
            category: Transfer category or list of categories. Options:
                      'external', 'internal', 'erc20', 'erc721', 'erc1155',
                      'specialnft'. Default: 'erc20'.
            max_count: Maximum number of results (default 100, max 1000).
            direction: 'from', 'to', or 'both' (default 'both').

        Returns:
            List of transfer objects with fields like ``from``, ``to``,
            ``value``, ``asset``, ``category``, ``blockNum``, ``hash``.
        """
        categories = [category] if isinstance(category, str) else category
        max_count_hex = hex(min(max_count, 1000))
        all_transfers: list[dict] = []

        if direction in ("from", "both"):
            params: dict[str, Any] = {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "fromAddress": wallet_address,
                "category": categories,
                "maxCount": max_count_hex,
                "withMetadata": True,
                "order": "desc",
            }
            result = await self._rpc_call(
                "alchemy_getAssetTransfers",
                [params],
            )
            transfers = result.get("transfers", []) if result else []
            all_transfers.extend(transfers)
            logger.info(
                "Asset transfers FROM %s: %d results",
                wallet_address, len(transfers),
            )

        if direction in ("to", "both"):
            params = {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "toAddress": wallet_address,
                "category": categories,
                "maxCount": max_count_hex,
                "withMetadata": True,
                "order": "desc",
            }
            result = await self._rpc_call(
                "alchemy_getAssetTransfers",
                [params],
            )
            transfers = result.get("transfers", []) if result else []
            all_transfers.extend(transfers)
            logger.info(
                "Asset transfers TO %s: %d results",
                wallet_address, len(transfers),
            )

        logger.info(
            "Total asset transfers for %s: %d (categories=%s, direction=%s)",
            wallet_address, len(all_transfers), categories, direction,
        )
        return all_transfers


# ---------------------------------------------------------------------------
# Test / fixture mode
# ---------------------------------------------------------------------------

# Fixture data for --test mode. No network calls are made.
_FIXTURE_DATA: dict[str, Any] = {
    "block_number": 55_000_000,
    "balance": {
        "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18",
        "matic": 125.437821,
        "wei_hex": "0x6c9e1cf76a4b1e00000",
    },
    "token_balance": {
        "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18",
        "token_contract": USDC_POLYGON,
        "balance": 1042.583912,
        "decimals": 6,
        "raw_hex": "0x3e29e3e8",
    },
    "transaction": {
        "hash": "0xabc123def456789012345678901234567890123456789012345678901234abcd",
        "blockNumber": "0x346c7b0",
        "from": "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18",
        "to": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "value": "0x0",
        "gas": "0x5208",
        "gasPrice": "0x6fc23ac00",
        "nonce": "0x2a",
        "input": "0xa9059cbb",
    },
    "logs": [
        {
            "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000742d35cc6634c0532925a3b844bc9e7595f2bd18",
                "0x00000000000000000000000028c6c06298d514db089934071355e5743bf21d60",
            ],
            "data": "0x0000000000000000000000000000000000000000000000000000000005f5e100",
            "blockNumber": "0x346c7a0",
            "transactionHash": "0xabc123def456789012345678901234567890123456789012345678901234abcd",
            "logIndex": "0x1",
        },
        {
            "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x00000000000000000000000028c6c06298d514db089934071355e5743bf21d60",
                "0x000000000000000000000000742d35cc6634c0532925a3b844bc9e7595f2bd18",
            ],
            "data": "0x00000000000000000000000000000000000000000000000000000000002dc6c0",
            "blockNumber": "0x346c7b0",
            "transactionHash": "0xdef456abc789012345678901234567890123456789012345678901234567def0",
            "logIndex": "0x3",
        },
    ],
    "asset_transfers": [
        {
            "blockNum": "0x346c7a0",
            "hash": "0xabc123def456789012345678901234567890123456789012345678901234abcd",
            "from": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
            "to": "0x28c6c06298d514db089934071355e5743bf21d60",
            "value": 100.0,
            "asset": "USDC",
            "category": "erc20",
            "rawContract": {
                "value": "0x05f5e100",
                "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
                "decimal": "0x6",
            },
            "metadata": {
                "blockTimestamp": "2025-01-15T12:30:00.000Z",
            },
        },
        {
            "blockNum": "0x346c7b0",
            "hash": "0xdef456abc789012345678901234567890123456789012345678901234567def0",
            "from": "0x28c6c06298d514db089934071355e5743bf21d60",
            "to": "0x742d35cc6634c0532925a3b844bc9e7595f2bd18",
            "value": 3.0,
            "asset": "USDC",
            "category": "erc20",
            "rawContract": {
                "value": "0x002dc6c0",
                "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
                "decimal": "0x6",
            },
            "metadata": {
                "blockTimestamp": "2025-01-15T14:15:00.000Z",
            },
        },
    ],
}


def _write_fixtures() -> None:
    """Write fixture data to disk for reference/inspection."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    FIXTURE_FILE.write_text(
        json.dumps(_FIXTURE_DATA, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("Fixture data written to %s", FIXTURE_FILE)


async def _run_test_mode() -> None:
    """
    Execute all client methods against fixture data (no network calls).

    Prints the fixture results to stdout so the connector interface can
    be verified in CI without credentials.
    """
    _write_fixtures()

    fixtures = _FIXTURE_DATA
    test_address = fixtures["balance"]["address"]
    test_tx_hash = fixtures["transaction"]["hash"]

    print()
    print("=" * 72)
    print("  Polygon RPC Connector -- TEST MODE (fixture data)")
    print("=" * 72)

    # Block number
    block = fixtures["block_number"]
    print(f"\n  Block number: {block}")

    # MATIC balance
    matic = fixtures["balance"]["matic"]
    print(f"\n  MATIC balance for {test_address}:")
    print(f"    {matic:.6f} MATIC")

    # Token balance
    tb = fixtures["token_balance"]
    print(f"\n  USDC balance for {tb['address']}:")
    print(f"    {tb['balance']:.6f} USDC (contract: {tb['token_contract']})")

    # Transaction details
    tx = fixtures["transaction"]
    print(f"\n  Transaction {test_tx_hash}:")
    print(f"    from:  {tx['from']}")
    print(f"    to:    {tx['to']}")
    print(f"    block: {tx['blockNumber']}")
    print(f"    value: {tx['value']}")

    # Transfer logs
    logs = fixtures["logs"]
    print(f"\n  Transfer logs: {len(logs)} entries")
    for i, log in enumerate(logs):
        print(f"    [{i}] block={log['blockNumber']} tx={log['transactionHash'][:18]}...")

    # Asset transfers (Alchemy-specific)
    transfers = fixtures["asset_transfers"]
    print(f"\n  Asset transfers: {len(transfers)} entries")
    for i, t in enumerate(transfers):
        print(
            f"    [{i}] {t['from'][:10]}...  -> {t['to'][:10]}...  "
            f"{t['value']} {t['asset']}  (block {t['blockNum']})"
        )

    print()
    print("=" * 72)
    print("  All fixture checks passed.")
    print("=" * 72)
    print()


# ---------------------------------------------------------------------------
# Live mode demo
# ---------------------------------------------------------------------------

async def _run_live_mode() -> None:
    """
    Run a live demo that queries the Polygon blockchain via Alchemy.

    Requires ALCHEMY_API_KEY to be set. Performs a few read-only queries
    to verify the connector is working.
    """
    # Import config here to avoid import-time side effects in test mode
    sys.path.insert(0, str(PROJECT_ROOT))
    from execution.utils.config import config

    api_key = config.ALCHEMY_API_KEY
    if not api_key:
        logger.error(
            "ALCHEMY_API_KEY is not set. "
            "Set it in your .env file or run with --test."
        )
        sys.exit(1)

    # Use a well-known address for demo (Polygon bridge)
    demo_address = "0x0000000000000000000000000000000000001010"

    async with PolygonRPCClient(api_key=api_key) as client:
        print()
        print("=" * 72)
        print("  Polygon RPC Connector -- LIVE MODE")
        print("=" * 72)

        # 1. Block number
        block = await client.get_block_number()
        print(f"\n  Current block: {block}")

        # 2. MATIC balance
        balance = await client.get_balance(demo_address)
        print(f"\n  MATIC balance for {demo_address}:")
        print(f"    {balance:.6f} MATIC")

        # 3. Recent logs (last 100 blocks)
        from_block = block - 100
        logs = await client.get_logs(
            contract_address=USDC_POLYGON,
            topics=[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            ],
            from_block=from_block,
            to_block="latest",
        )
        print(f"\n  USDC Transfer logs (blocks {from_block}..latest): {len(logs)} entries")

        print()
        print("=" * 72)
        print("  Live demo complete.")
        print("=" * 72)
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point: parse --test flag and run the appropriate mode."""
    parser = argparse.ArgumentParser(
        description=(
            "Polygon RPC connector via Alchemy. "
            "Queries on-chain data for wallet profiling and IPS scoring."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run with fixture data (no network calls, no API key needed).",
    )
    args = parser.parse_args()

    if args.test:
        logger.info("Running in --test mode: using fixture data.")
        asyncio.run(_run_test_mode())
    else:
        logger.info("Running in live mode.")
        asyncio.run(_run_live_mode())


if __name__ == "__main__":
    main()
