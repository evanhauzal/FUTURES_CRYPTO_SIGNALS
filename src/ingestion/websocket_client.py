"""
src/ingestion/websocket_client.py
Real-time PEPE trade ingestion via the Birdeye REST API.

Strategy
────────
  • Poll  GET /defi/txs/token  every POLL_INTERVAL seconds (async, aiohttp).
  • Deduplicate via a rolling set of seen txHash values.
  • On HTTP / network error → exponential backoff up to MAX_BACKOFF seconds.
  • If BIRDEYE_API_KEY is missing → fall back to synthetic data so the rest
    of the pipeline still works for local dev / testing.

Environment variables (loaded via python-dotenv from .env):
    BIRDEYE_API_KEY   – required for live data
    BIRDEYE_CHAIN     – default: "ethereum"   (PEPE is an ERC-20 token)
    PEPE_ADDRESS      – override the token address if needed
    POLL_INTERVAL     – seconds between polls  (default: 2)
    MAX_BACKOFF       – max retry wait seconds (default: 60)

Output trade dict (compatible with the existing pipeline):
    {
        "type":      "BUY" | "SELL",
        "volume":    float,   # PEPE tokens
        "price":     float,   # USD price per PEPE
        "timestamp": datetime,
        "tx_hash":   str,
    }
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Any, Set

# ── optional async HTTP client ────────────────────────────────────────────────
try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    _AIOHTTP_AVAILABLE = False

from config.settings import (
    BIRDEYE_API_KEY,
    BIRDEYE_CHAIN,
    PEPE_ADDRESS,
    POLL_INTERVAL,
    MAX_BACKOFF,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
_BASE_URL  = "https://public-api.birdeye.so"
_ENDPOINT  = "/defi/txs/token"
_PAGE_SIZE = 50                     # max items per request
_SEEN_CACHE_MAX = 5_000             # cap the dedup set size


# ─────────────────────────────────────────────────────────────────────────────
# Response normaliser
# ─────────────────────────────────────────────────────────────────────────────
def _normalise(item: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Convert one Birdeye /defi/txs/token item into a pipeline trade dict.

    Birdeye item schema (relevant fields):
        txHash          str
        blockUnixTime   int   (seconds)
        side            str   "buy" | "sell"   ← primary field
        type            str   "buy" | "sell"   ← fallback
        from / to       obj   { symbol, amount, uiAmount, ... }
        volumeUsd       float

    base   = the token being traded    (PEPE)
    quote  = the counter token         (USDC / ETH / …)
    """
    try:
        tx_hash = item.get("txHash") or item.get("signature", "")

        # ── timestamp ─────────────────────────────────────────────────────────
        unix_time = item.get("blockUnixTime") or item.get("blockTime", 0)
        ts = datetime.fromtimestamp(unix_time, tz=timezone.utc).replace(tzinfo=None)

        # ── side ──────────────────────────────────────────────────────────────
        raw_side = (
            item.get("side")           # preferred
            or item.get("type")        # fallback
            or ""
        ).lower()
        if raw_side in ("buy", "b"):
            trade_type = "BUY"
        elif raw_side in ("sell", "s"):
            trade_type = "SELL"
        else:
            # derive from token flow: if 'from' symbol is the base token → SELL
            from_sym = (item.get("from") or {}).get("symbol", "")
            to_sym   = (item.get("to")   or {}).get("symbol", "")
            if "PEPE" in from_sym.upper():
                trade_type = "SELL"
            elif "PEPE" in to_sym.upper():
                trade_type = "BUY"
            else:
                logger.debug("Cannot determine trade side for tx %s — skipping", tx_hash)
                return None

        # ── volume (PEPE amount) ───────────────────────────────────────────────
        # Try structured fields first, then top-level volumeUsd / amount
        base = item.get("base") or {}
        frm  = item.get("from") or {}
        to   = item.get("to")   or {}

        volume = (
            base.get("uiAmount")
            or base.get("amount")
            or (frm.get("uiAmount") if "PEPE" in frm.get("symbol", "").upper() else None)
            or (to.get("uiAmount")  if "PEPE" in to.get("symbol",  "").upper() else None)
            or item.get("amount")
            or 0.0
        )

        volume = float(volume)

        # Reject invalid or noisy trades
        if volume <= 0:
            logger.debug("Invalid PEPE volume for tx %s — skipping", tx_hash)
            return None

        # Optional noise filter (skip tiny trades)
        if volume < 1000:
            logger.debug("Tiny PEPE trade for tx %s — skipping", tx_hash)
            return None

        # ── price (USD per PEPE) ───────────────────────────────────────────────
        price = (
            base.get("price")
            or item.get("price")
            or item.get("priceUsd")
            or 0.0
        )
        # derive from volumeUsd / volume when price is missing
        if not price and volume:
            price = float(item.get("volumeUsd") or 0.0) / volume

        price = float(price)

        if volume <= 0:
            logger.debug("Zero-volume tx %s — skipping", tx_hash)
            return None

        usd_value = volume * price

        return {
            "type":      trade_type,
            "volume":    round(volume, 6),
            "price":     round(price, 10),
            "usd_value": round(usd_value, 2),
            "timestamp": ts,
            "tx_hash":   tx_hash,
                                }
    except Exception as exc:
        logger.warning("Failed to normalise tx: %s — %s", item.get("txHash", "?"), exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Birdeye polling
# ─────────────────────────────────────────────────────────────────────────────
async def _fetch_trades(
    session: "aiohttp.ClientSession",
    seen: Set[str],
) -> list[Dict[str, Any]]:
    """
    Fetch the latest PEPE trades from Birdeye.
    Returns normalised trade dicts that haven't been seen before.
    """
    url    = f"{_BASE_URL}{_ENDPOINT}"
    params = {
        "address":   PEPE_ADDRESS,
        "tx_type":   "swap",
        "sort_type": "desc",       # newest first
        "offset":    0,
        "limit":     _PAGE_SIZE,
    }
    headers = {
        "x-api-key": BIRDEYE_API_KEY,
        "x-chain":   BIRDEYE_CHAIN,
        "accept":    "application/json",
    }

    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        if resp.status == 429:
            raise RuntimeError(f"Rate limited (HTTP 429). Retry after back-off.")
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")

        data = await resp.json()

    if not data.get("success"):
        raise RuntimeError(f"Birdeye API error: {data.get('message', data)}")

    items = (data.get("data") or {}).get("items") or []
    logger.info("Birdeye: fetched %d raw transactions for PEPE", len(items))

    new_trades: list[Dict[str, Any]] = []
    for item in items:
        tx_hash = item.get("txHash") or item.get("signature", "")
        if tx_hash in seen:
            continue                      # already processed

        trade = _normalise(item)
        if trade is None:
            continue

        seen.add(tx_hash)
        new_trades.append(trade)

    # Trim cache to avoid unbounded growth
    if len(seen) > _SEEN_CACHE_MAX:
        # Remove oldest entries (set is unordered — just discard some)
        overflow = len(seen) - _SEEN_CACHE_MAX
        to_remove = list(seen)[:overflow]
        for k in to_remove:
            seen.discard(k)

    # Return in chronological order (oldest first → natural pipeline order)
    new_trades.sort(key=lambda t: t["timestamp"])
    return new_trades


# ─────────────────────────────────────────────────────────────────────────────
# Async generator — primary export
# ─────────────────────────────────────────────────────────────────────────────
async def trade_stream() -> AsyncIterator[Dict[str, Any]]:
    """
    Yields normalised PEPE trade dicts indefinitely.

    Live path  : polls Birdeye REST API (requires BIRDEYE_API_KEY + aiohttp).
    Fallback   : synthetic data when API key is absent or aiohttp not installed.
    """
    if not BIRDEYE_API_KEY:
        logger.warning(
            "BIRDEYE_API_KEY not set — falling back to synthetic PEPE data."
        )
        async for trade in _synthetic_stream():
            yield trade
        return

    if not _AIOHTTP_AVAILABLE:
        logger.warning(
            "aiohttp not installed — falling back to synthetic PEPE data. "
            "Install with: pip install aiohttp"
        )
        async for trade in _synthetic_stream():
            yield trade
        return

    logger.info(
        "Starting Birdeye polling for PEPE (%s) on %s every %ss",
        PEPE_ADDRESS, BIRDEYE_CHAIN, POLL_INTERVAL,
    )

    seen: Set[str] = set()
    backoff = 1.0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                trades = await _fetch_trades(session, seen)

                if trades:
                    logger.info("Yielding %d new PEPE trades", len(trades))
                    backoff = 1.0          # reset on success
                else:
                    logger.debug("No new PEPE trades in this poll cycle.")

                for trade in trades:
                    ts_str = trade["timestamp"].strftime("%H:%M:%S")
                    logger.info(
                        "[%s] PEPE %s  %.0f tokens | $%.2f USD | price=%.10f",
                        ts_str,
                        trade["type"],
                        trade["volume"],
                        trade["usd_value"],
                        trade["price"],
                    )
                    yield trade

                await asyncio.sleep(POLL_INTERVAL)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.error(
                    "Birdeye fetch error: %s — retrying in %.0fs", exc, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)   # exponential back-off


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic fallback
# ─────────────────────────────────────────────────────────────────────────────
def _synthetic_trade() -> Dict[str, Any]:
    """Realistic-looking synthetic PEPE trade for offline development."""
    price  = random.uniform(0.000006, 0.000015)   # PEPE price range (USD)
    volume = random.expovariate(1 / 5_000_000)    # most trades small
    volume = min(volume, 500_000_000)             # cap at 500 M PEPE
    return {
        "type":      random.choice(["BUY", "SELL"]),
        "volume":    round(volume, 2),
        "price":     round(price,  10),
        "timestamp": datetime.utcnow(),
        "tx_hash":   f"synthetic-{random.randint(10**15, 10**16)}",
    }


async def _synthetic_stream() -> AsyncIterator[Dict[str, Any]]:
    """Yields synthetic PEPE trades at ~1 Hz indefinitely."""
    logger.info("Synthetic PEPE data stream started.")
    while True:
        trade = _synthetic_trade()
        ts_str = trade["timestamp"].strftime("%H:%M:%S")
        logger.info(
            "[%s] PEPE %s  %.0f @ %.10f  [SYNTHETIC]",
            ts_str, trade["type"], trade["volume"], trade["price"],
        )
        yield trade
        await asyncio.sleep(1.0)
