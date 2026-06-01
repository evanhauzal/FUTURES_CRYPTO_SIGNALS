"""
src/context/market_context.py
Fetches global market context (BTC & ETH) using the FREE CoinGecko API.
Includes a 60-second caching mechanism to avoid HTTP 429 Rate Limits.
"""

from __future__ import annotations
import urllib.request
import json
import time
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

def _neutral_context() -> dict:
    return {
        "timestamp": datetime.now(),
        "btc_price": 0.0,
        "btc_change_24h": 0.0,
        "eth_price": 0.0,
        "eth_change_24h": 0.0,
        "market_trend": "NEUTRAL"
    }

class MarketContextFetcher:
    def __init__(self) -> None:
        self.url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_24hr_change=true"
        # Caching layer variables
        self._last_fetched_time = 0.0
        self._cached_data = _neutral_context()
        self._cache_duration = 60.0  # Hanya hit API sekali setiap 60 detik

    def fetch(self) -> dict:
        current_time = time.time()
        
        # Jika belum lewat 60 detik, gunakan data lama yang ada di memori
        if current_time - self._last_fetched_time < self._cache_duration:
            # Perbarui timestamp agar sesuai dengan waktu insert saat ini
            self._cached_data["timestamp"] = datetime.now()
            return self._cached_data

        try:
            req = urllib.request.Request(
                self.url, 
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
            
            btc_data = data.get("bitcoin", {})
            eth_data = data.get("ethereum", {})

            btc_price = float(btc_data.get("usd", 0.0))
            btc_change = float(btc_data.get("usd_24h_change", 0.0))
            eth_price = float(eth_data.get("usd", 0.0))
            eth_change = float(eth_data.get("usd_24h_change", 0.0))

            avg_change = (btc_change + eth_change) / 2

            if avg_change > 1.5:
                market_trend = "BULLISH"
            elif avg_change < -1.5:
                market_trend = "BEARISH"
            else:
                market_trend = "NEUTRAL"

            # Simpan hasil terbaru ke cache
            self._cached_data = {
                "timestamp": datetime.now(),
                "btc_price": btc_price,
                "btc_change_24h": btc_change,
                "eth_price": eth_price,
                "eth_change_24h": eth_change,
                "market_trend": market_trend
            }
            self._last_fetched_time = current_time
            logger.info("Market context successfully refreshed from CoinGecko")
            return self._cached_data

        except Exception as exc:
            logger.error("Failed to fetch market context from CoinGecko (Using Fallback): %s", exc)
            # Kembalikan data netral namun dengan objek datetime yang valid agar DB tidak crash
            return _neutral_context()