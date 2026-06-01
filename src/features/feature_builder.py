"""
src/features/feature_builder.py
Derives trading features from a window of raw trades.
"""

from __future__ import annotations

from typing import Any, Dict, List

from config.settings import WHALE_THRESHOLD
from src.utils.logger import get_logger

logger = get_logger(__name__)


class FeatureBuilder:
    """
    Computes a feature vector from a list of trade dicts.

    Input trade schema:
        {
            "type":      "BUY" | "SELL",
            "volume":    float,   # jumlah token PEPE
            "price":     float,   # harga per token (USD)
            "usd_value": float,   # total nilai transaksi dalam USD
            "timestamp": datetime,
        }

    Output feature schema:
        {
            "timestamp":      datetime,
            "buy_volume":     float,   # total BUY dalam USD
            "sell_volume":    float,   # total SELL dalam USD
            "net_volume":     float,   # buy - sell (USD)
            "trade_count":    int,
            "avg_trade_size": float,   # rata-rata transaksi (USD)
            "max_trade_size": float,   # transaksi terbesar (USD)
            "whale_flag":     bool,
        }
    """

    def __init__(self, whale_threshold: float = WHALE_THRESHOLD) -> None:
        # Threshold whale dalam USD
        # Contoh: WHALE_THRESHOLD = 100000 artinya whale jika ada transaksi >= $100,000
        self.whale_threshold = whale_threshold

    def build(self, window: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not window:
            raise ValueError("Cannot build features from an empty window.")

        # =========================
        # Gunakan usd_value, bukan volume token
        # =========================
        buy_usd = sum(
            t.get("usd_value", t["volume"] * t["price"])
            for t in window
            if t["type"] == "BUY"
        )

        sell_usd = sum(
            t.get("usd_value", t["volume"] * t["price"])
            for t in window
            if t["type"] == "SELL"
        )

        usd_values = [
            t.get("usd_value", t["volume"] * t["price"])
            for t in window
        ]

        count = len(window)

        max_trade_usd = max(usd_values) if usd_values else 0.0
        avg_trade_usd = sum(usd_values) / count if count else 0.0

        features: Dict[str, Any] = {
            "timestamp": window[-1]["timestamp"],

            # Total volume BUY/SELL dalam USD
            "buy_volume": round(buy_usd, 2),
            "sell_volume": round(sell_usd, 2),
            "net_volume": round(buy_usd - sell_usd, 2),

            "trade_count": count,

            # Statistik ukuran trade dalam USD
            "avg_trade_size": round(avg_trade_usd, 2),
            "max_trade_size": round(max_trade_usd, 2),

            # Whale jika transaksi terbesar melebihi threshold USD
            "whale_flag": max_trade_usd >= self.whale_threshold,
        }

        logger.debug(
            "Features built — BUY=$%.2f SELL=$%.2f NET=$%.2f "
            "MAX_TRADE=$%.2f THRESHOLD=$%.2f WHALE=%s",
            features["buy_volume"],
            features["sell_volume"],
            features["net_volume"],
            features["max_trade_size"],
            self.whale_threshold,
            features["whale_flag"],
        )

        return features