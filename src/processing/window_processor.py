"""
src/processing/window_processor.py
Maintains a sliding window of trades and calculates advanced multi-step time-series projections.
"""

from __future__ import annotations
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List
import numpy as np

from config.settings import WINDOW_SIZE
from src.utils.logger import get_logger

logger = get_logger(__name__)

class WindowProcessor:
    def __init__(self, size: int = WINDOW_SIZE) -> None:
        self.size: int = size
        self._window: Deque[Dict[str, Any]] = deque(maxlen=size)

    def add(self, trade: Dict[str, Any]) -> None:
        """Push a trade into the sliding window."""
        self._window.append(trade)
        if len(self._window) % 10 == 0:
            logger.debug("Window status: %d/%d trades", len(self._window), self.size)

    @property
    def ready(self) -> bool:
        """Checks if enough data is accumulated for processing."""
        return len(self._window) >= self.size

    def get_window(self) -> List[Dict[str, Any]]:
        """Returns the current window snapshot."""
        return list(self._window)

    def clear(self) -> None:
        """Clears the buffer."""
        self._window.clear()

    def calculate_time_series_prediction(self) -> Dict[str, Any]:
        if not self.ready:
            logger.warning("Predictor not ready.")
            return {}

        trades = self.get_window()
        total_price = sum(float(t.get('price', 0)) for t in trades)
        current_avg_price = total_price / len(trades)
        
        net_volume_flow = 0.0
        for t in trades:
            usd_val = float(t.get('usd_value', 0))
            if t.get('type') == 'BUY': net_volume_flow += usd_val
            elif t.get('type') == 'SELL': net_volume_flow -= usd_val

        # Logic Multi-step
        price_impact_factor = 0.00000000001
        future_sequence = []
        last_val = current_avg_price
        for i in range(5):
            # Proyeksi kumulatif
            projection = last_val + (net_volume_flow * price_impact_factor)
            future_sequence.append(round(projection, 8))
            last_val = projection
        
        # Definisi trend_signal agar tidak error
        if net_volume_flow > 0: trend_signal = "UP TREND 📈"
        elif net_volume_flow < 0: trend_signal = "DOWN TREND 📉"
        else: trend_signal = "SIDEWAYS ⏳"
        
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "harga_saat_ini": round(current_avg_price, 8),
            "arus_kas_bandar_usd": round(net_volume_flow, 2),
            "prediksi_harga_ke_depan": future_sequence[0],
            "prediksi_full_sequence": future_sequence,
            "proyeksi_tren": trend_signal
        }
        return prediction_payload

    def __len__(self) -> int:
        return len(self._window)