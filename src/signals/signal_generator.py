"""
src/signals/signal_generator.py
Converts feature vectors into directional trading signals.

Signal logic
─────────────
• BULLISH  : net_volume > 0  AND  (buy_volume / total_volume) > 0.55
• BEARISH  : net_volume < 0  AND  (sell_volume / total_volume) > 0.55
• NEUTRAL  : everything else

PEPE trend
──────────
• UP       : net_volume > 0
• DOWN     : net_volume < 0
• SIDEWAYS : net_volume == 0

Whale amplification: a whale_flag bumps the threshold from 0.55 → 0.50
so large single trades can push the signal through sooner.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from src.utils.logger import get_logger

logger = get_logger(__name__)

_BULL_THRESHOLD   = 0.55
_BEAR_THRESHOLD   = 0.55
_WHALE_BOOST      = 0.05   # reduce threshold by this when whale detected


class SignalGenerator:

    def generate(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parameters
        ----------
        features : dict
            Output of FeatureBuilder.build()

        Returns
        -------
        dict with keys: timestamp, signal, pepe_trend
        """
        buy_vol  = features["buy_volume"]
        sell_vol = features["sell_volume"]
        net_vol  = features["net_volume"]
        whale    = features["whale_flag"]
        ts       = features["timestamp"]

        total = buy_vol + sell_vol or 1e-9  # guard /0

        bull_thr = _BULL_THRESHOLD - (_WHALE_BOOST if whale else 0)
        bear_thr = _BEAR_THRESHOLD - (_WHALE_BOOST if whale else 0)

        buy_ratio  = buy_vol  / total
        sell_ratio = sell_vol / total

        # ── signal ─────────────────────────────────────────────────────────
        if net_vol > 0 and buy_ratio >= bull_thr:
            signal = "BULLISH"
        elif net_vol < 0 and sell_ratio >= bear_thr:
            signal = "BEARISH"
        else:
            signal = "NEUTRAL"

        # ── PEPE trend ─────────────────────────────────────────────────────
        if net_vol > 0:
            pepe_trend = "UP"
        elif net_vol < 0:
            pepe_trend = "DOWN"
        else:
            pepe_trend = "SIDEWAYS"

        result = {
            "timestamp":  ts,
            "signal":     signal,
            "pepe_trend": pepe_trend,
        }

        logger.debug(
            "Signal generated — %s | trend: %s | net_vol: %.4f | whale: %s",
            signal, pepe_trend, net_vol, whale,
        )
        return result
