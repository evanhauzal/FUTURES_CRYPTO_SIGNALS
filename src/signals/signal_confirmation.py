"""
src/signals/signal_confirmation.py
Signal Confirmation Layer — majority voting + context validation.

SignalConfirmation
──────────────────
  confirm(signals) — majority voting from N raw signals
      ≥2 of last 3 = BULLISH → BUY
      ≥2 of last 3 = BEARISH → SELL
      otherwise              → HOLD

  validate(confirmed, market_trend) — Context Validation Layer
      Combines confirmed signal with global market trend to produce
      the final trading decision per the decision table below.

Decision Table
──────────────
  Confirmed │ Market Trend │ Final Decision
  ──────────┼──────────────┼───────────────
  BUY       │ BULLISH      │ BUY
  BUY       │ NEUTRAL      │ HOLD
  BUY       │ BEARISH      │ HOLD
  SELL      │ BEARISH      │ SELL
  SELL      │ NEUTRAL      │ HOLD
  SELL      │ BULLISH      │ HOLD
  HOLD      │ Any          │ HOLD
"""

from __future__ import annotations

from typing import Any, Dict, List

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ─── Decision table ───────────────────────────────────────────────────────────
# (confirmed_signal, market_trend) → final_decision
_DECISION_TABLE: Dict[tuple[str, str], str] = {
    ("BUY",  "BULLISH"): "BUY",
    ("BUY",  "NEUTRAL"): "HOLD",
    ("BUY",  "BEARISH"): "HOLD",
    ("SELL", "BEARISH"): "SELL",
    ("SELL", "NEUTRAL"): "HOLD",
    ("SELL", "BULLISH"): "HOLD",
    ("HOLD", "BULLISH"): "HOLD",
    ("HOLD", "NEUTRAL"): "HOLD",
    ("HOLD", "BEARISH"): "HOLD",
}


class SignalConfirmation:
    """
    Majority-voting signal confirmation and context validation.

    Parameters
    ──────────
    window : int
        Number of most-recent raw signals to consider (default: 3).
    """

    def __init__(self, window: int = 3) -> None:
        if window < 1:
            raise ValueError(f"window must be >= 1, got {window}")
        self.window = window

    # ── Majority voting ───────────────────────────────────────────────────────
    def confirm(self, signals: List[Dict[str, Any]]) -> str:
        """
        Apply majority voting to the last `window` raw signals.

        Parameters
        ──────────
        signals : list of signal dicts (from DB) — each must have key ``signal``
                  with value ``"BULLISH"`` | ``"BEARISH"`` | ``"NEUTRAL"``

        Returns
        ───────
        ``"BUY"``, ``"SELL"``, or ``"HOLD"``
        """
        if not signals:
            logger.warning("confirm() called with empty signal list — defaulting to HOLD.")
            return "HOLD"

        # Use only the last `window` entries (DB query should already limit, but
        # guard here for safety)
        recent = signals[-self.window :]

        bullish_count = sum(1 for s in recent if s.get("signal") == "BULLISH")
        bearish_count = sum(1 for s in recent if s.get("signal") == "BEARISH")
        total         = len(recent)
        threshold     = max(2, (total // 2) + 1)   # majority = >50 %, min 2

        if bullish_count >= threshold:
            confirmed = "BUY"
        elif bearish_count >= threshold:
            confirmed = "SELL"
        else:
            confirmed = "HOLD"

        logger.debug(
            "Signal confirmation — window=%d BULLISH=%d BEARISH=%d → %s",
            total, bullish_count, bearish_count, confirmed,
        )
        return confirmed

    # ── Context validation ────────────────────────────────────────────────────
    def validate(self, confirmed_signal: str, market_trend: str) -> str:
        """
        Apply the Context Validation decision table.

        Parameters
        ──────────
        confirmed_signal : ``"BUY"`` | ``"SELL"`` | ``"HOLD"``
        market_trend     : ``"BULLISH"`` | ``"BEARISH"`` | ``"NEUTRAL"``

        Returns
        ───────
        Final decision: ``"BUY"`` | ``"SELL"`` | ``"HOLD"``
        """
        key = (confirmed_signal.upper(), market_trend.upper())
        final = _DECISION_TABLE.get(key, "HOLD")

        if key not in _DECISION_TABLE:
            logger.warning(
                "Unknown (confirmed=%s, trend=%s) — defaulting to HOLD.",
                confirmed_signal, market_trend,
            )

        logger.debug(
            "Context validation — confirmed=%s market_trend=%s → final=%s",
            confirmed_signal, market_trend, final,
        )
        return final
