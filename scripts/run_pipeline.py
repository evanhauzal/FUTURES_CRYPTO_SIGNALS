"""
scripts/run_pipeline.py
Entry-point — runs the full trading signal pipeline with Apache Kafka integration.

Usage:
    python3 scripts/run_pipeline.py

Ctrl-C to stop gracefully.
"""

from __future__ import annotations

import asyncio
import sys
import os
import json

# ── make sure the project root is on sys.path ─────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from datetime import datetime

from config.settings import (
    WINDOW_SIZE,
    PEPE_ADDRESS,
    SIGNAL_CONFIRMATION_WINDOW,
)
from src.ingestion.websocket_client import trade_stream
from src.processing.window_processor import WindowProcessor
from src.features.feature_builder import FeatureBuilder
from src.signals.signal_generator import SignalGenerator
from src.signals.signal_confirmation import SignalConfirmation
from src.context.market_context import MarketContextFetcher
from src.storage.postgres_client import PostgresClient
from src.utils.logger import get_logger

# Import Apache Kafka
from kafka import KafkaProducer

logger = get_logger("pipeline")

# ─── ANSI colour helpers ──────────────────────────────────────────────────────
_GREEN  = "\033[92m"
_RED    = "\033[91m"
_YELLOW = "\033[93m"
_CYAN   = "\033[96m"
_MAGENTA = "\033[95m"
_RESET  = "\033[0m"

_SIGNAL_COLOUR = {
    "BULLISH": _GREEN,
    "BEARISH": _RED,
    "NEUTRAL": _YELLOW,
}
_TREND_COLOUR = {
    "UP":       _GREEN,
    "DOWN":     _RED,
    "SIDEWAYS": _YELLOW,
}
_FINAL_COLOUR = {
    "BUY":  _GREEN,
    "SELL": _RED,
    "HOLD": _YELLOW,
}
_MARKET_TREND_COLOUR = {
    "BULLISH": _GREEN,
    "BEARISH": _RED,
    "NEUTRAL": _YELLOW,
}

# Helper serializer untuk datetime objek agar bisa diubah menjadi format JSON
def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _print_prediction_output(pred_data: dict) -> None:
    """Mencetak hasil kalkulasi Predictive Time-Series Engine ke terminal."""
    if not pred_data:
        return
    print(f"{_MAGENTA}[PREDICTIVE ENGINE ACTIVE]{_RESET}")
    print(f" -> Rata-rata Harga Saat Ini : ${pred_data['harga_saat_ini']:.8f}")
    print(f" -> Arus Kas Bandar (Net)   : ${pred_data['arus_kas_bandar_usd']:,.2f}")
    print(f" -> PROYEKSI TREN MASA DEPAN : {pred_data['proyeksi_tren']}")
    print(f" -> PREDIKSI HARGA NEXT      : {_MAGENTA}${pred_data['prediksi_harga_ke_depan']:.8f}{_RESET}")
    print(f"{_CYAN}{'-' * 60}{_RESET}")


def _print_enhanced_signal(
    trade:     dict,
    features:  dict,
    signal:    dict,
    context:   dict,
    confirmed: str,
    final:     str,
) -> None:
    """Multi-line enhanced output including market context and final decision."""
    ts         = signal["timestamp"].strftime("%H:%M:%S")
    pepe_trend = signal["pepe_trend"]
    raw_sig    = signal["signal"]
    mkt_trend  = context.get("market_trend", "NEUTRAL")

    # Ambil data harga BTC dan ETH dari context
    btc_p      = context.get("btc_price", 0.0)
    btc_c      = context.get("btc_change_24h", 0.0)
    eth_p      = context.get("eth_price", 0.0)
    eth_c      = context.get("eth_change_24h", 0.0)

    trade_usd    = trade.get("usd_value", 0.0)
    trade_tokens = trade.get("volume", 0.0)
    trade_type   = trade.get("type", "UNKNOWN")
    whale        = "\U0001f40b WHALE DETECTED" if features["whale_flag"] else ""

    tc  = _TREND_COLOUR.get(pepe_trend, _RESET)
    sc  = _SIGNAL_COLOUR.get(raw_sig, _RESET)
    mtc = _MARKET_TREND_COLOUR.get(mkt_trend, _RESET)
    fc  = _FINAL_COLOUR.get(final, _RESET)
    cc  = _FINAL_COLOUR.get(confirmed, _RESET)

    sep = f"{_CYAN}{'-' * 60}{_RESET}"
    print(sep)
    print(
        f"{_CYAN}[{ts}]{_RESET} "
        f"PEPE {tc}{trade_type:<4}{_RESET} "
        f"{trade_tokens:>16,.0f} tokens "
        f"(${trade_usd:,.2f}) [DATA STREAMING TO KAFKA]"
    )
    print(f"RAW SIGNAL      : {sc}{raw_sig:<8}{_RESET}")
    print(f"TREND           : {tc}{pepe_trend:<8}{_RESET}")
    print(f"MARKET CONTEXT  : {mtc}{mkt_trend:<8}{_RESET} "
          f"(BTC: ${btc_p:,.2f} [{btc_c:+.2f}%] | ETH: ${eth_p:,.2f} [{eth_c:+.2f}%])")
    print(f"CONFIRMED       : {cc}{confirmed:<8}{_RESET}")
    print(f"FINAL DECISION  : {fc}{final:<8}{_RESET}")
    if whale:
        print(f"{_YELLOW}{whale}{_RESET}")
    print(sep)


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline coroutine
# ─────────────────────────────────────────────────────────────────────────────

async def run() -> None:
    db          = PostgresClient()
    window      = WindowProcessor(size=WINDOW_SIZE)
    builder     = FeatureBuilder()
    signals     = SignalGenerator()
    ctx_fetcher = MarketContextFetcher()
    confirmation = SignalConfirmation(window=SIGNAL_CONFIRMATION_WINDOW)

    # ── Initialize Kafka Producer ──────────────────────────────────────────────
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
        )
        logger.info("Kafka Producer successfully connected to localhost:9092")
    except Exception as exc:
        logger.critical("Cannot start pipeline without Kafka connection: %s", exc)
        sys.exit(1)

    # ── connect to PostgreSQL ──────────────────────────────────────────────────
    try:
        db.connect()
    except Exception as exc:
        logger.critical("Cannot start pipeline without a DB connection: %s", exc)
        sys.exit(1)

    logger.info(
        "Pipeline started — window size: %d trades | Streaming to Kafka Topic: trading-pepe",
        WINDOW_SIZE,
    )

    try:
        async for trade in trade_stream():
            
            # ── 1. Ingest & Stream Data to Kafka (Big Data Engine Entry-Point) ──
            try:
                producer.send('trading-pepe', trade)
                producer.flush() 
            except Exception as exc:
                logger.error("Kafka Streaming failed: %s", exc)

            # ── 2. Persist raw trade to Local Database ──────────────────────────
            try:
                db.insert_transaction(trade)
            except Exception as exc:
                logger.error("Failed to insert transaction: %s", exc)
                continue

            # ── 3. Update sliding window ───────────────────────────────────
            window.add(trade)

            if not window.ready:
                logger.debug(
                    "Filling window … %d/%d", len(window), window.size
                )
                continue

            # === SUNTIKAN PREDICTIVE TIME-SERIES ENGINE DI SINI ===
            # === SUNTIKAN PREDICTIVE TIME-SERIES ENGINE ===
            try:
                prediction_res = window.calculate_time_series_prediction()
                _print_prediction_output(prediction_res)
                
                # BARIS INI YANG HARUS KAMU TAMBAHKAN:
                db.insert_prediction(prediction_res)
                
            except Exception as exc:
                logger.error("Predictive Time-Series calculation failed: %s", exc)
            # ======================================================

            # ── 4. Compute features ────────────────────────────────────────
            try:
                feat = builder.build(window.get_window())
            except Exception as exc:
                logger.error("Feature engineering failed: %s", exc)
                continue

            # ── 5. Persist features ────────────────────────────────────────
            try:
                db.insert_features(feat)
            except Exception as exc:
                logger.error("Failed to insert features: %s", exc)

            # ── 6. Generate signal ─────────────────────────────────────────
            try:
                sig = signals.generate(feat)
            except Exception as exc:
                logger.error("Signal generation failed: %s", exc)
                continue

            # ── 7. Persist signal ──────────────────────────────────────────
            try:
                db.insert_signal(sig)
            except Exception as exc:
                logger.error("Failed to insert signal: %s", exc)

            # ── 8. Fetch market context (sync call → thread executor) ───────
            loop = asyncio.get_event_loop()
            try:
                ctx = await loop.run_in_executor(None, ctx_fetcher.fetch)
            except Exception as exc:
                logger.error("Market context fetch failed: %s", exc)
                from src.context.market_context import _neutral_context
                ctx = _neutral_context()

            # ── 9. Persist market context ──────────────────────────────────
            try:
                db.insert_market_context(ctx)
            except Exception as exc:
                logger.error("Failed to insert market context: %s", exc)

            # ── 10. Fetch last N signals for majority voting ────────────────
            last_signals: list = []
            try:
                last_signals = db.get_last_n_signals(n=SIGNAL_CONFIRMATION_WINDOW)
            except Exception as exc:
                logger.error("Failed to fetch last signals: %s", exc)

            # ── 11. Signal confirmation (majority voting) ──────────────────
            confirmed = confirmation.confirm(last_signals)

            # ── 12. Context validation → final decision ────────────────────
            mkt_trend = ctx.get("market_trend", "NEUTRAL")
            final     = confirmation.validate(confirmed, mkt_trend)

            # ── 13. Back-fill confirmation data on the signals row ─────────
            try:
                if last_signals:
                    latest_id = last_signals[-1]["id"]
                    db.update_signal_confirmation(
                        signal_id        = latest_id,
                        confirmed_signal = confirmed,
                        market_context   = mkt_trend,
                        final_decision   = final,
                    )
            except Exception as exc:
                logger.error("Failed to update signal confirmation: %s", exc)

            # ── 14. Enhanced terminal output ───────────────────────────────
            _print_enhanced_signal(trade, feat, sig, ctx, confirmed, final)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        db.close()
        producer.close()
        logger.info("Pipeline stopped.")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()