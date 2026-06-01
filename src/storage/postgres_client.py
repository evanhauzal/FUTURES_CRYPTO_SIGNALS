"""
src/storage/postgres_client.py
Thread-safe PostgreSQL client backed by a psycopg2 connection pool.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator

import psycopg2
from psycopg2 import pool, sql

from config.settings import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    DB_MIN_CONN, DB_MAX_CONN,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DDL statements
# ─────────────────────────────────────────────────────────────────────────────
_DDL_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS transactions (
    id         SERIAL PRIMARY KEY,
    type       TEXT      NOT NULL,
    volume     FLOAT     NOT NULL,
    price      FLOAT     NOT NULL,
    usd_value  FLOAT     NOT NULL,
    timestamp  TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

_DDL_FEATURES = """
CREATE TABLE IF NOT EXISTS features (
    id             SERIAL PRIMARY KEY,
    timestamp      TIMESTAMP NOT NULL DEFAULT NOW(),
    buy_volume     FLOAT     NOT NULL,
    sell_volume    FLOAT     NOT NULL,
    net_volume     FLOAT     NOT NULL,
    trade_count    INT       NOT NULL,
    avg_trade_size FLOAT     NOT NULL,
    max_trade_size FLOAT     NOT NULL,
    whale_flag     BOOLEAN   NOT NULL
);
"""

_DDL_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals (
    id         SERIAL PRIMARY KEY,
    timestamp  TIMESTAMP NOT NULL DEFAULT NOW(),
    signal     TEXT      NOT NULL,
    pepe_trend TEXT      NOT NULL
);
"""

_DDL_MARKET_CONTEXT = """
CREATE TABLE IF NOT EXISTS market_context (
    id             SERIAL PRIMARY KEY,
    timestamp      TIMESTAMP        NOT NULL,
    btc_price      DOUBLE PRECISION,
    btc_change_24h DOUBLE PRECISION,
    eth_price      DOUBLE PRECISION,
    eth_change_24h DOUBLE PRECISION,
    market_cap     DOUBLE PRECISION,
    volume_24h     DOUBLE PRECISION,
    btc_dominance  DOUBLE PRECISION,
    market_trend   TEXT
);
"""

# Tabel baru untuk Prediksi
_DDL_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS predictions (
    id                       SERIAL PRIMARY KEY,
    timestamp                TIMESTAMP NOT NULL,
    harga_saat_ini           DOUBLE PRECISION,
    arus_kas_bandar_usd      DOUBLE PRECISION,
    prediksi_harga_ke_depan  DOUBLE PRECISION,
    proyeksi_tren            TEXT
);
"""

_DDL_ALTER_SIGNALS = [
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS confirmed_signal TEXT;",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS market_context   TEXT;",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS final_decision   TEXT;",
]

# ─────────────────────────────────────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────────────────────────────────────
class PostgresClient:
    _instance: "PostgresClient | None" = None
    _lock = threading.Lock()

    def __new__(cls) -> "PostgresClient":
        with cls._lock:
            if cls._instance is None:
                obj = super().__new__(cls)
                obj._pool: pool.ThreadedConnectionPool | None = None
                cls._instance = obj
        return cls._instance

    def connect(self) -> None:
        if self._pool is not None: return
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN, host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        )
        self._create_tables()

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            self._pool = None

    @contextmanager
    def _get_conn(self) -> Generator[Any, None, None]:
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def _create_tables(self) -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_DDL_TRANSACTIONS)
                cur.execute(_DDL_FEATURES)
                cur.execute(_DDL_SIGNALS)
                cur.execute(_DDL_MARKET_CONTEXT)
                cur.execute(_DDL_PREDICTIONS)
                for stmt in _DDL_ALTER_SIGNALS:
                    cur.execute(stmt)

    # ── INSERT helpers ─────────────────────────────────────────────────────────
    def insert_transaction(self, trade: Dict[str, Any]) -> None:
        q = "INSERT INTO transactions (type, volume, price, usd_value, timestamp) VALUES (%s, %s, %s, %s, %s)"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (trade["type"], trade["volume"], trade["price"], trade.get("usd_value", trade["volume"]*trade["price"]), trade["timestamp"]))

    def insert_features(self, feat: Dict[str, Any]) -> None:
        q = "INSERT INTO features (timestamp, buy_volume, sell_volume, net_volume, trade_count, avg_trade_size, max_trade_size, whale_flag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (feat["timestamp"], feat["buy_volume"], feat["sell_volume"], feat["net_volume"], feat["trade_count"], feat["avg_trade_size"], feat["max_trade_size"], feat["whale_flag"]))

    def insert_signal(self, sig: Dict[str, Any]) -> None:
        q = "INSERT INTO signals (timestamp, signal, pepe_trend) VALUES (%s, %s, %s)"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (sig["timestamp"], sig["signal"], sig["pepe_trend"]))

    def insert_market_context(self, context: Dict[str, Any]) -> None:
        q = "INSERT INTO market_context (timestamp, btc_price, btc_change_24h, eth_price, eth_change_24h, market_trend) VALUES (%s, %s, %s, %s, %s, %s)"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (context["timestamp"], context.get("btc_price"), context.get("btc_change_24h"), context.get("eth_price"), context.get("eth_change_24h"), context.get("market_trend")))

    # Fungsi Baru: Simpan Prediksi
    def insert_prediction(self, pred: Dict[str, Any]) -> None:
        q = "INSERT INTO predictions (timestamp, harga_saat_ini, arus_kas_bandar_usd, prediksi_harga_ke_depan, proyeksi_tren) VALUES (%s, %s, %s, %s, %s)"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (pred["timestamp"], pred["harga_saat_ini"], pred["arus_kas_bandar_usd"], pred["prediksi_harga_ke_depan"], pred["proyeksi_tren"]))

    def get_last_n_signals(self, n: int = 3) -> list[Dict[str, Any]]:
        q = "SELECT id, timestamp, signal, pepe_trend FROM signals ORDER BY id DESC LIMIT %s"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (n,))
                rows = cur.fetchall()
        return [{"id": r[0], "timestamp": r[1], "signal": r[2], "pepe_trend": r[3]} for r in reversed(rows)]

    def update_signal_confirmation(self, signal_id: int, confirmed_signal: str, market_context: str, final_decision: str) -> None:
        q = "UPDATE signals SET confirmed_signal = %s, market_context = %s, final_decision = %s WHERE id = %s"
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (confirmed_signal, market_context, final_decision, signal_id))