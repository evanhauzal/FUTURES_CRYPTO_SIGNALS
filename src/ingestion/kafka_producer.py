"""
Kafka Producer untuk pipeline data crypto.

Script ini bertugas:
1. Mengecek timestamp data terakhir di Cassandra untuk setiap token.
2. Jika data tertinggal (> 2 jam), menarik data baru dari yfinance.
3. Mempublish data baru ke topik Kafka 'crypto_signals' dalam format JSON.

Jika data sudah terbaru, script tidak melakukan apa-apa.
"""

import os
import json
import time
from datetime import datetime, timedelta

import pandas as pd
from cassandra.cluster import Cluster
from kafka import KafkaProducer

from src.ingestion.yahoo_loader import YahooDataLoader


# Konfigurasi — single node (localhost)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
KAFKA_TOPIC = "crypto_signals"
TOKENS = ["BTC", "ETH", "SOL", "XRP", "BNB"]
STALENESS_THRESHOLD_HOURS = 0


def get_latest_datetime_from_cassandra(session, token: str):
    """
    Query Cassandra untuk mendapatkan waktu data terakhir dari token tertentu.
    Mengembalikan datetime object atau None jika belum ada data.
    """
    query = f'SELECT MAX(datetime) AS latest_dt FROM signals WHERE "token" = \'{token}\''
    try:
        result = session.execute(query).one()
        if result and result.latest_dt:
            return result.latest_dt
    except Exception as e:
        print(f"[!] Gagal query Cassandra untuk {token}: {e}")
    return None


def publish_to_kafka(producer, token: str, df: pd.DataFrame):
    """
    Mengirim setiap baris DataFrame ke topik Kafka dalam format JSON.
    Key = nama token, Value = JSON data OHLCV per candle.
    """
    count = 0
    for _, row in df.iterrows():
        message = {
            "token": token,
            "Datetime": str(row["Datetime"]),
            "Open": float(row["Open"]),
            "High": float(row["High"]),
            "Low": float(row["Low"]),
            "Close": float(row["Close"]),
            "Volume": float(row["Volume"])
        }
        producer.send(
            KAFKA_TOPIC,
            key=token.encode("utf-8"),
            value=json.dumps(message).encode("utf-8")
        )
        count += 1

    producer.flush()
    return count


def run_kafka_producer():
    print("\n" + "=" * 60)
    print("[*] KAFKA PRODUCER: SINKRONISASI DATA CRYPTO KE KAFKA")
    print("=" * 60)

    # --- Koneksi Cassandra ---
    print(f"[*] Menghubungkan ke Cassandra ({CASSANDRA_HOST}:{CASSANDRA_PORT})...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect("crypto_ks")

    # --- Koneksi Kafka ---
    print(f"[*] Menghubungkan ke Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=3
    )

    loader = YahooDataLoader()
    current_time = datetime.utcnow()

    for token in TOKENS:
        print(f"\n--- Token: {token} ---")

        latest_dt = get_latest_datetime_from_cassandra(session, token)

        if latest_dt is None:
            print(f"[!] Tidak ada data {token} di Cassandra. Mengambil data historis 4 tahun...")
            df_new = loader.fetch_historical_data(token, period="4y", interval="1h")
        else:
            # Cassandra mengembalikan datetime aware (UTC), kita perlu bandingkan dgn naive
            if hasattr(latest_dt, 'tzinfo') and latest_dt.tzinfo is not None:
                latest_dt_naive = latest_dt.replace(tzinfo=None)
            else:
                latest_dt_naive = latest_dt

            time_diff = current_time - latest_dt_naive
            hours_behind = time_diff.total_seconds() / 3600

            if hours_behind < 0: # Force fetch always
                pass

            print(f"[*] Mengecek update real-time untuk {token} (terakhir: {latest_dt_naive}). Mengambil data baru...")
            # Ambil data 7 hari terakhir lalu filter yang lebih baru atau sama dengan latest_dt
            df_new = loader.fetch_historical_data(token, period="7d", interval="1h")

        if df_new is None or df_new.empty:
            print(f"[!] Tidak ada data baru untuk {token}.")
            continue

        # Standardisasi kolom Datetime
        df_new["Datetime"] = pd.to_datetime(df_new["Datetime"], errors="coerce").dt.tz_localize(None)
        df_new = df_new.dropna(subset=["Datetime"])

        # Filter data: Update candle terakhir ATAU masukkan candle baru
        if latest_dt is not None:
            if hasattr(latest_dt, 'tzinfo') and latest_dt.tzinfo is not None:
                latest_dt_naive = latest_dt.replace(tzinfo=None)
            else:
                latest_dt_naive = latest_dt
            # Gunakan >= agar candle jam saat ini yang belum ditutup bisa terus di-update (Upsert realtime)
            df_new = df_new[df_new["Datetime"] >= latest_dt_naive]

        if df_new.empty:
            print(f"[+] Tidak ada data baru setelah filter untuk {token}.")
            continue

        # Publish ke Kafka
        num_published = publish_to_kafka(producer, token, df_new)
        print(f"[+] {num_published} baris data {token} berhasil dipublish ke Kafka topik '{KAFKA_TOPIC}'.")

    # --- Cleanup ---
    producer.close()
    cluster.shutdown()
    print("\n[+] Kafka Producer selesai.")


if __name__ == "__main__":
    run_kafka_producer()
