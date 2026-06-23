"""
DAG: Pipeline Harga Crypto
Jadwal: Setiap 1 jam (0 * * * *)

Mengorkestrasi pipeline utama:
1. Kafka Producer — Tarik data harga terbaru dari Yahoo Finance, kirim ke Kafka
2. Tunggu Consumer — Beri jeda 10 detik agar Consumer sempat menulis ke Cassandra
3. Train Model — Latih ulang model XGBoost dengan data terbaru dari Cassandra
4. Scan Signals — Jalankan scanner sinyal trading, kirim hasil ke DB & Telegram
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ============================================================
# Default arguments untuk semua task dalam DAG ini
# ============================================================
default_args = {
    "owner": "rob-sbd",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# ============================================================
# Definisi DAG
# ============================================================
with DAG(
    dag_id="crypto_price_pipeline",
    default_args=default_args,
    description="Pipeline utama: Kafka ingestion → XGBoost training → Signal scanning",
    schedule_interval="0 * * * *",  # Setiap jam tepat
    start_date=datetime(2026, 6, 23),
    catchup=False,
    tags=["crypto", "pipeline", "production"],
) as dag:

    # ----------------------------------------------------------
    # TASK 1: Jalankan Kafka Producer
    # Menarik data OHLCV terbaru dari Yahoo Finance dan
    # mempublish ke topik Kafka 'crypto_signals'
    # ----------------------------------------------------------
    kafka_producer_task = BashOperator(
        task_id="kafka_producer",
        bash_command="cd /opt/airflow && python -m src.ingestion.kafka_producer",
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "{{ var.value.get('kafka_servers', 'kafka:29092') }}",
            "CASSANDRA_HOST": "{{ var.value.get('cassandra_host', 'cassandra') }}",
            "CASSANDRA_PORT": "{{ var.value.get('cassandra_port', '9042') }}",
        },
    )

    # ----------------------------------------------------------
    # TASK 2: Tunggu Consumer memproses data
    # Consumer (kafka_to_cassandra.py) berjalan sebagai service
    # Docker terpisah yang selalu hidup. Task ini hanya memberi
    # jeda waktu agar data sempat masuk ke Cassandra.
    # ----------------------------------------------------------
    wait_for_consumer = BashOperator(
        task_id="wait_for_consumer",
        bash_command="echo '[*] Menunggu 10 detik agar Kafka Consumer selesai menulis ke Cassandra...' && sleep 10",
    )

    # ----------------------------------------------------------
    # TASK 3: Training ulang model XGBoost
    # Membaca data terbaru dari Cassandra, melakukan feature
    # engineering, dan melatih ulang model untuk setiap token.
    # ----------------------------------------------------------
    train_model_task = BashOperator(
        task_id="train_model",
        bash_command="cd /opt/airflow && python -m src.models.train_model",
        env={
            "CASSANDRA_HOST": "{{ var.value.get('cassandra_host', 'cassandra') }}",
            "CASSANDRA_PORT": "{{ var.value.get('cassandra_port', '9042') }}",
        },
        # Training bisa memakan waktu lama
        execution_timeout=timedelta(minutes=30),
    )

    # ----------------------------------------------------------
    # TASK 4: Scan sinyal trading
    # Menjalankan inferensi model terhadap data terkini,
    # menyimpan hasil ke PostgreSQL (Supabase), dan mengirim
    # notifikasi ke Telegram jika ada sinyal.
    # ----------------------------------------------------------
    scan_signals_task = BashOperator(
        task_id="scan_signals",
        bash_command="cd /opt/airflow && python -m src.signals.generator",
        env={
            "CASSANDRA_HOST": "{{ var.value.get('cassandra_host', 'cassandra') }}",
            "CASSANDRA_PORT": "{{ var.value.get('cassandra_port', '9042') }}",
        },
    )

    # ----------------------------------------------------------
    # Dependency chain (urutan eksekusi)
    # ----------------------------------------------------------
    kafka_producer_task >> wait_for_consumer >> train_model_task >> scan_signals_task
