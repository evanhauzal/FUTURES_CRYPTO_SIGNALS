"""
DAG: Pipeline Berita Crypto
Jadwal: Setiap 5 menit (*/5 * * * *)

Mengorkestrasi pipeline berita pasar:
1. Fetch News — Ambil berita makro kripto terbaru dari NewsAPI
2. Save & Notify — Simpan ke database dan kirim notifikasi Telegram
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
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ============================================================
# Definisi DAG
# ============================================================
with DAG(
    dag_id="crypto_news_pipeline",
    default_args=default_args,
    description="Pipeline berita: Fetch dari NewsAPI → Simpan ke DB → Kirim ke Telegram",
    schedule_interval="*/5 * * * *",  # Setiap 5 menit
    start_date=datetime(2026, 6, 23),
    catchup=False,
    tags=["crypto", "news", "production"],
) as dag:

    # ----------------------------------------------------------
    # TASK 1: Jalankan News Pipeline (1 siklus)
    # Mengambil 10 berita terbaru dari NewsAPI, menyimpan ke
    # tabel v_market_news di PostgreSQL (Supabase), dan mengirim
    # notifikasi ke Telegram untuk berita baru.
    # ----------------------------------------------------------
    fetch_and_send_news = BashOperator(
        task_id="fetch_and_send_news",
        bash_command="cd /opt/airflow && python -m src.ingestion.run_news_pipeline --once",
        execution_timeout=timedelta(minutes=5),
    )
