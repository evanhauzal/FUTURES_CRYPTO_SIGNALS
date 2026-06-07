import sys
import subprocess

print("=" * 60)
print("MEMULAI RUN_PIPELINE: SISTEM TRADING FUTURES MULTI-TOKEN (ROBD)")
print("=" * 60)

# 1. Jalankan Sinkronisasi Data Otomatis untuk Semua Token (BTC, ETH, SOL, XRP, BNB)
# Modul ini akan mengecek data lokal dan mengunduh yang terbaru dari yfinance
print("\n[STEP 1/3] Menjalankan Sinkronisasi Data...")
subprocess.run(
    [
        sys.executable,
        "-m",
        "src.ingestion.download_all"  # Menggantikan yahoo_loader tunggal
    ]
)

# 2. Jalankan Proses Training Otomatis dengan Model XGBoost Baru
print("\n[STEP 2/3] Menjalankan Training Model XGBoost...")
subprocess.run(
    [
        sys.executable,
        "-m",
        "src.models.train_model"  # Melatih model untuk semua token
    ]
)

# 3. Jalankan Scanner untuk Menghasilkan Sinyal Trading Teranyar
print("\n[STEP 3/3] Menjalankan Scanner Pembuat Sinyal Trading...")
subprocess.run(
    [
        sys.executable,
        "-m",
        "src.signals.generator"  # Menghasilkan output sinyal Long, TP, dan SL
    ]
)

print("\n" + "=" * 60)
print("PIPELINE SELESAI DIEKSEKUSI OLEH SUBPROCESS")
print("=" * 60)