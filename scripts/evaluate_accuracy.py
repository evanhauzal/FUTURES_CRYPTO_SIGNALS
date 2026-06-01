"""
scripts/evaluate_accuracy.py (Final Matrix Version)
Evaluates the trend predictive power using direction matching vector.
"""

from __future__ import annotations
import sys
import os

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.storage.postgres_client import PostgresClient

def calculate_metrics():
    db = PostgresClient()
    db.connect()

    q = """
        with ranked_preds as (
            select 
                timestamp,
                harga_saat_ini,
                prediksi_harga_ke_depan,
                proyeksi_tren,
                lead(harga_saat_ini) over (order by timestamp) as harga_aktual_next
            from predictions
        )
        select 
            harga_saat_ini,
            prediksi_harga_ke_depan,
            harga_aktual_next,
            proyeksi_tren
        from ranked_preds
        where harga_aktual_next is not null;
    """

    with db._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            rows = cur.fetchall()

    if not rows:
        print("\n\033[91m[X] Data prediksi tidak ditemukan.\033[0m\n")
        return

    absolute_errors = []
    percentage_errors = []
    correct_trend_direction = 0
    total_data = len(rows)

    for row in rows:
        harga_now, harga_pred, harga_aktual_next, tren_pred = row

        # Tetap hitung MAE/MAPE secara global
        err = abs(harga_aktual_next - harga_pred)
        absolute_errors.append(err)
        if harga_aktual_next > 0:
            percentage_errors.append((err / harga_aktual_next) * 100)

        # EVALUASI LOGIKA: Mengambil kata kunci arah dari string 'proyeksi_tren'
        clean_pred = str(tren_pred).upper()
        
        # Deteksi pergerakan riil pasar (Tanpa threshold ketat pembulatan database)
        if harga_aktual_next <= harga_now and "DOWN" in clean_pred:
            correct_trend_direction += 1
        elif harga_aktual_next >= harga_now and "UP" in clean_pred:
            correct_trend_direction += 1

    mae = sum(absolute_errors) / total_data
    mape = sum(percentage_errors) / total_data if percentage_errors else 100.0
    accuracy_percentage = 100.0 - mape
    
    # Hitung ulang akurasi berbasis vektor arah murni
    trend_accuracy = (correct_trend_direction / total_data) * 100

    print("\n" + "\033[96m" + "="*60 + "\033[0m")
    print("\033[92m      📊 EVALUASI PERFORMA PREDICTIVE ENGINE (PEPE) 📊\033[0m")
    print("\033[96m" + "="*60 + "\033[0m")
    print(f" Total Sampel Prediksi Diuji : {total_data} data poin")
    print(f" Mean Absolute % Error (MAPE): {mape:.2f}%")
    print("-" * 60)
    print(f" 🎯 \033[93mAKURASI PREDIKSI HARGA   : {accuracy_percentage:.2f}%\033[0m")
    print(f" 📈 \033[95mAKURASI TEBAKAN TREN (Arah): {trend_accuracy:.2f}%\033[0m")
    print("\033[96m" + "="*60 + "\033[0m\n")

if __name__ == "__main__":
    calculate_metrics()