import time
import psycopg2
import pandas as pd

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="rosbd_trading_db",
        user="postgres",
        password="Naya110212"
    )

def run_ml_model_simulation():
    print("\n" + "="*60)
    print("[*] ENGINE MODEL ML: EVALUASI SINYAL (XGBOOST/RF/LIGHTGBM) ACTIVE")
    print("="*60)
    
    try:
        while True:
            print(f"\n[*] Model memindai database pada: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Ambil data harga paling terakhir dari masing-masing token yang masuk dari yfinance
            query = """
                SELECT DISTINCT ON (token) id, token, price 
                FROM v_crypto_signals 
                ORDER BY token, created_at DESC;
            """
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                for idx, row in df.iterrows():
                    row_id = int(row['id'])
                    token = row['token']
                    price = float(row['price'])
                    
                    # -------------------------------------------------------------
                    # LOGIKA SIMULASI MODEL ML (Berbasis Angka Terakhir Harga)
                    # Ini mensimulasikan keputusan model setelah membaca fitur pasar
                    # -------------------------------------------------------------
                    # Contoh: Jika digit terakhir harga genap, model prediksi LONG
                    if int(price * 100) % 2 == 0:
                        status = "LONG"
                        probability = round(51.5 + (price % 5), 2)
                        if probability > 100 or probability < 0:
                            probability = 54.25
                        tp = round(price * 1.02, 4)
                        sl = round(price * 0.99, 4)
                    else:
                        status = "Wait & See"
                        probability = round(42.1 + (price % 3), 2)
                        tp = None
                        sl = None
                    
                    # Perbarui baris data harga mentah tadi dengan hasil prediksi model ML
                    update_query = """
                        UPDATE v_crypto_signals 
                        SET probability = %s, signal_status = %s, take_profit = %s, stop_loss = %s
                        WHERE id = %s;
                    """
                    cursor.execute(update_query, (probability, status, tp, sl, row_id))
                
                conn.commit()
                print("[+] Model ML berhasil mengevaluasi dan memperbarui sinyal pasar.")
            else:
                print("[!] Belum ada data harga mentah di database untuk dievaluasi.")
                
            cursor.close()
            conn.close()
            
            # Jeda 10 detik agar model memproses tepat setelah harga baru masuk
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n[!] Engine Model ML dihentikan secara manual.")

if __name__ == "__main__":
    run_ml_model_simulation()