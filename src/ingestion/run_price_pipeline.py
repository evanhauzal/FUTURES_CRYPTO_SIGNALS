import os
import time
import datetime
import subprocess
import sys
import yfinance as yf
import pandas as pd
from src.signals.generator import TradingSignalCenter

def append_price_to_local_csv(data_dir: str, token: str, price: float):
    """
    Menambahkan data harga baru dari yfinance ke file CSV lokal 
    agar data fitur yang dibaca oleh model ML selalu diperbarui.
    """
    file_name = f"{token}_1h.csv"
    file_path = os.path.join(data_dir, file_name)
    
    current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    new_data = pd.DataFrame([{
        "Datetime": current_time_str,
        "Open": price,
        "High": price,
        "Low": price,
        "Close": price,
        "Volume": 0
    }])
    
    if os.path.exists(file_path):
        new_data.to_csv(file_path, mode='a', header=False, index=False)
    else:
        new_data.to_csv(file_path, index=False)

def start_price_pipeline():
    print("\n" + "="*60)
    print("[*] BACKEND PIPELINE: FULL EXPERIMENT (LIVE DATA + RETRAIN + SCANNER)")
    print("="*60)
    
    scanner = TradingSignalCenter()
    tokens = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD"]
    
    try:
        while True:
            print(f"\n[*] Siklus eksperimen penuh dimulai pada: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # STEP 1: Ambil data harga live dari yfinance dan perbarui file lokal
            print("\n[STEP 1] Menyinkronkan data live ke file CSV lokal...")
            for ticker in tokens:
                try:
                    crypto = yf.Ticker(ticker)
                    df_latest = crypto.history(period="1d", interval="1m")
                    
                    if not df_latest.empty:
                        last_row = df_latest.iloc[-1]
                        token_name = ticker.split("-")[0]
                        current_price = float(last_row["Close"])
                        
                        append_price_to_local_csv(scanner.data_dir, token_name, current_price)
                        print(f"   [+] Data live {token_name} (${current_price:.2f}) masuk ke CSV.")
                        
                except Exception as e:
                    print(f"   [!] Gagal sinkronisasi data {ticker}: {str(e)}")
            
            # STEP 2: JALANKAN PROSES TRAINING ULANG MODEL (EKSPERIMEN BARU)
            print("\n[STEP 2] Menjalankan Re-Training Model XGBoost (Membaca ulang puluhan ribu baris)...")
            try:
                subprocess.run(
                    [sys.executable, "-m", "src.models.train_model"],
                    check=True
                )
                print("[+] Proses Re-Training ke-5 Model XGBoost selesai.")
            except Exception as e:
                print(f"[!] Gagal melakukan training ulang model: {str(e)}")
            
            # STEP 3: Panggil model yang baru dilatih untuk scan sinyal dan tembak ke DB
            print("\n[STEP 3] Mengeksekusi Scanner untuk mengevaluasi sinyal baru ke Database...")
            try:
                scanner.scan_market_for_signals()
                print("[+] Sinyal hasil model baru berhasil disimpan ke database.")
            except Exception as e:
                print(f"[!] Terjadi gangguan pada saat scanner berjalan: {str(e)}")
            
            print("\n[*] Siklus penuh selesai. Menunggu Jeda 30 detik...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n[!] Pipeline eksperimen dihentikan secara manual.")

if __name__ == "__main__":
    start_price_pipeline()