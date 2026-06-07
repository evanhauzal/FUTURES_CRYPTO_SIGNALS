import os
import pandas as pd
from datetime import datetime, timedelta
from src.ingestion.yahoo_loader import YahooDataLoader

def sync_all_tokens():
    loader = YahooDataLoader()
    
    # Menambahkan huruf 'r' di depan string agar backslash Windows tidak error
    file_paths = {
        "BTC": r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA\BTC_1h.csv",
        "ETH": r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA\ETH_1h.csv",
        "SOL": r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA\SOL_1h.csv",
        "XRP": r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA\XRP_1h.csv",
        "BNB": r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA\BNB_1h.csv"
    }
    
    print("\n" + "="*50)
    print("[*] MEMULAI PIPELINE SINKRONISASI DATA DENGAN CUSTOM PATH")
    print("="*50)
    
    for token, file_path in file_paths.items():
        if not os.path.exists(file_path):
            print(f"[!] File tidak ditemukan di lokasi: {file_path}")
            print(f"[*] Membuat file baru untuk {token}...")
            df_new = loader.fetch_historical_data(token, period="4y", interval="1h")
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df_new.to_csv(file_path, index=False)
            continue
            
        try:
            df_local = pd.read_csv(file_path)
            df_local['Datetime'] = pd.to_datetime(df_local['Datetime'])
            
            last_local_time = df_local['Datetime'].max()
            current_time = datetime.utcnow()
            
            if current_time - last_local_time.to_pydatetime() > timedelta(hours=2):
                print(f"[*] Data {token} tertinggal. Mengambil data baru...")
                df_update = loader.fetch_historical_data(token, period="7d", interval="1h")
                df_update['Datetime'] = pd.to_datetime(df_update['Datetime'])
                
                df_combined = pd.concat([df_local, df_update]).drop_duplicates(subset=['Datetime'], keep='last')
                df_combined = df_combined.sort_values('Datetime')
                df_combined.to_csv(file_path, index=False)
                print(f"[+] File {token} berhasil diperbarui.")
            else:
                print(f"[+] File {token} sudah mutakhir (Terakhir: {last_local_time}).")
                
        except Exception as e:
            print(f"[!] Gagal memproses file {token}: {str(e)}")

if __name__ == "__main__":
    sync_all_tokens()