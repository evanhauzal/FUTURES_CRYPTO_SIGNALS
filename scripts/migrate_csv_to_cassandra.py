import os
import pandas as pd
from cassandra.cluster import Cluster
from datetime import datetime

# Konfigurasi
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "DATA")
TOKENS = ["BTC", "ETH", "SOL", "XRP", "BNB"]

def setup_cassandra():
    print("[*] Menghubungkan ke Cassandra di localhost...")
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    
    print("[*] Membuat Keyspace crypto_ks jika belum ada...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS crypto_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    
    session.set_keyspace('crypto_ks')
    
    print("[*] Membuat tabel signals jika belum ada...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            "token" text,
            Datetime timestamp,
            Open double,
            High double,
            Low double,
            Close double,
            Volume double,
            PRIMARY KEY ("token", Datetime)
        )
    """)
    return cluster, session

def migrate_data():
    cluster, session = setup_cassandra()
    
    insert_query = session.prepare("""
        INSERT INTO signals ("token", Datetime, Open, High, Low, Close, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    
    for token in TOKENS:
        file_path = os.path.join(DATA_DIR, f"{token}_1h.csv")
        if not os.path.exists(file_path):
            print(f"[!] File {file_path} tidak ditemukan, skip {token}.")
            continue
            
        print(f"[*] Membaca data {token} dari CSV...")
        # Membaca CSV, pandas kadang salah membaca kolom tambahan tak bernama (Unamed: ...)
        df = pd.read_csv(file_path, usecols=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume'])
        
        # Bersihkan data NA
        df = df.dropna()
        
        print(f"[*] Mulai insert {len(df)} baris untuk {token} ke Cassandra. Proses ini mungkin butuh beberapa menit...")
        
        count = 0
        for index, row in df.iterrows():
            try:
                # Konversi string Datetime ke objek datetime python
                dt = datetime.strptime(str(row['Datetime']), '%Y-%m-%d %H:%M:%S')
                
                session.execute(insert_query, (
                    token,
                    dt,
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    float(row['Volume'])
                ))
                count += 1
                if count % 5000 == 0:
                    print(f"    - {count} baris tersimpan...")
            except Exception as e:
                print(f"[!] Error pada baris {index}: {e}")
                continue
                
        print(f"[+] Selesai migrasi {count} baris data {token}.")

    print("\n[+] Migrasi semua token selesai!")
    cluster.shutdown()

if __name__ == "__main__":
    migrate_data()
