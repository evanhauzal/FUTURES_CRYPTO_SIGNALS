from cassandra.cluster import Cluster

def verify_data():
    print("[*] Menghubungkan ke Cassandra di localhost...")
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('crypto_ks')
    
    tokens = ["BTC", "ETH", "SOL", "XRP", "BNB"]
    
    print("\n--- HASIL VERIFIKASI DATA DI CASSANDRA ---")
    for token in tokens:
        query = f"SELECT COUNT(*) FROM signals WHERE \"token\" = '{token}'"
        result = session.execute(query)
        count = result.one()[0]
        print(f"Token {token}: {count} baris data")
        
        if count > 0:
            # Ambil 1 sample data terbaru
            sample_query = f"SELECT Datetime, Close FROM signals WHERE \"token\" = '{token}' ORDER BY Datetime DESC LIMIT 1"
            try:
                sample = session.execute(sample_query).one()
                if sample:
                    print(f"   -> Data terbaru: {sample.datetime} | Harga Close: {sample.close}")
            except Exception as e:
                # ORDER BY requires CLUSTERING ORDER on Datetime which we didn't specify explicitly (though it's part of PK, we might need to specify it)
                # Let's just get any 1 sample
                sample = session.execute(f"SELECT Datetime, Close FROM signals WHERE \"token\" = '{token}' LIMIT 1").one()
                if sample:
                    print(f"   -> Contoh data: {sample.datetime} | Harga Close: {sample.close}")
                
    print("------------------------------------------\n")
    cluster.shutdown()

if __name__ == "__main__":
    verify_data()
