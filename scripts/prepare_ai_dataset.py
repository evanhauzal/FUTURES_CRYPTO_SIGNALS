import pandas as pd
from src.storage.postgres_client import PostgresClient

def export_data_for_training():
    db = PostgresClient()
    db.connect()
    
    # Kita ambil data historis: Feature saat ini (T) dan Harga target (T+5)
    q = """
    select 
        timestamp,
        harga_saat_ini,
        lead(harga_saat_ini, 5) over (order by timestamp) as target_harga_t5,
        arus_kas_bandar_usd
    from predictions
    where timestamp is not null;
    """
    
    with db._get_conn() as conn:
        df = pd.read_sql_query(q, conn)
    
    # Bersihkan data (hapus row yang tidak punya target harga)
    df = df.dropna()
    
    # Simpan ke CSV untuk proses training nanti
    df.to_csv("data_training_pepe.csv", index=False)
    print(f"✅ Dataset siap! {len(df)} baris data disimpan ke data_training_pepe.csv")

if __name__ == "__main__":
    export_data_for_training()