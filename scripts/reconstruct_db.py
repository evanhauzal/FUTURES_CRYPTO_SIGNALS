import sys
import os
# Tambahkan root path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.postgres_client import PostgresClient

def clean_data():
    db = PostgresClient()
    db.connect()
    
    print("🧹 Membersihkan database dari data skala rendah...")
    
    # Kita hanya menyimpan data yang dianggap 'bersih' (data setelah migrasi presisi)
    # Sesuaikan tanggal dengan kapan kamu mulai migrasi ke DOUBLE PRECISION
    query = """
    DELETE FROM predictions 
    WHERE timestamp < '2026-06-01 00:00:00'; -- Sesuaikan tanggal migrasimu
    """
    
    with db._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
            print(f"✅ Berhasil menghapus {cur.rowcount} baris data lama yang tidak presisi.")

if __name__ == "__main__":
    clean_data()