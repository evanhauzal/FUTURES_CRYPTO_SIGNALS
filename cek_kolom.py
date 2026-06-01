from src.storage.postgres_client import PostgresClient

db = PostgresClient()
db.connect()

with db._get_conn() as conn:
    with conn.cursor() as cur:
        # Mengambil 1 baris data saja untuk mendeteksi struktur tabel
        cur.execute("SELECT * FROM predictions LIMIT 1;")
        colnames = [desc[0] for desc in cur.description]
        print("\n📋 DAFTAR NAMA KOLOM ASLI DI DATABASE:")
        print("------------------------------------------")
        for col in colnames:
            print(f"-> {col}")
        print("------------------------------------------\n")