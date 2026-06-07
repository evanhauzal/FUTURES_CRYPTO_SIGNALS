import psycopg2
from datetime import datetime

class TradingDatabaseConnector:
    def __init__(self):
        # Konfigurasi kredensial PostgreSQL lokal di laptop Anda
        self.host = "localhost"
        self.database = "rosbd_trading_db"
        self.user = "postgres"
        self.password = "Naya110212" # <-- Ganti dengan password Anda

    def _get_connection(self):
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def insert_crypto_signal(self, token: str, price: float, probability: float, status: str, tp: float = None, sl: float = None):
        """
        Memasukkan data harga berjalan dan simulasi status sinyal ke tabel v_crypto_signals
        """
        query = """
            INSERT INTO v_crypto_signals (token, price, probability, signal_status, take_profit, stop_loss)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (token, price, probability, status, tp, sl))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[DB SUCCESS] Berhasil menyimpan harga terbaru {token}: ${price:.2f}")
        except Exception as e:
            print(f"[DB ERROR] Gagal menyimpan data harga/sinyal: {str(e)}")

    def insert_market_news(self, source_name: str, title: str, description: str, url: str, published_at: str):
        """
        Memasukkan data teks berita global ke tabel v_market_news
        """
        # Cek duplikasi judul berita agar database tidak penuh dengan berita yang sama
        check_query = "SELECT id FROM v_market_news WHERE title = %s;"
        insert_query = """
            INSERT INTO v_market_news (source_name, title, description, url, published_at)
            VALUES (%s, %s, %s, %s, %s);
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Eksekusi cek duplikasi
            cursor.execute(check_query, (title,))
            if cursor.fetchone() is not None:
                cursor.close()
                conn.close()
                return # Skip jika berita sudah pernah dimasukkan sebelumnya
                
            # Konversi string ISO waktu NewsAPI ke format timestamp Python demi validitas data
            try:
                clean_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            except:
                clean_date = datetime.now()

            cursor.execute(insert_query, (source_name, title, description, url, clean_date))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[DB SUCCESS] Berhasil menyimpan berita baru: {title[:40]}...")
        except Exception as e:
            print(f"[DB ERROR] Gagal menyimpan data berita: {str(e)}")

# =========================================================================
# JALUR TESTING KONEKSI LOKAL
# =========================================================================
if __name__ == "__main__":
    print("[*] Menguji coba koneksi dan fungsionalitas insert PostgreSQL...")
    db = TradingDatabaseConnector()
    
    # Uji coba input data harga tiruan
    db.insert_crypto_signal(token="BTC", price=62190.50, probability=55.4, status="LONG", tp=64055.0, sl=61443.0)
    
    # Uji coba input data berita tiruan
    db.insert_market_news(
        source_name="Test Source", 
        title="Uji Coba Koneksi Sistem Database Terdistribusi ROSBD", 
        description="Deskripsi uji coba koneksi lokal.", 
        url="https://localhost", 
        published_at="2026-06-07T12:00:00Z"
    )