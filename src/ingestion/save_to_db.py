# src/ingestion/save_to_db.py
import os
import psycopg2
from datetime import datetime
# Mengimpor modul analisis sentimen mandiri dari folder models yang sudah dibuat
from src.models.sentiment_model import CryptoSentimentAnalyzer
from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SUPABASE_URL, SUPABASE_KEY
import requests

class TradingDatabaseConnector:
    def __init__(self):
        self.analyzer = CryptoSentimentAnalyzer()
        self.headers = {
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type': 'application/json'
        }

    def insert_crypto_signal(self, token: str, price: float, probability: float, status: str, tp: float = None, sl: float = None):
        """
        Memasukkan data harga berjalan dan simulasi status sinyal ke tabel v_crypto_signals via REST API
        """
        url = f"{SUPABASE_URL}/rest/v1/v_crypto_signals"
        payload = {
            "token": token,
            "price": price,
            "probability": probability,
            "signal_status": status,
            "take_profit": tp,
            "stop_loss": sl
        }
        
        try:
            res = requests.post(url, headers=self.headers, json=payload)
            res.raise_for_status()
            print(f"[DB SUCCESS] Berhasil menyimpan harga terbaru {token}: ${price:.2f}")
        except Exception as e:
            print(f"[DB ERROR] Gagal menyimpan data harga/sinyal: {str(e)}")

    def insert_market_news(self, source_name: str, title: str, description: str, news_url: str, published_at: str):
        """
        Memasukkan data teks berita global sekaligus label sentimennya ke tabel v_market_news via REST API
        """
        try:
            # Cek duplikasi judul berita
            check_url = f"{SUPABASE_URL}/rest/v1/v_market_news"
            check_params = {"title": f"eq.{title}", "select": "id"}
            check_res = requests.get(check_url, headers=self.headers, params=check_params)
            
            if check_res.status_code == 200 and len(check_res.json()) > 0:
                return # Skip jika berita sudah pernah dimasukkan sebelumnya
                
            try:
                clean_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").isoformat()
            except:
                clean_date = datetime.now().isoformat()

            sentiment_label = self.analyzer.calculate_sentiment_label(title, description)

            payload = {
                "source_name": source_name,
                "title": title,
                "description": description,
                "url": news_url,
                "published_at": clean_date,
                "sentiment": sentiment_label
            }
            
            res = requests.post(check_url, headers=self.headers, json=payload)
            res.raise_for_status()
            print(f"[DB SUCCESS] Berhasil menyimpan berita baru [{sentiment_label}]: {title[:40]}...")
        except Exception as e:
            print(f"[DB ERROR] Gagal menyimpan data berita: {str(e)}")

    def get_latest_market_news(self, limit: int = 1):
        """
        Mengambil berita kripto paling baru dari tabel v_market_news beserta kolom sentimen aslinya via REST API
        """
        try:
            url = f"{SUPABASE_URL}/rest/v1/v_market_news"
            params = {
                "select": "source_name,title,description,url,sentiment",
                "order": "id.desc",
                "limit": str(limit)
            }
            res = requests.get(url, headers=self.headers, params=params)
            res.raise_for_status()
            data = res.json()
            
            if data and len(data) > 0:
                row = data[0]
                return {
                    "source": row.get("source_name"),
                    "title": row.get("title"),
                    "description": row.get("description"),
                    "url": row.get("url"),
                    "sentiment": row.get("sentiment", "NEUTRAL")
                }
            return None
        except Exception as e:
            print(f"[DB ERROR] Gagal mengambil berita terbaru: {str(e)}")
            return None

# =========================================================================
# JALUR TESTING KONEKSI LOKAL
# =========================================================================
if __name__ == "__main__":
    print("[*] Menguji coba koneksi dan fungsionalitas insert PostgreSQL dengan Sentimen Otomatis...")
    db = TradingDatabaseConnector()
    
    # Uji coba input data harga tiruan
    db.insert_crypto_signal(token="BTC", price=62190.50, probability=55.4, status="LONG", tp=64055.0, sl=61443.0)
    
    # Uji coba input data berita tiruan (Sistem akan otomatis memberikan label lewat model eksternal)
    db.insert_market_news(
        source_name="Test Source", 
        title="Uji Coba Koneksi Sistem Database Terdistribusi ROSBD", 
        description="Deskripsi uji coba koneksi lokal.", 
        url="https://localhost", 
        published_at="2026-06-07T12:00:00Z"
    )