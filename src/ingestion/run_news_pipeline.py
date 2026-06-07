import time
from src.ingestion.news_collector import GlobalNewsCollector
from src.ingestion.save_to_db import TradingDatabaseConnector

def start_news_pipeline(api_key: str):
    print("\n" + "="*60)
    print("[*] BACKEND PIPELINE: KOLEKTOR BERITA MAKRO GLOBAL DIMULAI")
    print("="*60)
    
    # Inisialisasi kolektor berita dan konektor database
    collector = GlobalNewsCollector(api_key=api_key)
    db = TradingDatabaseConnector()
    
    try:
        while True:
            print(f"\n[*] Memulai siklus penarikan berita pada: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Tarik 10 berita makro teranyar dari internet
            latest_articles = collector.fetch_latest_news(limit=10)
            
            if latest_articles:
                print(f"[+] Menemukan {len(latest_articles)} artikel relevan. Memproses ke database...")
                
                # Masukkan setiap artikel ke dalam PostgreSQL tabel v_market_news
                for art in latest_articles:
                    db.insert_market_news(
                        source_name=art["Source"],
                        title=art["Title"],
                        description=art["Description"],
                        url=art["URL"],
                        published_at=art["Datetime"]
                    )
            else:
                print("[!] Tidak ada berita baru yang ditemukan atau kuota API penuh.")
            
            # Beri jeda waktu 5 menit (300 detik) per siklus agar aman dari rate limit API gratisan
            print("\n[*] Siklus selesai. Menunggu 5 menit untuk pemindaian berikutnya...")
            time.sleep(300)
            
    except KeyboardInterrupt:
        print("\n[!] Pipeline berita dihentikan secara manual oleh pengguna.")

if __name__ == "__main__":
    # MASUKKAN API KEY NEWSAPI AKTIF MILIK KELOMPOK ANDA DI SINI
    YOUR_NEWS_API_KEY = "12fbb5d602ed4e90a9c442f484bd5d2d"
    
    start_news_pipeline(api_key=YOUR_NEWS_API_KEY)