import sys
import time
from src.ingestion.news_collector import GlobalNewsCollector
from src.ingestion.save_to_db import TradingDatabaseConnector
from src.signals.generator import TradingSignalCenter


def _run_one_cycle(collector, db, signal_center):
    """Menjalankan satu siklus penarikan dan pengiriman berita."""
    print(f"\n[*] Memulai siklus penarikan berita pada: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Tarik 10 berita makro teranyar dari internet
    latest_articles = collector.fetch_latest_news(limit=10)

    if latest_articles:
        print(f"[+] Menemukan {len(latest_articles)} artikel relevan. Memproses ke database...")

        # Masukkan setiap artikel ke dalam PostgreSQL tabel v_market_news
        for art in latest_articles:
            # Menangkap label sentimen hasil kalkulasi VADER dari fungsi database
            sentiment_label = db.insert_market_news(
                source_name=art["Source"],
                title=art["Title"],
                description=art["Description"],
                url=art["URL"],
                published_at=art["Datetime"]
            )

            # Mengirimkan notifikasi ke Telegram hanya jika berita tersebut bukan duplikat (baru masuk database)
            if sentiment_label:
                signal_center.send_news_notification(
                    title=art["Title"],
                    source=art["Source"],
                    url=art["URL"],
                    sentiment=sentiment_label
                )
    else:
        print("[!] Tidak ada berita baru yang ditemukan atau kuota API penuh.")


def start_news_pipeline(api_key: str, run_once: bool = False):
    """
    Menjalankan pipeline berita.
    
    Args:
        api_key: API Key untuk NewsAPI.
        run_once: Jika True, hanya jalankan 1 siklus lalu keluar (untuk Airflow).
                  Jika False, loop terus-menerus (untuk manual terminal).
    """
    print("\n" + "="*60)
    print("[*] BACKEND PIPELINE: KOLEKTOR BERITA MAKRO GLOBAL DIMULAI")
    print("="*60)
    
    # Inisialisasi kolektor berita, konektor database, dan pusat sinyal Telegram
    collector = GlobalNewsCollector(api_key=api_key)
    db = TradingDatabaseConnector()
    signal_center = TradingSignalCenter()
    
    if run_once:
        # Mode Airflow: jalankan 1 siklus lalu selesai
        _run_one_cycle(collector, db, signal_center)
        print("\n[+] Siklus tunggal selesai (mode Airflow).")
        return
    
    try:
        while True:
            _run_one_cycle(collector, db, signal_center)
            
            # Beri jeda waktu 5 menit (300 detik) per siklus agar aman dari rate limit API gratisan
            print("\n[*] Siklus selesai. Menunggu 5 menit untuk pemindaian berikutnya...")
            time.sleep(100)
            
    except KeyboardInterrupt:
        print("\n[!] Pipeline berita dihentikan secara manual oleh pengguna.")


if __name__ == "__main__":
    # MASUKKAN API KEY NEWSAPI AKTIF MILIK KELOMPOK ANDA DI SINI
    YOUR_NEWS_API_KEY = "12fbb5d602ed4e90a9c442f484bd5d2d"
    
    # Jika dipanggil dengan --once, jalankan satu siklus saja (untuk Airflow)
    run_once = "--once" in sys.argv
    
    start_news_pipeline(api_key=YOUR_NEWS_API_KEY, run_once=run_once)