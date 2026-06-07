import requests
import pandas as pd
from datetime import datetime

class GlobalNewsCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
        
        # Klaster 1: Geopolitik, Perang, dan Krisis Energi Global
        geopolitics = '(war OR conflict OR military OR sanction OR "geopolitical crisis" OR "oil prices" OR crude)'
        
        # Klaster 2: Makroekonomi, Kebijakan Bank Sentral, dan Finansial Global
        macroeconomics = '("fed rate" OR "interest rate" OR inflation OR recession OR FOMC OR "central bank" OR "liquidity crunch" OR brent)'
        
        # Klaster 3: Regulasi Finansial dan Institusi Keuangan Besar
        regulations = '(SEC OR CFTC OR regulation OR "subprime" OR ETF OR "wall street" OR banking)'
        
        # Klaster 4: Aset Kripto Utama (sebagai jangkar relevansi langsung)
        crypto_anchor = '(crypto OR bitcoin OR ethereum OR btc OR eth)'
        
        # GABUNGAN FORMULA LOGIKA: Berita makro global (Klaster 1 ATAU 2 ATAU 3) YANG berkorelasi/berdampak pada pasar finansial/kripto
        self.search_queries = f"({geopolitics} OR {macroeconomics} OR {regulations}) AND {crypto_anchor}"

    def fetch_latest_news(self, limit: int = 10) -> list:
        """
        Mengambil berita berskala luas yang memengaruhi sentimen pasar kripto secara global.
        """
        params = {
            "q": self.search_queries,
            "sortBy": "publishedAt",  # Selalu ambil berita paling segar / terbaru
            "language": "en",         # Bahasa Inggris untuk mencakup Reuters, Bloomberg, CNBC, dll.
            "pageSize": limit,         # Jumlah berita yang ditarik dalam satu kali hit
            "apiKey": self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            articles = data.get("articles", [])
            parsed_articles = []
            
            for art in articles:
                # Validasi konten agar tidak memasukkan artikel yang rusak/dihapus dari sumbernya
                title = art.get("title")
                if not title or "[Removed]" in title:
                    continue
                    
                parsed_articles.append({
                    "Datetime": art.get("publishedAt"),
                    "Source": art.get("source", {}).get("name"),
                    "Title": title,
                    "Description": art.get("description"),
                    "URL": art.get("url")
                })
                
            return parsed_articles
            
        except Exception as e:
            print(f"[!] Gagal mengeksekusi penarikan berita berskala luas: {str(e)}")
            return []

# =========================================================================
# JALUR TESTING LOKAL
# =========================================================================
if __name__ == "__main__":
    # Ganti dengan API Key aktif milik kelompok Anda
    YOUR_API_KEY = "12fbb5d602ed4e90a9c442f484bd5d2d"
    
    print("[*] Menguji penarikan berita multi-klaster makroekonomi & geopolitik...")
    collector = GlobalNewsCollector(api_key=YOUR_API_KEY)
    news_data = collector.fetch_latest_news(limit=5)
    
    if news_data:
        print(f"[+] Sukses menangkap {len(news_data)} berita global berdampak tinggi.\n")
        for idx, row in enumerate(news_data, 1):
            print(f"{idx}. [{row['Source']}] - {row['Datetime']}")
            print(f"   Judul: {row['Title']}")
            print(f"   Link : {row['URL']}\n")
            print("-" * 60)
    else:
        print("[!] Data kosong. Periksa limitasi API Key, query pencarian, atau koneksi ZeroTier Anda.")