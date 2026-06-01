import time
import json
import urllib.request
import xml.etree.ElementTree as ET
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from kafka import KafkaProducer

print("=========================================================")
# PILAR OFF-CHAIN: CRYPTO ADVANCED NEWS SENTIMENT GENERATOR
print("=========================================================")

# 1. Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Inisialisasi VADER & Suntik Kamus Istilah Crypto Dunia Nyata
analyzer = SentimentIntensityAnalyzer()
crypto_lexicon = {
    'burn': 2.0,        # Burn suplai = Bagus banget
    'listing': 2.5,     # Listing di exchange = Bullish parah
    'all-time high': 3.0,
    'ath': 3.0,
    'pump': 2.0,
    'moon': 2.5,
    'dump': -2.5,
    'rugpull': -3.5,    # Penipuan/Scam = Bearish parah
    'scam': -3.0,
    'hack': -2.5,
    'liquidated': -2.0,
    'collapse': -3.0
}
analyzer.lexicon.update(crypto_lexicon)

RSS_URL = "https://cryptonews.com/news/feed/"

def fetch_and_analyze_news():
    try:
        with urllib.request.urlopen(RSS_URL) as response:
            xml_data = response.read()
        
        root = ET.fromstring(xml_data)
        
        for item in root.findall('.//item'):
            title = item.find('title').text
            pub_date = item.find('pubDate').text
            
            # Cek Relevansi: Apakah berita ini berdampak langsung ke PEPE atau Meme Coin?
            title_lower = title.lower()
            is_pepe_related = "pepe" in title_lower or "meme" in title_lower
            
            # Analisis Sentimen menggunakan VADER (mengambil nilai compound score)
            vs = analyzer.polarity_scores(title)
            sentiment_score = vs['compound'] # Nilai mutlak antara -1 dan +1
            
            # Klasifikasi Label
            if sentiment_score >= 0.05:
                sentiment_label = "BULLISH"
            elif sentiment_score <= -0.05:
                sentiment_label = "BEARISH"
            else:
                sentiment_label = "NEUTRAL"
            
            # Bungkus payload data, berikan flag relevansi PEPE
            news_payload = {
                "source": "CryptoNews",
                "headline": title,
                "timestamp": pub_date,
                "sentiment_score": round(sentiment_score, 2),
                "sentiment_label": sentiment_label,
                "is_pepe_target": is_pepe_related # True jika membahas PEPE/Meme, False jika berita umum
            }
            
            # Kirim data ke Kafka Topic
            producer.send('trading-news', news_payload)
            
            # Log ke terminal biar keliatan bedanya
            target_marker = "🐋 [PEPE TARGET]" if is_pepe_related else "[GENERAL]"
            print(f"[{sentiment_label}] {target_marker} {title[:60]}...")
            
        producer.flush()
        
    except Exception as e:
        print(f"Gagal mengambil berita: {e}")

if __name__ == "__main__":
    print("Scraper Berita Pintar Aktif... Memantau pasar dengan Kamus Crypto khusus.")
    while True:
        fetch_and_analyze_news()
        time.sleep(300)