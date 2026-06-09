# src/models/sentiment_model.py
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

class CryptoSentimentAnalyzer:
    def __init__(self):
        # Inisialisasi mesin VADER untuk analisis sentimen teks finansial
        self.sia = SentimentIntensityAnalyzer()
        
        # Kamus kata khusus pergerakan pasar kripto untuk meningkatkan akurasi
        crypto_lexicon = {
            'bullish': 2.0, 'bearish': -2.0, 'pump': 1.5, 'dump': -1.5,
            'scam': -2.5, 'hack': -2.0, 'surge': 1.5, 'collapse': -2.5,
            'war': -2.0, 'attack': -1.5, 'ban': -2.0, 'etf': 1.5
        }
        self.sia.lexicon.update(crypto_lexicon)

    def calculate_sentiment_label(self, title: str, description: str) -> str:
        """Menghitung skor polaritas teks dan mengembalikan label string kapital"""
        full_text = f"{title}. {description}" if description else title
        score = self.sia.polarity_scores(full_text)
        compound = score['compound']
        
        if compound >= 0.05:
            return "POSITIVE"
        elif compound <= -0.05:
            return "NEGATIVE"
        else:
            return "NEUTRAL"