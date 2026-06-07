import os
import pandas as pd
import yfinance as yf
from datetime import datetime

class YahooDataLoader:
    def __init__(self, data_dir: str = "DATA"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Mapping token ke ticker yfinance
        self.ticker_map = {
            "BTC": "BTC-USD",
            "ETH": "ETH-USD",
            "SOL": "SOL-USD",
            "XRP": "XRP-USD",
            "BNB": "BNB-USD"
        }

    def fetch_historical_data(self, token: str, period: str = "4y", interval: str = "1h") -> pd.DataFrame:
        ticker = self.ticker_map.get(token)
        if not ticker:
            raise ValueError(f"Token {token} tidak terdaftar di konfigurasi pipeline.")
            
        print(f"[*] Mengunduh data historis {token} ({ticker}) dari yfinance...")
        df = yf.download(tickers=ticker, period=period, interval=interval)
        
        if df.empty:
            raise RuntimeError(f"[!] Gagal mengunduh data untuk {token}.")
            
        df = df.reset_index()
        # Standardisasi nama kolom utama
        df.rename(columns={"Datetime": "Datetime", "Date": "Datetime"}, inplace=True)
        return df

    def get_local_path(self, token: str) -> str:
        return os.path.join(self.data_dir, f"{token}-USD.csv")