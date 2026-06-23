import os
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster

# Konfigurasi Cassandra — single node (localhost)
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
CASSANDRA_KEYSPACE = "crypto_ks"


class CryptoFeatureEngineer:
    def __init__(self, data_dir: str = "DATA", cassandra_session=None):
        self.data_dir = data_dir
        self.cassandra_session = cassandra_session

    def _get_cassandra_session(self):
        """
        Mendapatkan koneksi Cassandra. Jika belum ada, buat koneksi baru.
        """
        if self.cassandra_session is None:
            try:
                cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
                self.cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
                self._owns_cluster = cluster  # simpan referensi untuk cleanup
            except Exception as e:
                print(f"[!] Gagal konek ke Cassandra: {e}. Fallback ke CSV.")
                return None
        return self.cassandra_session

    def load_btc_anchor_features(self) -> pd.DataFrame:
        """
        Membaca data BTC untuk fitur jangkar (BTC_Vol_1h, BTC_Vol_3h).
        Sumber utama: Cassandra. Fallback: file CSV lokal.
        """
        session = self._get_cassandra_session()

        if session is not None:
            # --- Baca dari Cassandra ---
            try:
                query = 'SELECT datetime, close FROM signals WHERE "token" = \'BTC\''
                rows = session.execute(query)
                data = [{"Datetime": row.datetime, "Close": row.close} for row in rows]
                btc_df = pd.DataFrame(data)

                if not btc_df.empty:
                    btc_df["Datetime"] = pd.to_datetime(btc_df["Datetime"]).dt.tz_localize(None)
                    btc_df = btc_df.sort_values("Datetime").reset_index(drop=True)
                    btc_df["Close"] = pd.to_numeric(btc_df["Close"], errors="coerce")
                    btc_df = btc_df.dropna(subset=["Datetime", "Close"])

                    btc_df["BTC_Vol_1h"] = btc_df["Close"].pct_change(1).abs()
                    btc_df["BTC_Vol_3h"] = btc_df["Close"].pct_change(3).abs()
                    print("[+] Fitur jangkar BTC berhasil dibaca dari Cassandra.")
                    return btc_df[["Datetime", "BTC_Vol_1h", "BTC_Vol_3h"]]
            except Exception as e:
                print(f"[!] Gagal baca BTC dari Cassandra: {e}. Fallback ke CSV.")

        # --- Fallback: Baca dari CSV ---
        btc_path = os.path.join(self.data_dir, "BTC_1h.csv") 
        if not os.path.exists(btc_path):
            raise FileNotFoundError(f"[!] File jangkar {btc_path} wajib ada di direktori DATA.")
            
        btc_df = pd.read_csv(btc_path, low_memory=False)
        
        # Penyeragaman format waktu wajib: hilangkan zona waktu jika ada (+00:00 atau UTC)
        btc_df["Datetime"] = pd.to_datetime(btc_df["Datetime"], errors="coerce").dt.tz_localize(None)
        btc_df["Close"] = pd.to_numeric(btc_df["Close"], errors="coerce")
        btc_df = btc_df.dropna(subset=["Datetime", "Close"])
        
        btc_df["BTC_Vol_1h"] = btc_df["Close"].pct_change(1).abs()
        btc_df["BTC_Vol_3h"] = btc_df["Close"].pct_change(3).abs()
        print("[*] Fitur jangkar BTC dibaca dari CSV (fallback).")
        return btc_df[["Datetime", "BTC_Vol_1h", "BTC_Vol_3h"]]

    def build_features(self, df: pd.DataFrame, token: str, is_training: bool = True) -> pd.DataFrame:
        df = df.copy()
        
        # Penyeragaman format waktu pada token yang sedang dievaluasi
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce").dt.tz_localize(None)
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df = df.dropna(subset=["Datetime", "Close"])

        if df.empty:
            return df

        # 1. Fitur Momentum Historis
        df["Return_1h"] = df["Close"].pct_change(1)
        df["Return_3h"] = df["Close"].pct_change(3)
        df["Return_12h"] = df["Close"].pct_change(12)
        
        # 2. Fitur Indikator Tren Makro (EMA 50)
        df["EMA_50h"] = df["Close"].ewm(span=50, adjust=False).mean()
        df["Trend_Direction"] = (df["Close"] > df["EMA_50h"]).astype(int)
        
        # 3. Fitur Kompresi Bollinger Bands
        df["Std_12h"] = df["Close"].rolling(window=12).std()
        df["MA_12h"] = df["Close"].rolling(window=12).mean()
        df["BB_Upper"] = df["MA_12h"] + (2 * df["Std_12h"])
        df["BB_Lower"] = df["MA_12h"] - (2 * df["Std_12h"])
        df["BB_Position"] = (df["Close"] - df["BB_Lower"]) / (df["BB_Upper"] - df["BB_Lower"] + 1e-9)

        # 4. Fitur Rezim Volatilitas Pasar
        df["BB_Bandwidth"] = (df["BB_Upper"] - df["BB_Lower"]) / (df["MA_12h"] + 1e-9)
        df["BB_Bandwidth_MA"] = df["BB_Bandwidth"].rolling(window=24).mean()
        df["Volatility_Regime"] = df["BB_Bandwidth"] / (df["BB_Bandwidth_MA"] + 1e-9)

        # 5. Fitur Akselerasi Volume
        if "Volume" in df.columns:
            df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
            df["Volume_MA12"] = df["Volume"].rolling(window=12).mean()
            df["Volume_Ratio"] = df["Volume"] / (df["Volume_MA12"] + 1e-9)
        else:
            df["Volume_Ratio"] = 1.0

        # 6. Integrasi aman dengan data pasar jangkar BTC
        if token != "BTC":
            btc_feats = self.load_btc_anchor_features()
            # Urutkan berdasarkan Datetime sebelum merge_asof untuk mencegah error sortir
            df = df.sort_values("Datetime")
            btc_feats = btc_feats.sort_values("Datetime")
            
            # Menggunakan merge_asof agar jika ada selisih detik/menit tipis, data tetap menyatu dan TIDAK JADI NAN
            df = pd.merge_asof(df, btc_feats, on="Datetime", direction="nearest")
        else:
            df["BTC_Vol_1h"] = df["Return_1h"].abs()
            df["BTC_Vol_3h"] = df["Return_3h"].abs()

        # 7. Pembuatan Label Target (Hanya saat Training)
        if is_training:
            target_threshold = 0.010 if token in ["BTC", "ETH", "BNB"] else 0.018
            df["Target_Vol"] = (((df["Close"].shift(-3) - df["Close"]).abs() / df["Close"]) >= target_threshold).astype(int)

        # 8. Proteksi Kebocoran Data (Data Leakage)
        features_to_shift = [
            "Return_1h", "Return_3h", "Return_12h", 
            "BB_Position", "Volume_Ratio", "BTC_Vol_1h", "BTC_Vol_3h",
            "Trend_Direction", "Volatility_Regime"
        ]
        
        if is_training:
            df[features_to_shift] = df[features_to_shift].shift(1)
            # Buang baris kosong yang wajar akibat pergeseran rolling awal
            return df.dropna(subset=features_to_shift + ["Target_Vol"])
        else:
            if "Target_Vol" in df.columns:
                df = df.drop(columns=["Target_Vol"])
            # Mengisi kekosongan fitur masa lalu dengan aman
            df[features_to_shift] = df[features_to_shift].ffill().bfill()
            return df.tail(1)