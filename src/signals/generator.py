import os
import pandas as pd
from datetime import datetime
from src.features.feature_engineering import CryptoFeatureEngineer
from src.models.predict_linear import CryptoInferenceEngine
# Tambahkan import konektor database kelompok Anda di sini
from src.ingestion.save_to_db import TradingDatabaseConnector

class TradingSignalCenter:
    def __init__(self, model_dir: str = "MODEL"):
        self.data_dir = r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA"
        self.engineer = CryptoFeatureEngineer(data_dir=self.data_dir)
        self.inference = CryptoInferenceEngine(model_dir=model_dir)
        
        # Inisialisasi konektor database terpusat
        self.db = TradingDatabaseConnector()

        self.confidence_thresholds = {
            "BTC": 0.51, 
            "BNB": 0.51, 
            "ETH": 0.52, 
            "XRP": 0.52, 
            "SOL": 0.53
        }

        self.risk_parameters = {
            "BTC": {"TP": 0.030, "SL": 0.012},
            "BNB": {"TP": 0.030, "SL": 0.012},
            "ETH": {"TP": 0.025, "SL": 0.008},
            "SOL": {"TP": 0.050, "SL": 0.020},
            "XRP": {"TP": 0.050, "SL": 0.020}
        }

    def scan_market_for_signals(self):
        file_paths = {
            "BTC": os.path.join(self.data_dir, "BTC_1h.csv"),
            "ETH": os.path.join(self.data_dir, "ETH_1h.csv"),
            "SOL": os.path.join(self.data_dir, "SOL_1h.csv"),
            "XRP": os.path.join(self.data_dir, "XRP_1h.csv"),
            "BNB": os.path.join(self.data_dir, "BNB_1h.csv")
        }
        
        print("\n" + "="*50)
        print(f"[*] MENJALANKAN SCANNER SINYAL TRADING ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("="*50)

        for token, file_path in file_paths.items():
            if not os.path.exists(file_path):
                print(f"[!] Berkas transaksi {token} tidak ditemukan di {file_path}")
                continue
                
            df_raw = pd.read_csv(file_path, low_memory=False)
            df_features = self.engineer.build_features(df_raw, token, is_training=False)
            
            if df_features.empty:
                print(f"[!] Data fitur untuk {token} kosong setelah pembersihan.")
                continue
                
            latest_row = df_features.iloc[-1]
            current_price = float(latest_row["Close"])
            current_time = latest_row["Datetime"]
            
            prob_raw = self.inference.predict_next_probability(df_features, token)
            prob = float(prob_raw)
            threshold = self.confidence_thresholds.get(token, 0.52)
            
            print(f"[{token}] Price: ${current_price:<10} | Prob: {prob*100:.2f}% | Threshold: {threshold*100:.1f}%")
            
            # Ambil parameter manajemen risiko
            risk = self.risk_parameters[token]
            
            # Logika evaluasi kondisi pasar berdasarkan Model XGBoost
            if prob >= threshold:
                status = "LONG"
                tp_price = current_price * (1 + risk["TP"])
                sl_price = current_price * (1 - risk["SL"])
                
                print(f"   [!!!] SIGNAL DETECTED - EXECUTE LONG {token} [!!!]")
                print(f"         Execution Entry : ${current_price:.4f}")
                print(f"         Take Profit (TP): ${tp_price:.4f} (+{risk['TP']*100}%)")
                print(f"         Stop Loss (SL)  : ${sl_price:.4f} (-{risk['SL']*100}%)")
                print(f"         Data Timestamp  : {current_time}")
                print("-" * 40)
            else:
                status = "Wait & See"
                tp_price = None
                sl_price = None
            
            # KUNCI UTAMA: Tembak hasil kalkulasi asli XGBoost ke PostgreSQL agar masuk ke Streamlit
            try:
                self.db.insert_crypto_signal(
                    token=token,
                    price=current_price,
                    probability=round(prob * 100, 2), # Ubah desimal ke persentase (misal 57.39)
                    status=status,
                    tp=tp_price,
                    sl=sl_price
                )
            except Exception as e:
                print(f"[!] Gagal menyimpan sinyal XGBoost {token} ke DB: {str(e)}")

# Tambahkan fungsi pembantu agar bisa dipanggil secara modular oleh file lain
def run_signal_scanner():
    center = TradingSignalCenter()
    center.scan_market_for_signals()

if __name__ == "__main__":
    run_signal_scanner()