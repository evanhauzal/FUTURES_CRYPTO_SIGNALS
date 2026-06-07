import os
import pandas as pd
from datetime import datetime
from src.features.feature_engineering import CryptoFeatureEngineer
from src.models.predict_linear import CryptoInferenceEngine

class TradingSignalCenter:
    def __init__(self, model_dir: str = "MODEL"):
        # Mengunci lokasi folder data absolut sesuai dengan direktori laptop Anda
        self.data_dir = r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA"
        self.engineer = CryptoFeatureEngineer(data_dir=self.data_dir)
        self.inference = CryptoInferenceEngine(model_dir=model_dir)

        # Ambang batas probabilitas (Confidence Threshold) untuk memicu sinyal LONG
        self.confidence_thresholds = {
            "BTC": 0.51, 
            "BNB": 0.51, 
            "ETH": 0.52, 
            "XRP": 0.52, 
            "SOL": 0.53
        }

        # Parameter manajemen risiko untuk posisi Futures (Target persentase TP/SL)
        self.risk_parameters = {
            "BTC": {"TP": 0.030, "SL": 0.012},
            "BNB": {"TP": 0.030, "SL": 0.012},
            "ETH": {"TP": 0.025, "SL": 0.008},
            "SOL": {"TP": 0.050, "SL": 0.020},
            "XRP": {"TP": 0.050, "SL": 0.020}
        }

    def scan_market_for_signals(self):
        # Memetakan jalur file data mentah secara absolut untuk kelima token
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
                
            # Membaca data mentah dengan low_memory=False untuk mencegah DtypeWarning
            df_raw = pd.read_csv(file_path, low_memory=False)
            
            # Ekstrak fitur teknis non-linear menggunakan modul feature engineering
            df_features = self.engineer.build_features(df_raw, token, is_training=False)
            
            if df_features.empty:
                print(f"[!] Data fitur untuk {token} kosong setelah pembersihan.")
                continue
                
            # Mengambil baris data jam terakhir (paling baru) untuk dievaluasi
            latest_row = df_features.iloc[-1]
            current_price = latest_row["Close"]
            current_time = latest_row["Datetime"]
            
            # Melakukan kalkulasi probabilitas arah pasar menggunakan model XGBoost
            prob = self.inference.predict_next_probability(df_features, token)
            threshold = self.confidence_thresholds.get(token, 0.52)
            
            # Menampilkan log pemantauan indikator ke layar terminal
            print(f"[{token}] Price: ${current_price:<10} | Prob: {prob*100:.2f}% | Threshold: {threshold*100:.1f}%")
            
            # Evaluasi kondisi: Jika probabilitas melampaui batas, cetak sinyal eksekusi
            if prob >= threshold:
                risk = self.risk_parameters[token]
                tp_price = current_price * (1 + risk["TP"])
                sl_price = current_price * (1 - risk["SL"])
                
                print(f"   [!!!] SIGNAL DETECTED - EXECUTE LONG {token} [!!!]")
                print(f"         Execution Entry : ${current_price:.4f}")
                print(f"         Take Profit (TP): ${tp_price:.4f} (+{risk['TP']*100}%)")
                print(f"         Stop Loss (SL)  : ${sl_price:.4f} (-{risk['SL']*100}%)")
                print(f"         Data Timestamp  : {current_time}")
                print("-" * 40)

if __name__ == "__main__":
    center = TradingSignalCenter()
    center.scan_market_for_signals()