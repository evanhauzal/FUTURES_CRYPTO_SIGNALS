import os
import joblib
import pandas as pd
import numpy as np

class CryptoInferenceEngine:
    def __init__(self, model_dir: str = "MODEL"):
        # Mengunci direktori tempat penyimpanan model XGBoost (.pkl)
        self.model_dir = model_dir
        self.models = {}
        self.feature_columns = []
        
        # Memuat daftar kolom fitur yang wajib digunakan oleh model
        self.load_feature_metadata()
        # Memuat seluruh model token yang tersedia ke dalam memori
        self.load_all_trained_models()

    def load_feature_metadata(self):
        metadata_path = os.path.join(self.model_dir, "feature_columns.pkl")
        if os.path.exists(metadata_path):
            self.feature_columns = joblib.load(metadata_path)
        else:
            # Fallback jika berkas metadata pkl belum terbentuk
            self.feature_columns = [
                "Return_1h", "Return_3h", "Return_12h", 
                "BB_Position", "Volume_Ratio", "BTC_Vol_1h", "BTC_Vol_3h",
                "Trend_Direction", "Volatility_Regime"
            ]

    def load_all_trained_models(self):
        tokens = ["BTC", "ETH", "SOL", "XRP", "BNB"]
        for token in tokens:
            model_path = os.path.join(self.model_dir, f"{token.lower()}_xgb_model.pkl")
            if os.path.exists(model_path):
                self.models[token] = joblib.load(model_path)
            else:
                self.models[token] = None

    def predict_next_probability(self, df_features: pd.DataFrame, token: str) -> float:
        model = self.models.get(token)
        if model is None:
            return 0.0
            
        try:
            # Ambil fitur yang sesuai dengan kebutuhan kolom model
            latest_data = df_features[self.feature_columns].tail(1)
            
            # Hitung nilai prediksi probabilitas
            prob_breakout = model.predict_proba(latest_data)[0][1]
            return float(prob_breakout)
            
        except Exception as e:
            # JALUR DIAGNOSTIK: Menampilkan pesan kesalahan asli ke terminal Anda
            print(f"[DEBUG LOG] Terjadi kendala prediksi pada token {token}: {str(e)}")
            return 0.0

if __name__ == "__main__":
    # Pengujian mandiri komponen engine inferensi
    engine = CryptoInferenceEngine()
    print("[*] Metadata Fitur Terdeteksi:", engine.feature_columns)
    print("[*] Status Model Terbaca:", {k: "Ready" if v is not None else "Missing" for k, v in engine.models.items()})