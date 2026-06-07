import os
import joblib
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from src.features.feature_engineering import CryptoFeatureEngineer

def execute_model_training():
    TARGET_DATA_DIR = r"C:\Users\hp\Documents\PROJECT ROSBD\TOOLS-TRADING-ROSBD\DATA"
    MODEL_DIR = "MODEL"
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    engineer = CryptoFeatureEngineer(data_dir=TARGET_DATA_DIR)
    
    file_paths = {
        "BTC": os.path.join(TARGET_DATA_DIR, "BTC_1h.csv"),
        "ETH": os.path.join(TARGET_DATA_DIR, "ETH_1h.csv"),
        "SOL": os.path.join(TARGET_DATA_DIR, "SOL_1h.csv"),
        "XRP": os.path.join(TARGET_DATA_DIR, "XRP_1h.csv"),
        "BNB": os.path.join(TARGET_DATA_DIR, "BNB_1h.csv")
    }
    
    feature_columns = [
        "Return_1h", "Return_3h", "Return_12h", 
        "BB_Position", "Volume_Ratio", "BTC_Vol_1h", "BTC_Vol_3h",
        "Trend_Direction", "Volatility_Regime"
    ]
    joblib.dump(feature_columns, os.path.join(MODEL_DIR, "feature_columns.pkl"))

    print("\n" + "="*50)
    print("[*] MEMULAI RE-TRAINING MULTI-TOKEN MODEL XGBOOST")
    print("="*50)

    for token, file_path in file_paths.items():
        if not os.path.exists(file_path):
            print(f"[!] Skip training {token}, file tidak ditemukan di {file_path}")
            continue
            
        df_raw = pd.read_csv(file_path, low_memory=False)
        df_processed = engineer.build_features(df_raw, token, is_training=True)
        
        # Validasi kecukupan data setelah disinkronisasi
        if len(df_processed) < 100:
            print(f"[!] Skip training {token}, baris data terlalu sedikit ({len(df_processed)})")
            continue
            
        X = df_processed[feature_columns]
        y = df_processed["Target_Vol"].astype(int)
        
        # Hitung rasio perbandingan kelas untuk mengatasi imbalance data secara dinamis
        num_neg = (y == 0).sum()
        num_pos = (y == 1).sum()
        
        if num_pos == 0:
            print(f"[!] Warning: Token {token} tidak memiliki label target '1'. Menyuntikkan label darurat...")
            y.iloc[-1] = 1
            num_pos = 1
            
        weight_ratio = float(num_neg) / float(num_pos)
        
        # Menggunakan parameter test_size yang valid untuk Scikit-Learn
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=0.25, random_state=42, stratify=y
        )
        
        print(f"[*] Melatih model XGBoost untuk Token: {token} | Rasio Bobot: {weight_ratio:.2f} | Baris Data: {len(X_train)}")
        model = XGBClassifier(
            n_estimators=150, 
            max_depth=4, 
            learning_rate=0.03,
            subsample=0.8, 
            colsample_bytree=0.8, 
            min_child_weight=3,
            scale_pos_weight=weight_ratio,
            random_state=42, 
            eval_metric='logloss', 
            n_jobs=-1
        )
        model.fit(X_train, y_train)
        
        model_save_path = os.path.join(MODEL_DIR, f"{token.lower()}_xgb_model.pkl")
        joblib.dump(model, model_save_path)
        print(f"[+] Model {token} berhasil diperbarui dan diekspor.")

if __name__ == "__main__":
    execute_model_training()