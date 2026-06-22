import os
import shutil
import sys
import joblib
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import xgboost.spark as xgb_spark
from src.features.feature_engineering import CryptoFeatureEngineer

def execute_model_training():
    TARGET_DATA_DIR = str(Path(__file__).resolve().parents[2] / "DATA")
    MODEL_DIR = "MODEL"
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    os.makedirs(TARGET_DATA_DIR, exist_ok=True)
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

    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("SparkXGBoostTraining") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.pyspark.driver.python", python_executable) \
        .config("spark.pyspark.python", python_executable) \
        .config("spark.executorEnv.PYSPARK_PYTHON", python_executable) \
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for token, file_path in file_paths.items():
        if not os.path.exists(file_path):
            print(f"[!] Skip training {token}, file tidak ditemukan di {file_path}")
            continue
            
        df_raw = pd.read_csv(file_path, low_memory=False)
        df_processed = engineer.build_features(df_raw, token, is_training=True)
        
        if len(df_processed) < 100:
            print(f"[!] Skip training {token}, baris data terlalu sedikit ({len(df_processed)})")
            continue
            
        df_train = df_processed[feature_columns + ["Target_Vol"]].astype(float)
        num_neg = (df_train["Target_Vol"] == 0).sum()
        num_pos = (df_train["Target_Vol"] == 1).sum()

        if num_pos == 0:
            print(f"[!] Warning: Token {token} tidak memiliki label target '1'. Menyuntikkan label darurat...")
            df_train.loc[df_train.index[-1], "Target_Vol"] = 1.0
            num_pos = 1

        weight_ratio = float(num_neg) / float(num_pos)

        print(f"[*] Melatih model XGBoost untuk Token: {token} | Rasio Bobot: {weight_ratio:.2f} | Baris Data: {len(df_train)}")

        spark_df = spark.createDataFrame(df_train)
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        spark_train = assembler.transform(spark_df).select("features", "Target_Vol")
        spark_train = spark_train.withColumnRenamed("Target_Vol", "label")

        classifier = xgb_spark.SparkXGBClassifier(
            features_col="features",
            label_col="label",
            prediction_col="prediction",
            probability_col="probability",
            num_workers=1,
            n_estimators=150,
            max_depth=4,
            learning_rate=0.03,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            scale_pos_weight=weight_ratio,
            eval_metric="logloss"
        )

        spark_model = classifier.fit(spark_train)

        spark_model_dir = os.path.join(MODEL_DIR, f"{token.lower()}_spark_model")
        if os.path.exists(spark_model_dir):
            shutil.rmtree(spark_model_dir)
        spark_model.save(spark_model_dir)

        booster_path = os.path.join(MODEL_DIR, f"{token.lower()}_xgb_model.json")
        booster = spark_model.get_booster()
        booster.save_model(booster_path)

        print(f"[+] Model {token} berhasil diperbarui dan diekspor.")

if __name__ == "__main__":
    execute_model_training()