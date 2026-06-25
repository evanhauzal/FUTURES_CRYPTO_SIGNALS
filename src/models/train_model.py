import os
import shutil
import sys
import joblib
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import xgboost.spark as xgb_spark
from cassandra.cluster import Cluster
from src.features.feature_engineering import CryptoFeatureEngineer
import yaml

# Default configuration
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "local[*]")
SPARK_NUM_WORKERS = 1

# Coba baca config YAML jika ada
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "spark_cluster.yml"
if CONFIG_PATH.exists():
    try:
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f)
            if "spark_cluster" in cfg:
                scfg = cfg["spark_cluster"]
                CASSANDRA_HOST = scfg.get("cassandra_host", CASSANDRA_HOST)
                CASSANDRA_PORT = scfg.get("cassandra_port", CASSANDRA_PORT)
                SPARK_MASTER = f"spark://{scfg['master_ip']}:{scfg['master_port']}"
                SPARK_NUM_WORKERS = 2  # default untuk 1 master + 1 worker setup
    except Exception as e:
        print(f"[!] Gagal membaca spark_cluster.yml: {e}")
CASSANDRA_KEYSPACE = "crypto_ks"
CASSANDRA_TABLE = "signals"


def load_token_from_cassandra(session, token: str) -> pd.DataFrame:
    """
    Membaca data OHLCV untuk satu token dari Cassandra dan 
    mengembalikannya sebagai Pandas DataFrame.
    """
    query = f'SELECT datetime, open, high, low, close, volume FROM signals WHERE "token" = \'{token}\''
    rows = session.execute(query)
    
    data = []
    for row in rows:
        data.append({
            "Datetime": row.datetime,
            "Open": row.open,
            "High": row.high,
            "Low": row.low,
            "Close": row.close,
            "Volume": row.volume
        })
    
    df = pd.DataFrame(data)
    if not df.empty:
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.sort_values("Datetime").reset_index(drop=True)
    return df


def execute_model_training():
    TARGET_DATA_DIR = str(Path(__file__).resolve().parents[2] / "DATA")
    MODEL_DIR = "MODEL"
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    os.makedirs(TARGET_DATA_DIR, exist_ok=True)

    print("\n" + "="*50)
    print("[*] MEMULAI RE-TRAINING MULTI-TOKEN MODEL XGBOOST")
    print("[*] Sumber data: Cassandra (crypto_ks.signals)")
    print("="*50)

    # --- Koneksi ke Cassandra (harus sebelum engineer) ---
    print(f"[*] Menghubungkan ke Cassandra ({CASSANDRA_HOST}:{CASSANDRA_PORT})...")
    cass_cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    cass_session = cass_cluster.connect(CASSANDRA_KEYSPACE)
    print("[+] Cassandra terhubung.")

    engineer = CryptoFeatureEngineer(data_dir=TARGET_DATA_DIR, cassandra_session=cass_session)

    TOKENS = ["BTC", "ETH", "SOL", "XRP", "BNB"]

    feature_columns = [
        "Return_1h", "Return_3h", "Return_12h", 
        "BB_Position", "Volume_Ratio", "BTC_Vol_1h", "BTC_Vol_3h",
        "Trend_Direction", "Volatility_Regime"
    ]
    joblib.dump(feature_columns, os.path.join(MODEL_DIR, "feature_columns.pkl"))

    # Gunakan pemanggilan 'python' secara umum daripada path absolut
    # Ini mencegah error jika lokasi instalasi Python di Laptop 1 dan Laptop 2 berbeda
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Jika jalan di cluster, pastikan bindAddress pakai 0.0.0.0 agar bisa diakses
    bind_addr = "127.0.0.1" if SPARK_MASTER.startswith("local") else "0.0.0.0"

    print(f"[*] Menjalankan Spark dengan Master URL: {SPARK_MASTER}")

    spark = SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("SparkXGBoostTraining") \
        .config("spark.driver.bindAddress", bind_addr) \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT)) \
        .config("spark.pyspark.driver.python", python_executable) \
        .config("spark.pyspark.python", python_executable) \
        .config("spark.executorEnv.PYSPARK_PYTHON", python_executable) \
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", python_executable) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for token in TOKENS:
        print(f"\n--- Token: {token} ---")
        
        # Membaca data dari Cassandra (bukan dari CSV)
        df_raw = load_token_from_cassandra(cass_session, token)
        
        if df_raw.empty:
            print(f"[!] Skip training {token}, tidak ada data di Cassandra.")
            continue
            
        print(f"[*] {len(df_raw)} baris data {token} berhasil dibaca dari Cassandra.")
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
            num_workers=SPARK_NUM_WORKERS,
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

        booster_path = os.path.join(MODEL_DIR, f"{token.lower()}_xgb_model.json")
        booster = spark_model.get_booster()
        booster.save_model(booster_path)

        print(f"[+] Model {token} berhasil diperbarui dan diekspor (Booster Only).")

    # --- Cleanup koneksi Cassandra ---
    cass_cluster.shutdown()
    print("\n[+] Koneksi Cassandra ditutup. Training selesai.")

if __name__ == "__main__":
    execute_model_training()