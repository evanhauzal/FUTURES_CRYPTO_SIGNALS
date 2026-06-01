import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit, avg, sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

print("\n" + "="*70)
print("  MESIN ANALISIS QUANTITATIVE FUNDAMENTAL FUSION - APACHE SPARK  ")
print("="*70 + "\n")

# 1. Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("PepeAdvancedFundamentalFusion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Skema Data On-Chain ($PEPE Transaction)
pepe_schema = StructType([
    StructField("type", StringType(), True),
    StructField("volume", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("usd_value", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("tx_hash", StringType(), True)
])

# 3. Skema Data Off-Chain (News Sentiment)
news_schema = StructType([
    StructField("source", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
    StructField("is_pepe_target", BooleanType(), True)
])

# 4. Ambil Stream dari Dua Topic Kafka Sekaligus
pepe_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "trading-pepe").load()

news_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "trading-news").load()

# 5. Parsing Masing-Masing Stream Data
pepe_stream = pepe_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), pepe_schema).alias("data")).select("data.*")

news_stream = news_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), news_schema).alias("data")).select("data.*")

# 6. Registrasi ke Temporary View untuk pemrosesan SQL kompleks
pepe_stream.createOrReplaceTemporaryView("v_pepe_trades")
news_stream.createOrReplaceTemporaryView("v_news_sentiment")

# 7. LOGIKA MATRIKS FUNDAMENTAL (Kombinasi On-Chain + Off-Chain)
# Kita menghitung agregat transaksi dan sentimen secara bersamaan di memori RAM
fused_analytics_query = spark.sql("""
    WITH pepe_agg AS (
        SELECT 
            SUM(CASE WHEN type = 'BUY' THEN usd_value ELSE 0 END) as total_buy_volume,
            SUM(CASE WHEN type = 'SELL' THEN usd_value ELSE 0 END) as total_sell_volume,
            COUNT(tx_hash) as total_trades,
            AVG(price) as current_avg_price
        FROM v_pepe_trades
    ),
    news_agg AS (
        SELECT 
            COALESCE(AVG(sentiment_score), 0.0) as avg_pepe_sentiment
        FROM v_news_sentiment
        WHERE is_pepe_target = true
    )
    SELECT 
        p.total_trades,
        p.current_avg_price,
        (p.total_buy_volume - p.total_sell_volume) as net_volume_flow_usd,
        n.avg_pepe_sentiment,
        
        -- LOGIKA VALIDASI: Tidak asal menyimpulkan tren pasar
        CASE 
            WHEN (p.total_buy_volume > p.total_sell_volume) AND (n.avg_pepe_sentiment > 0.1) 
                THEN '✅ STRONG BULLISH (Valid: Data On-Chain & Sentimen Kompak Naik)'
            WHEN (p.total_sell_volume > p.total_buy_volume) AND (n.avg_pepe_sentiment < -0.1) 
                THEN '❌ STRONG BEARISH (Valid: Tekanan Jual Tinggi & Berita Negatif)'
            WHEN (p.total_buy_volume > p.total_sell_volume) AND (n.avg_pepe_sentiment <= 0.1) 
                THEN '⚠️ FALSE PUMP ALERT (Anomali: Harga Naik tapi Fundamental Berita Lemah)'
            ELSE '⏳ SIDEWAYS / MARKET NEUTRAL (Menunggu Konformitas Data)'
        END as fundamental_conclusion
    FROM pepe_agg p
    CROSS JOIN news_agg n
""")

# 8. Output Dashboard Analisis Matriks ke Terminal
query = fused_analytics_query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()