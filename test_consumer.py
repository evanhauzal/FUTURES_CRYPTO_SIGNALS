from kafka import KafkaConsumer
import json
import sys

print("=== Menunggu Data Masuk dari Kafka Topic: trading-pepe ===")
sys.stdout.flush()

try:
    # Menggunakan value_deserializer untuk membaca data JSON dari Kafka
    consumer = KafkaConsumer(
        'trading-pepe',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Loop ini akan terus menggantung selama pipeline mengirimkan data
    for message in consumer:
        data = message.value
        print(f"[KAFKA RECEIVE] Data Baru Masuk: {data}")
        sys.stdout.flush()

except Exception as e:
    print(f"Terjadi error pada Consumer: {e}")