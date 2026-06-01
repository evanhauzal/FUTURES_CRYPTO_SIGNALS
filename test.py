import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="trading_db",
    user="postgres",
    password="Naya110212"
)

print("SUCCESS CONNECT")