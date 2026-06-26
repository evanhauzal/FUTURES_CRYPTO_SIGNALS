# Dockerfile khusus untuk Airflow agar menginstall library project kita
FROM apache/airflow:2.10.5-python3.11

# Berpindah ke root user untuk menginstall system dependencies jika diperlukan
USER root
RUN rm -f /etc/apt/sources.list.d/*.list \
  && apt-get update -o Acquire::ForceIPv4=true \
  && apt-get install -y --no-install-recommends \
         build-essential \
         default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Kembali ke user airflow
USER airflow

# Install library Python dari requirements project Anda
# (Termasuk yfinance, xgboost, kafka-python, cassandra-driver, dll)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
