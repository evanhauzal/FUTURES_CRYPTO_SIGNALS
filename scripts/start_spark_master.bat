@echo off
echo ========================================================
echo [*] Memulai Spark Master Server
echo ========================================================

set SPARK_LOCAL_IP=192.168.1.10
echo [*] SPARK_LOCAL_IP disetel ke %SPARK_LOCAL_IP%

echo [*] Menjalankan Master (Port 7077, WebUI Port 8080)...
spark-class org.apache.spark.deploy.master.Master --ip 192.168.1.10 --port 7077 --webui-port 8080
