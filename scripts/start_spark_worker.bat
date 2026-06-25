@echo off
echo ========================================================
echo [*] Memulai Spark Worker Node
echo ========================================================

:: Pastikan IP ini diganti sesuai dengan IP Laptop yang bertindak sebagai Worker
set SPARK_LOCAL_IP=192.168.1.20
echo [*] SPARK_LOCAL_IP disetel ke %SPARK_LOCAL_IP%

set MASTER_URL=spark://192.168.1.10:7077
echo [*] Menghubungkan ke Master %MASTER_URL%...

spark-class org.apache.spark.deploy.worker.Worker %MASTER_URL% --webui-port 8081
