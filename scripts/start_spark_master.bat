@echo off
echo ========================================================
echo [*] Memulai Spark Master Server
echo ========================================================

set SPARK_LOCAL_IP=192.168.1.10
echo [*] SPARK_LOCAL_IP disetel ke %SPARK_LOCAL_IP%

echo [*] Mencari lokasi instalasi PySpark di Python...
FOR /F "tokens=*" %%i IN ('python -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))"') DO set SPARK_HOME=%%i
echo [*] SPARK_HOME ditemukan di: %SPARK_HOME%

echo [*] Menjalankan Master (Port 7077, WebUI Port 8080)...
"%SPARK_HOME%\bin\spark-class2.cmd" org.apache.spark.deploy.master.Master --ip 192.168.1.10 --port 7077 --webui-port 8080
