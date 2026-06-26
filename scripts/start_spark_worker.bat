@echo off
echo ========================================================
echo [*] Memulai Spark Worker Node
echo ========================================================

echo [*] Memastikan winutils.exe (Hadoop) tersedia di Windows...
python "%~dp0setup_winutils.py"
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%

:: Pastikan IP ini diganti sesuai dengan IP Laptop yang bertindak sebagai Worker
set SPARK_LOCAL_IP=192.168.1.10
echo [*] SPARK_LOCAL_IP disetel ke %SPARK_LOCAL_IP%

set MASTER_URL=spark://192.168.1.10:7077
echo [*] Menghubungkan ke Master %MASTER_URL%...

echo [*] Mencari lokasi instalasi PySpark di Python...
FOR /F "tokens=*" %%i IN ('python -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))"') DO set SPARK_HOME=%%i
echo [*] SPARK_HOME ditemukan di: %SPARK_HOME%

"%SPARK_HOME%\bin\spark-class2.cmd" org.apache.spark.deploy.worker.Worker %MASTER_URL% --webui-port 8081
