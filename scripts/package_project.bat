@echo off
echo ========================================================
echo [*] Mem-package Python Source Code untuk Spark
echo ========================================================

if not exist dist mkdir dist

echo [*] Mengompres folder src ke dalam dist/project.zip...
powershell -noprofile -command "Compress-Archive -Path .\src\* -DestinationPath .\dist\project.zip -Force"

echo [+] Berhasil! Anda sekarang dapat menjalankan:
echo spark-submit --master spark://192.168.1.10:7077 --py-files dist/project.zip src/models/train_model.py
