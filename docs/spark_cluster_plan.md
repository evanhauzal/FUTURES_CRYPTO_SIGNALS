Rencana Deploy Cluster Spark 2-Node (LAN)

Tujuan
- Menjalankan Spark terdistribusi (Spark Standalone) di dua laptop pada jaringan LAN yang sama untuk mendistribusikan beban pelatihan.

Asumsi
- Kedua mesin menjalankan Windows (atau Windows + Linux — sesuaikan perintah jika perlu).
- Kedua mesin memiliki versi Python yang sama dan paket yang identik terinstal (`pyspark==3.5.6`, `xgboost`, dan dependensi lain).
- Root proyek dapat disinkronkan atau dideploy dari master ke worker.

Langkah Tingkat Tinggi (belum mengubah kode Python)
1. Pilih arsitektur
   - Opsi A (direkomendasikan): Cluster Spark Standalone dengan satu master dan satu worker.
   - Opsi B: Jalankan via `spark-submit` dengan `--master spark://<master-host>:7077`.

2. Persiapan mesin
   - Instal Java JDK (JDK 8+ / 11+) dan atur `JAVA_HOME`.
   - Instal Python versi yang sama, buat virtualenv, lalu pasang paket dari `requirements.txt` di kedua mesin.
   - Pastikan `SPARK_HOME` diset dan `spark/bin` ada di `PATH`.

3. Jaringan & firewall
   - Pastikan kedua mesin dapat saling diakses lewat hostname atau alamat IP di LAN.
   - Buka port minimal berikut:
     - 7077 (Spark master),
     - 8080 (web UI Spark master),
     - 4040 (web UI aplikasi, bersifat ephemeral),
     - port untuk RPC dan worker (rentang ephemeral). Disarankan menambahkan aturan inbound pada Windows Firewall untuk Java/Python atau menonaktifkan firewall sementara saat pengujian.

4. Strategi akses data (pilih salah satu)
   - Sistem berkas bersama: mount folder bersama (Samba) pada path yang sama di kedua mesin (mis. `\\master\\data` atau drive yang dipetakan), lalu tempatkan `DATA/` di lokasi tersebut.
   - Salin data ke setiap node sebelum menjalankan (robocopy/rsync) dan pastikan `TARGET_DATA_DIR` yang dipakai kode ada di setiap worker.
   - HDFS: opsi lebih kompleks untuk skala besar (opsional).

5. Variabel lingkungan & pengaturan Spark
   - Set `PYSPARK_PYTHON` dan `PYSPARK_DRIVER_PYTHON` ke path Python lengkap pada setiap node.
   - Set `spark.driver.host` dan `spark.driver.bindAddress` pada master jika driver berjalan di master dan perlu dijangkau oleh worker.
   - Gunakan `spark.executorEnv.PYSPARK_PYTHON` untuk meneruskan path Python ke executor.

6. Packaging dan pengiriman aplikasi
   - Opsi A: Gunakan `spark-submit --py-files` untuk mengirim paket `zip`/`egg` dari proyek.
   - Opsi B: Pasang proyek sebagai paket pip di setiap node.
   - Sediakan skrip pembuatan paket mis. `dist/project.zip` atau `scripts/package_project.bat` untuk membuat zip dari `src/` dan kirim lewat `--py-files`.

7. Menyalakan cluster (contoh)
   - Di laptop master:
     - Jalankan `SPARK_HOME\\sbin\\start-master.cmd` (atau `start-master.sh` di WSL)
     - Catat URL master: `spark://<master-ip>:7077`
   - Di laptop worker:
     - Jalankan `SPARK_HOME\\sbin\\start-worker.cmd spark://<master-ip>:7077`
   - Alternatif: jalankan worker secara manual dengan env vars yang benar.

8. Menjalankan pelatihan (contoh)
   - Dari master: jalankan
```powershell
spark-submit --master spark://<master-ip>:7077 --py-files dist/project.zip "python -m src.models.train_model"
```
   - Pastikan jalur kode dan folder `MODEL/`/`DATA/` dapat ditulis/dibaca sesuai strategi data yang dipilih.

9. Pengujian dan validasi
   - Jalankan job Spark minimal (mis. `sc.parallelize(range(100)).count()`) untuk memvalidasi cluster.
   - Jalankan `src.models.train_model` dengan `num_workers=1` lalu `num_workers=2` untuk memastikan distribusi kerja.
   - Verifikasi file model muncul di folder `MODEL/` yang diharapkan (baik di shared mount atau per-node sesuai strategi).

10. Daftar pengecekan troubleshooting
   - Jika Python worker gagal terhubung: pastikan `PYSPARK_PYTHON` menunjuk ke interpreter yang benar, `SPARK_LOCAL_IP` atau `spark.driver.bindAddress` diatur, dan aturan firewall memperbolehkan koneksi.
   - Jika data tidak ditemukan: periksa mount bersama atau proses penyalinan file.
   - Jika versi tidak cocok: pastikan versi Python, paket, dan Java identik di semua node.

File yang disarankan dibuat
- `docs/spark_cluster_plan.md` (file ini)
- `scripts/start_spark_master.bat` dan `scripts/start_spark_worker.bat` (perintah berbungkus dengan env vars)
- `scripts/package_project.bat` (membuat `dist/project.zip`)
- `config/spark_cluster.yml` (hostname, master_url, python_executable, data_strategy)
- Perbarui `README.md` dengan seksi "Distributed training over LAN" yang merujuk ke skrip ini.

Langkah selanjutnya yang bisa saya lakukan (pilih salah satu)
- Buatkan skrip helper dan `config/spark_cluster.yml` (tanpa mengubah kode Python).
- Buatkan skrip packaging `dist/project.zip`.
- Pandu Anda langkah-per-langkah untuk menyiapkan kedua laptop secara interaktif.

Catatan
- Jangan ubah kode pelatihan Python saat ini; perubahan akhir hanya mengarahkan `SparkSession.builder.master` ke `spark://<master-ip>:7077` atau menjalankan lewat `spark-submit --master`.
- Di Windows, gunakan skrip `sbin` yang disediakan Spark atau gunakan WSL untuk konsistensi shell.
