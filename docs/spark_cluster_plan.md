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

---

# Rencana Integrasi Big Data Pipeline (Kafka & Cassandra)

Selain arsitektur Spark Standalone di atas, kita akan menambahkan Kafka dan Cassandra ke dalam pipeline untuk memfasilitasi streaming data dan penyimpanan terdistribusi. Berikut adalah rancangan alurnya:

## 1. Arsitektur Data Pipeline
- **Data Ingestion (Kafka Producer)**: Script Python mengambil data sinyal crypto dari sumber (misal: API eksternal) dan melakukan *publish* data tersebut ke sebuah topik di Kafka.
- **Data Storage (Kafka Consumer ke Cassandra)**: Script Consumer membaca *stream* pesan dari Kafka dan menyimpannya secara persisten ke tabel di dalam database Cassandra. Cassandra bertindak sebagai *Data Lake / Data Warehouse* utama.
- **Model Training (Spark + Cassandra)**: Aplikasi Spark yang berjalan di klaster (Master & Worker) membaca data *training* langsung dari Cassandra menggunakan `spark-cassandra-connector`.
- **Model XGBoost**: Data yang dibaca oleh Spark dari Cassandra akan diproses, kemudian dilatih secara terdistribusi menggunakan library XGBoost untuk Spark.

## 2. Persiapan Infrastruktur Tambahan (Docker di Laptop Master)
Karena kita ingin mempermudah instalasi dan memastikan Kafka serta Cassandra bisa berkomunikasi lewat LAN, kita akan **menggunakan Docker (Docker Compose)** dan menjalankannya cukup di **satu laptop (Laptop Master)**. 

1. **Docker Compose**:
   - Kita akan membuat file `docker-compose.yml` di direktori proyek.
   - File ini akan menjalankan 3 *container*: **Zookeeper**, **Kafka**, dan **Cassandra**.
   
2. **Komunikasi LAN (Port Mapping)**:
   - **Kafka**: Meng-expose port `9092` (`-p 9092:9092`) dan disetel dengan konfigurasi `KAFKA_ADVERTISED_LISTENERS` mengarah ke IP LAN Master. Ini memungkinkan Laptop Worker untuk mem-publish atau men-subscribe pesan Kafka.
   - **Cassandra**: Meng-expose port `9042` (`-p 9042:9042`) agar Spark di Laptop Worker bisa melakukan proses query dari dan ke Cassandra menggunakan `spark-cassandra-connector`.
   - Pastikan *Windows Firewall* di Laptop Master mengizinkan koneksi *inbound* untuk port 9092 dan 9042.

3. **Inisialisasi Cassandra**:
   - Setelah Cassandra berjalan, kita akan mengeksekusi perintah inisialisasi untuk membuat *Keyspace* (misal: `crypto_ks`) dan *Table* (misal: `signals`) untuk menampung data.

## 3. Komponen Kode yang Akan Ditambahkan/Diubah
- `src/data/kafka_producer.py`: Script untuk mengambil data dan publish ke Kafka.
- `src/data/kafka_to_cassandra.py`: Script consumer untuk insert data dari Kafka ke Cassandra.
- `src/models/train_model.py`: Diubah untuk menambahkan konfigurasi koneksi Cassandra (`spark.cassandra.connection.host`) di `SparkSession` dan membaca DataFrame menggunakan `.format("org.apache.spark.sql.cassandra")`. Akan ditambahkan library XGBoost untuk PySpark.
- `requirements.txt`: Tambahan `kafka-python` dan `cassandra-driver`.
