# 📊 Arsitektur Pipeline Big Data: Futures Crypto Signals

Dokumen ini menjelaskan alur kerja (*workflow*) dan arsitektur pipeline Big Data yang digunakan di dalam proyek ini. Sistem ini dirancang untuk memproses data cryptocurrency secara *real-time* maupun *batch*, melatih model Machine Learning terdistribusi, dan menyajikan sinyal trading kepada pengguna akhir.

---

## 🏗️ Gambaran Umum Arsitektur

Pipeline ini dibagi menjadi 5 tahapan utama:
1. **Data Ingestion (Streaming)**
2. **Data Storage (NoSQL)**
3. **Orchestration (Workflow Management)**
4. **Data Processing & Machine Learning (Distributed)**
5. **Serving & Visualization (Frontend)**

---

## 🔄 1. Data Ingestion (Aliran Data Real-Time)
**Teknologi yang digunakan:** Apache Kafka, Zookeeper, Python
- Skrip (biasanya berupa *producer*) bertugas mengambil harga *real-time* cryptocurrency (OHLCV - Open, High, Low, Close, Volume) dari API Eksternal (misal: Binance/Yahoo Finance).
- Data mentah ini tidak langsung disimpan ke database, melainkan di- *publish* ke topik **Apache Kafka**.
- **Kafka** bertindak sebagai *Message Broker* yang sangat cepat dan tangguh, memungkinkan data mengalir tanpa *bottleneck*. *Zookeeper* bertugas mengoordinasikan cluster Kafka tersebut.

---

## 💾 2. Data Storage (Penyimpanan Skala Besar)
**Teknologi yang digunakan:** Apache Cassandra, Kafka Consumer
- Terdapat sebuah *service* bernama **Kafka Consumer** (berjalan terus-menerus via Docker) yang secara *real-time* membaca ( *subscribe* ) data dari topik Kafka.
- Begitu data diterima, Consumer akan menyimpannya ke dalam **Apache Cassandra** (Tabel `crypto_ks.signals`).
- **Cassandra** dipilih karena ini adalah database NoSQL terdistribusi yang sangat handal (*highly available*) untuk menyimpan data *time-series* (runtun waktu) dalam jumlah yang sangat besar secara cepat (skalabilitas tinggi untuk Big Data).

---

## ⏱️ 3. Orchestration (Penjadwalan & Otomatisasi)
**Teknologi yang digunakan:** Apache Airflow, PostgreSQL (Metadata)
- Mengelola data secara manual tidak efisien, oleh karena itu **Apache Airflow** digunakan sebagai *Orchestrator* (Dirigen).
- Airflow memiliki sekumpulan DAG (*Directed Acyclic Graph*) yang terjadwal (misal: setiap 1 jam atau setiap hari).
- Ketika jadwal tiba, Airflow tidak langsung mengeksekusi beban kerja berat di dalam containernya. Sebaliknya, Airflow membuat *file trigger* (seperti `trigger_train.txt` atau `trigger_scan.txt`) di dalam folder `DATA/`.
- Sebuah **Daemon Host (`spark_trigger_daemon.py`)** memantau file tersebut dan mengeksekusi tahapan berikutnya di komputer Host (memanfaatkan tenaga penuh laptop/server utama).

---

## 🧠 4. Data Processing & Machine Learning
**Teknologi yang digunakan:** Apache Spark (PySpark), XGBoost
Proses ini adalah inti dari kecerdasan buatan dalam sistem:
- **Feature Engineering:** Membaca jutaan baris data dari Cassandra dan menghitung indikator finansial teknikal (seperti *Bollinger Bands*, *Return Ratio*, dll) secara paralel menggunakan kecepatan in-memory dari **Apache Spark**.
- **Distributed Training:** Data hasil ekstraksi fitur kemudian diumpankan ke model **XGBoost Classifier**. Pelatihan ini didistribusikan antar *node* (Master-Worker) menggunakan Spark, sehingga proses yang tadinya butuh waktu berjam-jam bisa dipangkas drastis.
- Model memprediksi `Target_Vol` (potensi volatilitas harga ke depannya) dan menyimpannya ke dalam folder `MODEL/` sebagai file JSON.

---

## 📈 5. Serving & Visualization
**Teknologi yang digunakan:** Streamlit, Supabase, Python
- **Signal Generator:** Setelah model selesai dilatih, skrip `src.signals.generator` akan memprediksi sinyal (Beli/Jual/Tahan) untuk periode saat ini dan menyimpannya ke database cloud **Supabase**.
- **Dashboard Interaktif:** Pengguna mengakses **Streamlit** (aplikasi Web Frontend Python). Streamlit akan mengambil data prediksi dari Supabase dan menampilkannya dalam wujud *dashboard* visual yang cantik, lengkap dengan grafik pergerakan harga dan notifikasi sinyal terbaru.

---

## 💡 Ringkasan Alur (Summary Flow)

`API Market` ➔ **Kafka Producer** ➔ **Apache Kafka** (Broker) ➔ **Kafka Consumer** ➔ **Apache Cassandra** (Storage) 
<br>
*(Diatur oleh)* **Apache Airflow** ➔ Memicu **PySpark** ➔ Mengambil data dari Cassandra ➔ Fitur Ekstraksi ➔ Melatih **XGBoost**
<br>
Model Tersimpan ➔ **Signal Scanner** menghasilkan prediksi ➔ **Supabase** ➔ Tampil di **Streamlit UI**.
