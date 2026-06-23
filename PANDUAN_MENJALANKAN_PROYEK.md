# 🚀 Panduan Lengkap Menjalankan Proyek ROSBD Crypto Signals

Proyek ini adalah sistem terdistribusi yang menggabungkan aliran data *real-time* (Kafka), penyimpanan Big Data (Cassandra), Orkestrasi (Airflow), Machine Learning, dan Frontend interaktif (Streamlit).

Berikut adalah langkah-langkah untuk menjalankan proyek ini dari awal (Start-to-Finish).

---

## 1️⃣ Persiapan Infrastruktur (Docker)
Proyek ini sangat bergantung pada Docker untuk menjalankan Kafka, Cassandra, dan Apache Airflow.

1. Buka **Docker Desktop** dan pastikan statusnya *Running*.
2. Buka terminal (Command Prompt / PowerShell) di folder proyek ini (`FUTURES_CRYPTO_SIGNALS`).
3. Jalankan perintah berikut untuk menghidupkan seluruh infrastruktur:
   ```bash
   docker compose up -d
   ```
4. Tunggu beberapa menit. Anda bisa mengecek status container di aplikasi Docker Desktop. Pastikan `airflow-webserver`, `kafka`, dan `cassandra` berstatus *Running* (hijau).

---

## 2️⃣ Persiapan Database Supabase (IPv4 Connection Pooler)
Karena kita menggunakan Docker, koneksi ke Supabase harus menggunakan alamat IPv4 (Connection Pooler) agar terhindar dari pemblokiran jaringan internal Docker.

1. Buka Dashboard Supabase.
2. Masuk ke **Project Settings -> Database -> Connection pooler**.
3. Pastikan *Use connection pooling* dicentang.
4. Salin alamat Host (berakhiran `pooler.supabase.com`) dan Port (biasanya `6543`).
5. Buka file `.env` dan `.env.airflow` di folder proyek ini.
6. Ganti nilai `DB_HOST` dan `DB_PORT` dengan data dari Connection Pooler tersebut.
7. Restart Airflow: `docker compose restart`

---

## 3️⃣ Mengaktifkan Orkestrasi Pipeline (Apache Airflow)
Airflow bertugas sebagai "otak" yang menjadwalkan penarikan harga kripto, melatih model Machine Learning, dan menarik berita geopolitik.

1. Buka browser dan akses Airflow UI: **http://localhost:8080**
2. Login menggunakan:
   - **Username:** `admin`
   - **Password:** `admin`
3. Anda akan melihat dua DAG utama:
   - `crypto_price_pipeline`: Pipeline untuk menarik harga, mengirim ke Kafka, menyimpan ke Cassandra, dan Training Model.
   - `crypto_news_pipeline`: Pipeline untuk menarik berita dan sentimen.
4. Geser tombol *Toggle* di sebelah kiri nama DAG dari **Paused** menjadi **Unpaused** (warna biru).
5. (Opsional) Anda bisa menekan tombol **Play (Trigger DAG)** di ujung kanan untuk memaksanya berjalan saat itu juga tanpa menunggu jadwal.

> **Catatan:** Jika ada task yang merah (gagal) karena *history* lama, klik task tersebut lalu klik **Clear** agar dijalankan ulang dengan kondisi library terbaru.

---

## 4️⃣ Menjalankan Kafka Consumer (Pemrosesan Streaming)
Pipeline harga Airflow (di atas) akan menembakkan data ke Kafka. Kita perlu sebuah Consumer untuk mendengarkannya dan meneruskannya ke Database Cassandra.

1. Buka **Terminal Baru** (biarkan berjalan di latar belakang).
2. Pastikan Anda berada di dalam folder `FUTURES_CRYPTO_SIGNALS`.
3. Jalankan Kafka Consumer:
   ```bash
   python -m src.ingestion.kafka_consumer
   ```
4. Anda akan melihat log data kripto masuk setiap kali Airflow mengeksekusi pipeline harganya.

---

## 5️⃣ Menjalankan Dashboard Visual (Streamlit)
Langkah terakhir adalah menghidupkan antarmuka pengguna (Frontend) untuk memantau harga real-time dan sinyal Machine Learning.

1. Buka **Terminal Baru** (lagi).
2. Pastikan Anda berada di dalam folder proyek.
3. Jalankan perintah Streamlit:
   ```bash
   streamlit run FRONTEND/app.py
   ```
4. Browser akan otomatis membuka halaman **http://localhost:8501**.
5. Dashboard ini sudah dilengkapi dengan sistem **Anti-Blokir ISP** (menggunakan Yahoo Finance) sehingga harga dolar kripto akan bergerak secara *real-time* setiap beberapa detik!

---

## 🛑 Cara Mematikan Sistem
Jika Anda sudah selesai dan ingin mematikan semuanya agar RAM laptop kembali lega:
1. Matikan terminal yang menjalankan **Streamlit** dan **Kafka Consumer** (Tekan `Ctrl + C`).
2. Matikan infrastruktur Docker dengan menjalankan perintah:
   ```bash
   docker compose down
   ```
