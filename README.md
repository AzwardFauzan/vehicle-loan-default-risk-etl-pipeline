# Vehicle Loan Default Risk ETL Pipeline

## 📌 Deskripsi Proyek

Proyek ini merupakan hasil kolaborasi antara **Tim Data Analyst** (Herlina & Marsyal) dan **Tim Data Engineer** (Azward & Agung) untuk menganalisis faktor-faktor yang memengaruhi risiko gagal bayar (loan default) pada pinjaman kendaraan menggunakan **NBFI Vehicle Loan Repayment Dataset**. Dataset ini berisi informasi profil nasabah, detail pinjaman, riwayat kredit, dan status pembayaran yang digunakan untuk mengidentifikasi karakteristik nasabah berisiko tinggi. :contentReference[oaicite:0]{index=0}

Pada proyek ini, **Tim Data Engineer** bertanggung jawab membangun **ETL Pipeline** menggunakan **PySpark** dan **Apache Airflow**, melakukan transformasi data, serta memuat data ke **PostgreSQL NeonDB** dengan pendekatan **Star Schema** sebagai data warehouse. Selanjutnya, **Tim Data Analyst** melakukan analisis data dan membangun dashboard interaktif menggunakan **Tableau** untuk menyajikan insight mengenai risiko gagal bayar dan performa pinjaman.

Proyek ini dibuat sebagai bagian dari assignment **Data Engineering & Data Analytics** untuk mengimplementasikan proses ETL end-to-end, data warehouse, dan visualisasi data sebagai pendukung pengambilan keputusan berbasis data.

---

## 🎯 Tujuan

- Membangun ETL Pipeline menggunakan PySpark dan Apache Airflow.
- Melakukan proses ekstraksi, transformasi, dan pemuatan data ke PostgreSQL NeonDB.
- Merancang data warehouse menggunakan Star Schema.
- Menganalisis faktor-faktor yang memengaruhi risiko gagal bayar pinjaman kendaraan.
- Menyajikan insight bisnis melalui dashboard interaktif menggunakan Tableau.

---

## 🔄 Alur ETL

### 1. Extract
- Mengambil dataset **NBFI Vehicle Loan Repayment** dari Kaggle.
- Membaca dataset menggunakan PySpark sebagai proses awal ETL.

### 2. Transform
- Membersihkan data dan menangani missing values.
- Menyesuaikan tipe data pada setiap kolom.
- Melakukan transformasi dan standarisasi data.
- Membentuk tabel dimensi dan tabel fakta menggunakan pendekatan Star Schema.

### 3. Load
- Memuat hasil transformasi ke PostgreSQL NeonDB.
- Menjadikan data warehouse sebagai sumber utama analisis dan dashboard Tableau.

---

## 👥 Tim Proyek

### Data Engineer
- Azward Nurfauzan
- Yohanes Agung

**Tanggung Jawab:**
- ETL Pipeline menggunakan PySpark
- Workflow Automation menggunakan Apache Airflow
- Data Cleaning & Transformation
- Data Warehouse Design (Star Schema)
- PostgreSQL NeonDB

### Data Analyst
- Herlina
- Marsyal Renjiro

**Tanggung Jawab:**
- Exploratory Data Analysis (EDA)
- Business Insight
- Dashboard Development
- Tableau Visualization

---

## 🛠️ Tools & Technologies

- Python
- PySpark
- Apache Airflow
- PostgreSQL (NeonDB)
- Tableau
- SQL

---

## 📊 Dataset

Dataset yang digunakan adalah **NBFI Vehicle Loan Repayment Dataset**, yang berisi data nasabah, informasi pinjaman, riwayat kredit, dan status pembayaran untuk menganalisis risiko gagal bayar pada pinjaman kendaraan. Dataset ini digunakan sebagai dasar pembangunan ETL pipeline, data warehouse, serta dashboard analitik. :contentReference[oaicite:1]{index=1}

**Sumber Dataset:**

https://www.kaggle.com/datasets/meastanmay/nbfi-vehicle-loan-repayment-dataset

---

## 📈 Dashboard Tableau

Dashboard interaktif dapat diakses melalui:

**🔗 Tableau Public:**  
https://public.tableau.com/app/profile/herlina.4840/viz/FinalProjectGroup1_17830750443220/Dashboard12?publish=yes

---

## 📌 Hasil

Proyek ini berhasil membangun **ETL Pipeline** menggunakan **PySpark** dan **Apache Airflow** untuk mengolah dataset pinjaman kendaraan secara otomatis. Data hasil transformasi berhasil dimuat ke **PostgreSQL NeonDB** dengan desain **Star Schema**, sehingga lebih optimal untuk kebutuhan analisis.

Selanjutnya, data divisualisasikan menggunakan **Tableau** untuk menyajikan insight mengenai karakteristik nasabah, tingkat risiko gagal bayar, distribusi pinjaman, serta faktor-faktor yang memengaruhi performa pembayaran. Dashboard yang dihasilkan membantu mempermudah eksplorasi data dan mendukung pengambilan keputusan berbasis data.
