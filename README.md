# Data Engineering Pipeline - Olist E-Commerce

<img width="1334" height="874" alt="Image" src="https://github.com/user-attachments/assets/c908e929-faa8-4f0e-9a7e-6d4930cdaf0b" />

Pipeline ini membangun end-to-end data engineering workflow menggunakan:
- Kafka (streaming ingestion)
- Spark (data processing)
- Airflow (orchestration)
- PostgreSQL (data warehouse)

Pipeline terdiri dari:
- Bronze → Silver → Gold layer
- Streaming dari Kafka
- Transformasi dengan Spark
- Load ke data warehouse

## Setup & Running

### 1. Setup Environment

Buat file `.env` dari template:

cp .env.example .env

Lalu isi sesuai kebutuhan (Postgres, dll).

### 2. Build Docker

Jalankan build tanpa cache:

docker compose build --no-cache

### 3. Jalankan Semua Service

docker compose up -d

## Catatan

Pastikan koneksi internet stabil karena Spark akan download dependencies (JAR) saat pertama kali dijalankan.

## Akses Service

### Airflow
http://localhost:8080

Digunakan untuk monitoring pipeline dan trigger DAG.

### Kafka UI
http://localhost:8085/kafkaui/

Digunakan untuk cek topic Kafka dan monitor message.

## Menjalankan Pipeline

1. Buka Airflow UI
2. Cari DAG: pipeline_olist_ecommerce
3. Klik Trigger DAG

## Alur Pipeline

1. Check Data: scan file dari folder bronze
2. Create Kafka Topic: membuat topic sesuai dataset
3. Publish Data: kirim data CSV ke Kafka
4. Silver Layer: parsing JSON, cleaning, deduplication, simpan ke Parquet
5. Gold Layer: join dataset, build dimension dan fact table
6. Setup Data Warehouse: create database dan schema
7. Load Data: insert ke PostgreSQL

## Struktur Layer

### Bronze
Raw CSV

### Silver
Cleaned dan standardized data, deduplicated, disimpan sebagai Parquet

### Gold
Data warehouse ready, terdiri dari fact dan dimension table

## Troubleshooting

### Spark download lama
Normal pada first run, pastikan koneksi internet stabil

