# Data Engineering Pipeline - Olist E-Commerce

<img width="1334" height="874" alt="Image" src="https://github.com/user-attachments/assets/c908e929-faa8-4f0e-9a7e-6d4930cdaf0b"/>

This pipeline builds an end-to-end data engineering workflow using: -
Kafka (streaming ingestion) - Spark (data processing) - Airflow
(orchestration) - PostgreSQL (data warehouse)

The pipeline consists of: - Bronze → Silver → Gold layer - Streaming
from Kafka - Transformation using Spark - Loading into a data warehouse

## Setup & Running

### 1. Setup Environment

Create a `.env` file from the template:

cp .env.example .env

Then fill in the required configurations (Postgres, etc.).

### 2. Build Docker

Run build without cache:

docker compose build --no-cache

### 3. Run All Services

docker compose up -d

## Notes

Make sure your internet connection is stable, because Spark will
download dependencies (JAR files) during the first run.

## Access Services

### Airflow

http://localhost:8080

Used for monitoring the pipeline and triggering the DAG.

### Kafka UI

http://localhost:8085/kafkaui/

Used to check Kafka topics and monitor messages.

## Running the Pipeline

1.  Open Airflow UI
2.  Find DAG: pipeline_olist_ecommerce
3.  Click Trigger DAG

## Pipeline Flow

1.  Check Data: scan files from bronze folder
2.  Create Kafka Topic: create topics for each dataset
3.  Publish Data: send CSV data to Kafka
4.  Silver Layer: parse JSON, clean, deduplicate, save as Parquet
5.  Gold Layer: join datasets, build dimension and fact tables
6.  Setup Data Warehouse: create database and schema
7.  Load Data: insert into PostgreSQL

## Layer Structure

### Bronze

Raw CSV files

### Silver

Cleaned and standardized data, deduplicated, stored as Parquet

### Gold

Data warehouse-ready tables, including fact and dimension tables

## Troubleshooting

### Slow Spark Dependency Download

This is normal on the first run. Ensure your internet connection is
stable.
