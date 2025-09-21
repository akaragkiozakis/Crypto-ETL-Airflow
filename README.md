# Crypto ETL Pipelines with Apache Airflow

This repository contains a set of ETL pipelines built with Apache Airflow and executed via Docker Desktop.
The pipelines demonstrate how to collect, transform, and store cryptocurrency market data using APIs, MySQL, and MinIO (S3-compatible storage).
The goal is to showcase different ETL design patterns, including raw vs incremental loads, one-off initialization DAGs, streaming API ingestion, and transformation on object storage.

# Project Overview

Data Source: CoinGecko API
Destinations:
MySQL (with raw and incremental tables)
MinIO (S3-compatible) in Parquet format
CSV logs (for quick testing)
Orchestration: Apache Airflow (running on Docker Desktop)

# Technologies Used
Apache Airflow (workflow orchestration)
Python (data extraction and transformation)
Pandas (data manipulation)
Requests (API calls)
MySQL (structured data storage)
MinIO + S3FS + Parquet (data lake storage)
