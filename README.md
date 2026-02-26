# ETL-Pipeline-GCS-to-BigQuery-using-Airflow
# Project Overview

This project demonstrates a cloud-based ETL pipeline that:
Extracts a CSV file from Google Cloud Storage (GCS)
Loads data into a raw table in BigQuery
Transforms the raw data into a curated analytics-ready table
Uses Apache Airflow as the orchestration tool
The pipeline follows a modern ELT architecture where transformations are executed inside BigQuery.

# Architecture Overview
Data Flow:
GCS (house-price.csv)
→ Airflow DAG
→ BigQuery Raw Table
→ BigQuery Curated Table

# Technologies Used
Apache Airflow (Workflow Orchestration)
Google Cloud Storage (Data Lake / Raw Storage)
BigQuery (Cloud Data Warehouse)
Python (Airflow DAG)
SQL (Data Transformation)
