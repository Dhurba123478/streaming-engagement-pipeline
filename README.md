# Streaming Engagement Pipeline

## Overview
This project demonstrates a senior-style data engineering pipeline built in a Databricks-style workflow using PySpark and Spark SQL. The pipeline processes raw streaming engagement events, applies validation and deduplication, and creates curated business metrics that are ready for downstream analytics and Snowflake loading.

## Architecture
Bronze → Silver → Gold

- **Bronze:** Raw event ingestion and type casting
- **Silver:** Data cleaning, validation, and deduplication
- **Gold:** Aggregated business metrics for analytics consumption

## Business Problem
A streaming platform generates user engagement events such as play, pause, and complete. The business needs reliable engagement data to measure user activity, content performance, and repeat engagement behavior.

## Tools Used
- Databricks
- PySpark
- Spark SQL
- Parquet
- Snowflake-ready curated outputs
- GitHub

## Project Structure
```text
streaming-engagement-pipeline/
├── data/
│   └── raw/
│       └── engagement_events.csv
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleaning_dedup.py
│   └── 03_gold_aggregations.py
├── sql/
│   └── gold_metrics.sql
└── README.md
