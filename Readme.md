# Airflow Data Ingestion Orchestration

### 📋 Project Overview
- Automated data ingestion pipeline using Apache Airflow that orchestrates ETL processes from CSV files to PostgreSQL database with incremental loading for completed orders.

### 🎯 Objectives
- Automate ingestion of orders, products, and users data from CSV files
- Implement incremental data loading for completed orders based on date ranges
- Orchestrate ETL workflows using Apache Airflow
- Ensure data consistency and reliability in PostgreSQL database

### Daily Automated Workflow
- Step 1: Ingest products.csv → products table

- Step 2: Ingest users.csv → users table

- Step 3: Ingest orders.csv → orders table

- Step 4: Process completed orders from today & previous month → orders_completed table
