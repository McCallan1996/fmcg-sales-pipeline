# FMCG Sales Analytics Pipeline

An end-to-end data engineering pipeline that processes 3 years of daily FMCG (Fast-Moving Consumer Goods) sales data from the Polish market and delivers interactive dashboards for sales performance analysis.

Built as the course project for [Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## Problem

FMCG companies generate huge volumes of transactional sales data daily. Without a proper pipeline, this data sits in CSVs and spreadsheets with no easy way to answer questions like:

- Which sales channels are actually growing, and which are stalling?
- Do promotions drive real volume or just shift purchases in time?
- What seasonal patterns exist, and how do they vary by region?
- How does delivery lag correlate with stock levels?

This project builds a batch pipeline that ingests daily sales data, enriches it, and produces a dashboard answering these questions across 3 sales channels (Retail, Discount, E-commerce), 3 regions (Central, North, South Poland), and multiple product categories over 2022-2024.

## Dataset

[FMCG Daily Sales Data 2022-2024](https://www.kaggle.com/datasets/beatafaron/fmcg-daily-sales-data-to-2022-2024) from Kaggle (CC0 license). Synthetic dataset simulating realistic FMCG conditions including seasonality, promotions, new product introductions, and price trends.

## Architecture

```
Kaggle API
    |
    v
[Airflow DAG]
    |
    +--> Download CSV --> Upload to GCS (data lake)
                              |
                              v
                     Load into BigQuery (raw)
                              |
                              v
                     Spark enrichment job
                     (revenue, date parts, flags)
                              |
                              v
                     BigQuery (processed)
                              |
                              v
                     dbt models (staging -> marts)
                              |
                              v
                     BigQuery (analytics)
                              |
                              v
                     Looker Studio dashboard
```

## Tech Stack

| Component | Tool |
|-----------|------|
| Infrastructure | Terraform |
| Containerization | Docker + Docker Compose |
| Orchestration | Apache Airflow |
| Data Lake | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Batch Processing | Apache Spark (PySpark) |
| Transformations | dbt |
| Dashboard | Looker Studio |
| Cloud | Google Cloud Platform |

## Project Structure

```
.
├── terraform/          # GCS bucket, BQ datasets, IAM
├── docker/             # docker-compose for Airflow + Spark
├── airflow/
│   ├── dags/           # pipeline DAG
│   └── scripts/        # download, upload, load helpers
├── spark/jobs/         # PySpark transformation
├── dbt/models/
│   ├── staging/        # clean + standardize
│   ├── intermediate/   # aggregation logic
│   └── marts/          # fact + dimension tables
└── dashboards/         # screenshots
```

## BigQuery Partitioning & Clustering

The main fact table (`fct_sales_summary`) is:
- **Partitioned by** `sale_date` (monthly granularity) — every dashboard query filters on date, so BQ skips irrelevant partitions
- **Clustered by** `sales_channel, region` — the two most common filter/group-by dimensions after date

The processed table uses daily partitioning on `sale_date` and clusters by `sales_channel, region, category` since analytical queries at that layer tend to drill into product categories too.

## Dashboard

The Looker Studio dashboard has these main views:

1. **Monthly Revenue Trend** (line chart) — revenue over time, split by sales channel. Shows seasonality and growth.
2. **Revenue by Channel & Region** (stacked bar) — how total revenue distributes across the 3 channels and 3 regions.
3. **Promotion Impact** — average daily sales with vs without promotions, per category.
4. **KPIs** — total revenue, units sold, number of SKUs, avg promotion lift.

Screenshots are in `dashboards/screenshots/` once the pipeline has run.

## How to Reproduce

### Prerequisites

- GCP account with a project + billing enabled
- Docker and Docker Compose installed
- Terraform installed
- Kaggle account (for API key)

### Step 1: Clone and configure

```bash
git clone <this-repo-url>
cd fmcg-sales-pipeline
cp .env.example .env
```

Edit `.env` with your actual values:
- `GCP_PROJECT_ID` — your GCP project
- `KAGGLE_USERNAME` / `KAGGLE_KEY` — from kaggle.com > Account > API

### Step 2: GCP setup

1. Create a service account with BigQuery Admin + Storage Admin roles
2. Download the JSON key to `credentials/google_credentials.json`
3. Enable these APIs: BigQuery, Cloud Storage

### Step 3: Provision infrastructure

```bash
cd terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

This creates the GCS bucket and 3 BigQuery datasets.

### Step 4: Start containers

```bash
cd ../docker
docker-compose --env-file ../.env up -d
```

Wait a minute for Airflow to initialize, then open http://localhost:8080 (login: admin / admin).

### Step 5: Run the pipeline

In the Airflow UI, find the `fmcg_daily_sales_pipeline` DAG and trigger it manually. The tasks run in sequence:

1. Download dataset from Kaggle
2. Upload CSV to GCS
3. Load into BigQuery raw table
4. Spark enrichment (adds revenue, date parts, weekend flag)
5. dbt build (staging -> intermediate -> mart tables)

### Step 6: Dashboard

Open Looker Studio, connect to `fmcg_analytics` dataset in BigQuery, and build your visualizations from the `fct_sales_summary` and `fct_promotion_impact` tables.

### Teardown

```bash
cd docker && docker-compose down -v
cd ../terraform && terraform destroy -var="project_id=YOUR_PROJECT_ID"
```

## What I Learned

Building this pipeline end-to-end taught me how the pieces of a data stack actually fit together — Terraform for the infrastructure, Airflow for orchestrating the steps, Spark for the heavy transformations, dbt for the business logic layer, and how partitioning/clustering in BigQuery really matters when you start querying at scale. The biggest gotcha was getting Spark to talk to BigQuery through the right connector JARs.
