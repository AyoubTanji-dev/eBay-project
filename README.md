# 🦅 eBay Morocco Market Monitor
**Enterprise-Grade Data Engineering Project**

This project transforms unstructured eBay listing data into a professional, automated Business Intelligence dashboard.
It demonstrates a complete End-to-End pipeline: `Data Lake` -> `Silver Layer` -> `Star Schema (Gold)` -> `PostgreSQL` -> `Power BI` -> `Airflow Automation`.

![Power BI Dashboard](assets/dashboard_background.png)

## 🏗️ Architecture

1.  **Ingestion**: Python scripts scrape/ingest eBay listing data.
2.  **Transformation (Silver)**: Cleaning, type casting, currency normalization (`clean_transform.py`).
3.  **Modeling (Gold)**: Aggregation into **Star Schema** (Fact & Dimensions) (`silver_to_gold.py`) optimized for BI.
4.  **Storage**: Centralized **PostgreSQL** Data Warehouse.
5.  **Visualization**: **Power BI** dashboard with DirectQuery, custom theme, and dynamic filtering.
6.  **Orchestration**: **Apache Airflow** (Dockerized) simulating daily incremental loads (`airflow/`).

---

## 📂 Project Structure

```
eBay-project/
├── data/
│   ├── raw/                  # Original scraped CSVs
│   ├── silver/               # Cleaned data (The "Lake")
│   └── gold/                 # Star Schema CSVs (Facts & Dims)
├── transformation/
│   ├── clean_transform.py    # Bronze -> Silver logic
│   └── silver_to_gold.py     # Silver -> Gold logic
├── load/
│   └── load_to_postgres.py   # Database Loading Script
├── airflow/
│   ├── dags/                 # Airflow DAGs
│   ├── scripts/              # Incremental Scripts
│   └── docker-compose.yaml   # Infrastructure configuration
└── README.md                 # This file
```

---

## 🚀 How to Run

### Option A: Manual Full Refresh (Dev Mode)
Use this to rebuild the entire database from scratch.

1.  **Clean & Transform**:
    ```bash
    python transformation/clean_transform.py
    python transformation/silver_to_gold.py
    ```
2.  **Load to DB**:
    ```bash
    python load/load_to_postgres.py
    ```

### Option B: Automated Pipeline (Prod Mode)
Use this to simulate daily emerging data.

1.  **Start Airflow**:
    ```bash
    cd airflow
    docker compose up -d
    ```
2.  **Access UI**: [http://localhost:8085](http://localhost:8085) (admin/admin).
3.  **Trigger DAG**: Enable `ebay_morocco_etl_pipeline` and click Play.

---

## 📊 Power BI Setup
1.  Open `eBay_Dashboard.pbix` (or create new).
2.  Connect to PostgreSQL (`localhost:5433`).
3.  Import Tables: `fact_market_activity`, `fact_seller_performance`, `dim_time`, `dim_category`, `dim_price_bucket`.
4.  Set Background: Use `assets/dashboard_background.png` (Fit, 0% Transparency).

---

## 🛠️ Tech Stack
-   **Python 3.10** (Pandas)
-   **PostgreSQL 14**
-   **Apache Airflow 2.7** (Docker)
-   **Power BI Desktop**
