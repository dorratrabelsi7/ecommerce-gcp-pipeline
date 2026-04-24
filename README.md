# E-Commerce GCP Cloud-Native BI Pipeline

A complete cloud-native business intelligence pipeline for e-commerce data processing using Google Cloud Platform.

## 📋 Project Overview

This project implements a full ETL/ELT pipeline for real-time e-commerce analytics:
- **Ingestion**: Data from Cloud Storage and Pub/Sub
- **Processing**: Apache Beam via Dataflow
- **Storage**: BigQuery data warehouse
- **Visualization**: Looker Studio dashboards

## 🏗️ Project Structure

```
ecommerce-gcp-pipeline/
├── scripts/
│   └── generate_data.py          # Synthetic data generation (2K clients, 15K orders, etc.)
├── sql/
│   ├── create_tables.sql         # BigQuery table schemas
│   └── create_views.sql          # Analytics views for Looker Studio
├── pipeline/
│   └── dataflow_pipeline.py      # Apache Beam ETL pipeline
├── cloud_functions/
│   ├── main.py                   # Cloud Functions for triggering Dataflow
│   └── runtime.yaml              # Cloud Function configuration
├── terraform/                     # IaC for GCP resource provisioning
├── config/                        # Configuration files
├── data/
│   └── raw/                      # Generated datasets
└── docs/                          # Documentation
```

## 📊 Data Model

### Tables
- **clients**: Customer demographics (2K records)
- **products**: Product catalog (50 items)
- **orders**: Order transactions (15K records)
- **order_items**: Line-item details
- **incidents**: Support tickets (3K records)
- **page_views**: Website analytics (50K sessions)

### Views
- `v_revenue_by_region`: Regional sales analytics
- `v_inactive_clients`: Customer churn detection
- `v_top_products`: Product performance ranking
- `v_recurring_incidents`: Support issue patterns
- `v_navigation_funnel`: Website user funnel
- `v_weekly_kpis`: Time-series business metrics
- `v_client_360`: Customer 360 profile

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- GCP account with billing enabled
- gcloud CLI installed
- Required packages: `pip install -r requirements.txt`

### Step 1: Generate Sample Data
```bash
python scripts/generate_data.py
```
Output: CSV files in `data/raw/`

### Step 2: Create BigQuery Dataset and Tables
```bash
bq mk ecommerce_analytics
bq query < sql/create_tables.sql
bq query < sql/create_views.sql
```

### Step 3: Deploy Dataflow Pipeline
```bash
python pipeline/dataflow_pipeline.py \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --region europe-west1
```

### Step 4: Connect Looker Studio
1. Go to [Looker Studio](https://lookerstudio.google.com)
2. Create new report
3. Connect to BigQuery
4. Select `ecommerce_analytics` dataset
5. Build dashboards using views

## 📈 Key Metrics
- Total Revenue (simulated): 15M+ EUR
- Delivery Rate: ~65%
- Customer Segments: VIP, Regular, At Risk
- Support Resolution Rate: ~60%

## 🔧 GCP Services Used

| Service | Purpose |
|---------|---------|
| Cloud Storage | Raw data ingestion |
| Pub/Sub | Real-time streaming |
| Cloud Functions | Orchestration |
| Dataflow | ETL processing |
| BigQuery | Data warehouse |
| Looker Studio | Dashboards & BI |
| Cloud Scheduler | Job scheduling |

## 📝 Documentation

- [Data Generation Report](docs/data_generation_report.txt)
- [Pipeline Architecture](docs/ARCHITECTURE.md)
- [BigQuery Schema](sql/create_tables.sql)
- [SQL Views](sql/create_views.sql)

## 📧 Contact

**Author**: Mohamed Najeh ISSAOUI  
**Email**: issaoui.mn@itbs.tn  
**School**: IT Business School

## 📄 License

Project for educational purposes - IT Business School Mini-Project

---

**Last Updated**: June 2024
