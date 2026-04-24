# Cloud Storage Upload Guide

## 📊 Generated Data Summary

Your synthetic datasets have been successfully generated locally:

```
data/raw/
├── clients.csv          (2,020 rows, 221 KB)        - Customer master data
├── products.csv         (50 rows, 2 KB)             - Product catalog
├── orders.csv           (15,150 rows, 1.1 MB)       - Order transactions
├── order_items.csv      (25,487 rows, 875 KB)       - Order line items
├── incidents.csv        (3,030 rows, 216 KB)        - Support tickets
└── page_views.csv       (50,500 rows, 3.5 MB)       - Web analytics
```

**Total Simulated Data**: 12.3M EUR revenue | 63.4% delivery rate

---

## ☁️ Step 1: Set Up Google Cloud Storage Bucket

### 1a. Install gcloud CLI
Download from: https://cloud.google.com/sdk/docs/install

### 1b. Authenticate
```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### 1c. Create Storage Bucket
```bash
gsutil mb -l europe-west1 gs://ecommerce-data-lake
```

> **Note**: Replace `YOUR_PROJECT_ID` with your actual GCP project ID and adjust location if needed.

---

## 🚀 Step 2: Upload CSV Files to Cloud Storage

### Option A: Using Python Script (Recommended)

Edit `scripts/upload_to_gcs.py` and update:
```python
PROJECT_ID = "your-actual-project-id"    # Replace with your GCP project
BUCKET_NAME = "ecommerce-data-lake"      # Or your bucket name
```

Then run:
```bash
python scripts/upload_to_gcs.py
```

Expected output:
```
INFO:__main__:Found 6 CSV files to upload
INFO:__main__:Uploading clients.csv → gs://ecommerce-data-lake/raw/clients.csv
INFO:__main__:✓ clients.csv (0.22 MB)
...
INFO:__main__:✓ All files uploaded to gs://ecommerce-data-lake/raw/
```

### Option B: Using gsutil CLI

```bash
gsutil -m cp data/raw/*.csv gs://ecommerce-data-lake/raw/
```

---

## 3️⃣ Step 3: Verify Files in Cloud Storage

### Check uploaded files:
```bash
gsutil ls -lh gs://ecommerce-data-lake/raw/
```

Expected output:
```
gs://ecommerce-data-lake/raw/clients.csv
gs://ecommerce-data-lake/raw/incidents.csv
gs://ecommerce-data-lake/raw/orders.csv
gs://ecommerce-data-lake/raw/order_items.csv
gs://ecommerce-data-lake/raw/page_views.csv
gs://ecommerce-data-lake/raw/products.csv
```

---

## 4️⃣ Step 4: Load Data into BigQuery

### Create BigQuery Dataset:
```bash
bq mk --location=EU ecommerce_analytics
```

### Load CSV files:
```bash
# Clients
bq load --source_format=CSV \
  ecommerce_analytics.clients \
  gs://ecommerce-data-lake/raw/clients.csv \
  --allow_quoted_newlines --skip_leading_rows=1

# Products
bq load --source_format=CSV \
  ecommerce_analytics.products \
  gs://ecommerce-data-lake/raw/products.csv \
  --allow_quoted_newlines --skip_leading_rows=1

# Orders
bq load --source_format=CSV \
  ecommerce_analytics.orders \
  gs://ecommerce-data-lake/raw/orders.csv \
  --allow_quoted_newlines --skip_leading_rows=1

# Order Items
bq load --source_format=CSV \
  ecommerce_analytics.order_items \
  gs://ecommerce-data-lake/raw/order_items.csv \
  --allow_quoted_newlines --skip_leading_rows=1

# Incidents
bq load --source_format=CSV \
  ecommerce_analytics.incidents \
  gs://ecommerce-data-lake/raw/incidents.csv \
  --allow_quoted_newlines --skip_leading_rows=1

# Page Views
bq load --source_format=CSV \
  ecommerce_analytics.page_views \
  gs://ecommerce-data-lake/raw/page_views.csv \
  --allow_quoted_newlines --skip_leading_rows=1
```

Or use the SQL schema file:
```bash
bq query --use_legacy_sql=false < sql/create_tables.sql
```

---

## 5️⃣ Step 5: Create Analytics Views

```bash
bq query --use_legacy_sql=false < sql/create_views.sql
```

Verify views:
```bash
bq ls ecommerce_analytics
```

---

## 📋 Troubleshooting

### Error: "Access Denied"
```bash
gcloud auth application-default login
```

### Error: "Bucket not found"
```bash
gsutil mb -l europe-west1 gs://ecommerce-data-lake
```

### Error: "Project not set"
```bash
gcloud config set project YOUR_PROJECT_ID
```

### Check credentials:
```bash
gcloud config list
```

---

## ✅ Next Steps

1. ✓ Data generated locally
2. ☐ Create GCS bucket
3. ☐ Upload CSV files to Cloud Storage
4. ☐ Create BigQuery dataset
5. ☐ Load data into BigQuery
6. ☐ Create analytics views
7. ☐ Connect Looker Studio dashboard

---

## 📊 Sample Queries

After loading into BigQuery:

```sql
-- Total revenue by region
SELECT 
  region, 
  COUNT(*) as orders,
  ROUND(SUM(total_amount), 2) as revenue
FROM `ecommerce_analytics.orders`
WHERE status = 'Delivered'
GROUP BY region
ORDER BY revenue DESC;

-- Top 10 customers
SELECT 
  c.client_id, 
  CONCAT(c.first_name, ' ', c.last_name) as name,
  c.country,
  COUNT(o.order_id) as num_orders,
  ROUND(SUM(o.total_amount), 2) as total_spent
FROM `ecommerce_analytics.clients` c
LEFT JOIN `ecommerce_analytics.orders` o ON c.client_id = o.client_id
GROUP BY c.client_id, name, c.country
ORDER BY total_spent DESC
LIMIT 10;
```

---

**Questions?** Check the project README or GCP documentation.
