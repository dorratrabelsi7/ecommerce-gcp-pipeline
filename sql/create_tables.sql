-- ============================================================================
-- BIGQUERY SCHEMA - GENERATED FROM ACTUAL DATA STRUCTURE
-- ============================================================================

-- Drop existing tables to allow schema changes
DROP TABLE IF EXISTS ecommerce_analytics.clients;
DROP TABLE IF EXISTS ecommerce_analytics.products;
DROP TABLE IF EXISTS ecommerce_analytics.orders;
DROP TABLE IF EXISTS ecommerce_analytics.order_items;
DROP TABLE IF EXISTS ecommerce_analytics.incidents;
DROP TABLE IF EXISTS ecommerce_analytics.page_views;

-- ============================================================================
-- 1. CLIENTS TABLE
-- ============================================================================

CREATE TABLE ecommerce_analytics.clients (
  client_id STRING,
  last_name STRING,
  first_name STRING,
  email STRING,
  age FLOAT64,
  gender STRING,
  country STRING,
  city STRING,
  phone STRING,
  registration_date TIMESTAMP,
  segment STRING
)
PARTITION BY TIMESTAMP_TRUNC(registration_date, DAY)
CLUSTER BY country, segment
OPTIONS(
  description = "Client master data with demographics",
  labels = [("domain", "customer"), ("pii", "true")]
);

-- ============================================================================
-- 2. PRODUCTS TABLE (No partition - small dimension)
-- ============================================================================

CREATE TABLE ecommerce_analytics.products (
  product_id STRING,
  product_name STRING,
  category STRING,
  unit_price FLOAT64,
  stock INT64
)
OPTIONS(
  description = "Product catalog with pricing",
  labels = [("domain", "product")]
);

-- ============================================================================
-- 3. ORDERS TABLE
-- ============================================================================

CREATE TABLE ecommerce_analytics.orders (
  order_id STRING,
  client_id STRING,
  order_date TIMESTAMP,
  status STRING,
  payment_method STRING,
  region STRING,
  total_amount FLOAT64
)
PARTITION BY TIMESTAMP_TRUNC(order_date, DAY)
CLUSTER BY client_id, region
OPTIONS(
  description = "Order transactions with amounts and status",
  labels = [("domain", "order")]
);

-- ============================================================================
-- 4. ORDER_ITEMS TABLE
-- ============================================================================

CREATE TABLE ecommerce_analytics.order_items (
  item_id STRING,
  order_id STRING,
  product_id STRING,
  quantity FLOAT64,
  unit_price FLOAT64
)
OPTIONS(
  description = "Line items for orders",
  labels = [("domain", "order")]
);

-- ============================================================================
-- 5. INCIDENTS TABLE (Support tickets)
-- ============================================================================

CREATE TABLE ecommerce_analytics.incidents (
  incident_id STRING,
  client_id STRING,
  report_date TIMESTAMP,
  category STRING,
  order_id STRING,
  status STRING,
  priority STRING,
  resolution_time_h FLOAT64
)
PARTITION BY TIMESTAMP_TRUNC(report_date, DAY)
CLUSTER BY status, priority
OPTIONS(
  description = "Customer support incidents",
  labels = [("domain", "support")]
);

-- ============================================================================
-- 6. PAGE_VIEWS TABLE (Website analytics)
-- ============================================================================

CREATE TABLE ecommerce_analytics.page_views (
  session_id STRING,
  client_id STRING,
  page STRING,
  event_datetime TIMESTAMP,
  duration_seconds FLOAT64,
  device STRING,
  browser STRING,
  traffic_source STRING
)
PARTITION BY TIMESTAMP_TRUNC(event_datetime, DAY)
CLUSTER BY client_id, page
OPTIONS(
  description = "Website page view events",
  labels = [("domain", "web_analytics")]
);
