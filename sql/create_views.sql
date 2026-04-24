-- ============================================================================
-- BIGQUERY ANALYTICS VIEWS FOR LOOKER STUDIO
-- ============================================================================

-- ============================================================================
-- VIEW 1: Revenue by Region
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_revenue_by_region` AS
SELECT
  c.country,
  o.region,
  TIMESTAMP_TRUNC(o.order_date, MONTH) AS month,
  COUNT(DISTINCT o.order_id) AS nb_orders,
  ROUND(SUM(o.total_amount), 2) AS revenue,
  ROUND(AVG(o.total_amount), 2) AS avg_basket,
  ROUND(SUM(o.total_amount) / SUM(SUM(o.total_amount)) OVER (), 4) AS pct_global_revenue
FROM `ecommerce_analytics.orders` o
JOIN `ecommerce_analytics.clients` c ON o.client_id = c.client_id
WHERE o.status != 'Cancelled'
GROUP BY c.country, o.region, month;

-- ============================================================================
-- VIEW 2: Inactive Clients
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_inactive_clients` AS
WITH last_order AS (
  SELECT client_id,
    MAX(order_date) AS last_order_date,
    COUNT(order_id) AS nb_orders,
    ROUND(SUM(total_amount), 2) AS historical_revenue
  FROM `ecommerce_analytics.orders`
  WHERE status != 'Cancelled'
  GROUP BY client_id
)
SELECT
  c.client_id, c.first_name, c.last_name, c.email,
  c.country, c.segment,
  l.last_order_date,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), l.last_order_date, DAY) AS days_inactive,
  l.nb_orders,
  l.historical_revenue
FROM `ecommerce_analytics.clients` c
JOIN last_order l ON c.client_id = l.client_id
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), l.last_order_date, DAY) > 60
ORDER BY l.historical_revenue DESC;

-- ============================================================================
-- VIEW 3: Top Products
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_top_products` AS
SELECT
  p.product_id, p.product_name, p.category,
  COUNT(DISTINCT oi.order_id) AS nb_orders,
  SUM(oi.quantity) AS qty_sold,
  ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue,
  ROUND(AVG(oi.unit_price), 2) AS avg_price,
  ROUND(COUNTIF(o.status = 'Cancelled') / COUNT(*), 4) AS cancellation_rate,
  RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS rank_in_category
FROM `ecommerce_analytics.order_items` oi
JOIN `ecommerce_analytics.products` p ON oi.product_id = p.product_id
JOIN `ecommerce_analytics.orders` o ON oi.order_id = o.order_id
GROUP BY p.product_id, p.product_name, p.category;

-- ============================================================================
-- VIEW 4: Recurring Incidents
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_recurring_incidents` AS
SELECT
  category,
  COUNT(*) AS nb_incidents,
  ROUND(AVG(resolution_time_h), 1) AS avg_resolution_h,
  ROUND(COUNTIF(status = 'Escalated') / COUNT(*), 4) AS pct_escalated,
  ROUND(COUNTIF(priority = 'Critical') / COUNT(*), 4) AS pct_critical,
  ROUND(COUNTIF(status = 'Resolved') / COUNT(*), 4) AS resolution_rate
FROM `ecommerce_analytics.incidents`
GROUP BY category;

-- ============================================================================
-- VIEW 5: Navigation Funnel
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_navigation_funnel` AS
SELECT
  page,
  COUNT(*) AS nb_sessions,
  COUNT(DISTINCT client_id) AS unique_users,
  ROUND(AVG(duration_seconds), 1) AS avg_duration_s,
  ROUND(COUNTIF(device = 'Mobile') / COUNT(*), 4) AS pct_mobile,
  ROUND(COUNTIF(device = 'Desktop') / COUNT(*), 4) AS pct_desktop,
  ROUND(AVG(duration_seconds) * COUNT(*) / 1000, 2) AS engagement_score
FROM `ecommerce_analytics.page_views`
GROUP BY page;

-- ============================================================================
-- VIEW 6: Weekly KPIs
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_weekly_kpis` AS
WITH weekly AS (
  SELECT
    DATE_TRUNC(DATE(order_date), ISOWEEK) AS week,
    COUNT(DISTINCT order_id) AS nb_orders,
    ROUND(SUM(total_amount), 2) AS revenue,
    COUNT(DISTINCT client_id) AS active_clients
  FROM `ecommerce_analytics.orders`
  WHERE status != 'Cancelled'
  GROUP BY week
)
SELECT
  week,
  nb_orders,
  revenue,
  active_clients,
  LAG(revenue) OVER (ORDER BY week) AS prev_week_revenue,
  ROUND((revenue - LAG(revenue) OVER (ORDER BY week))
    / NULLIF(LAG(revenue) OVER (ORDER BY week), 0) * 100, 2) AS growth_pct
FROM weekly
ORDER BY week;

-- ============================================================================
-- VIEW 7: Customer 360
-- ============================================================================

CREATE OR REPLACE VIEW `ecommerce_analytics.v_client_360` AS
WITH order_stats AS (
  SELECT client_id,
    COUNT(DISTINCT order_id) AS nb_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_basket,
    MAX(order_date) AS last_order_date
  FROM `ecommerce_analytics.orders`
  WHERE status != 'Cancelled'
  GROUP BY client_id
),
incident_stats AS (
  SELECT client_id,
    COUNT(*) AS nb_incidents
  FROM `ecommerce_analytics.incidents`
  GROUP BY client_id
),
nav_stats AS (
  SELECT client_id,
    COUNT(*) AS nb_pageviews
  FROM `ecommerce_analytics.page_views`
  WHERE client_id IS NOT NULL
  GROUP BY client_id
)
SELECT
  c.client_id, c.first_name, c.last_name, c.country, c.segment, c.registration_date,
  COALESCE(o.nb_orders, 0) AS nb_orders,
  COALESCE(o.total_revenue, 0) AS total_revenue,
  COALESCE(o.avg_basket, 0) AS avg_basket,
  o.last_order_date,
  COALESCE(i.nb_incidents, 0) AS nb_incidents,
  COALESCE(n.nb_pageviews, 0) AS nb_pageviews,
  ROUND(
    COALESCE(o.total_revenue, 0) * 0.5
    + (1 / NULLIF(COALESCE(i.nb_incidents, 0), 0)) * 20
    + COALESCE(o.nb_orders, 0) * 2, 2
  ) AS value_score,
  CASE
    WHEN COALESCE(o.total_revenue,0)*0.5 + COALESCE(o.nb_orders,0)*2 > 200 THEN 'VIP'
    WHEN COALESCE(o.total_revenue,0)*0.5 + COALESCE(o.nb_orders,0)*2 > 50  THEN 'Regular'
    ELSE 'At risk'
  END AS client_segment
FROM `ecommerce_analytics.clients` c
LEFT JOIN order_stats o ON c.client_id = o.client_id
LEFT JOIN incident_stats i ON c.client_id = i.client_id
LEFT JOIN nav_stats n ON c.client_id = n.client_id;
