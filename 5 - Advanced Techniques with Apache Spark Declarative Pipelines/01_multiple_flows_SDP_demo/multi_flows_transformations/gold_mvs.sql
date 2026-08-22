
----------------------------------------------------------------------------------
-- A - CREATE MATERIALIZED VIEW: Daily Total Revenuer and Sold units per subsidiary
----------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW multi_flow_3_gold.mv_daily_total_revenue_and_sold_units_per_subsidiary
AS
SELECT
  order_date,
  subsidiary_id,
  COUNT(order_id) as order_count,
  SUM(total_amount) AS total_revenue,
  SUM(qty) AS total_sold_units
FROM multi_flow_2_silver.orders_silver_flow_demo
GROUP BY order_date, subsidiary_id;



----------------------------------------------------------------------------------
-- B - CREATE MATERIALIZED VIEW: product performance for each subsidiary
----------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW multi_flow_3_gold.mv_product_performance_for_each_subsidiary
AS
SELECT
  subsidiary_id,
  category,
  sku,
  SUM(total_amount) AS total_revenue,
  SUM(qty) AS total_sold_units
FROM multi_flow_2_silver.orders_silver_flow_demo
GROUP BY subsidiary_id, category, sku;