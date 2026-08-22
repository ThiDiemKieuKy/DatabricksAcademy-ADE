---------------------------------------------------
-- CREATE MATERIALIZED VIEW FOR BUSINESS ANALYTICS
---------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dp.dp_3_gold.sales_analytics
AS
SELECT region,
        country,
        category,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) as total_unique_customers,
        SUM(qty) AS total_qty_sold,
        ROUND(SUM(total_amount),2) as total_revenue,
        ROUND(AVG(total_amount),2) AS average_order_value,
        ROUND(AVG(discount_pct),2) AS average_discount_pct,
        MIN(order_date) as earliest_order_date,
        MAX(order_date) as latest_order_date,
        current_timestamp() as last_refreshed_at
FROM dp.dp_2_silver.sales_silver_valid
GROUP BY region, country, category