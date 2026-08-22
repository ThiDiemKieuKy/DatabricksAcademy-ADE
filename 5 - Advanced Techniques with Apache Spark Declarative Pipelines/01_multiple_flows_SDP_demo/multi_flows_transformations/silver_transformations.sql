
---------------------------------------------
-- 1. CREATE SILVER TABLE
---------------------------------------------
CREATE OR REFRESH STREAMING TABLE multi_flow_2_silver.orders_silver_flow_demo
(
    subsidiary_id	STRING,
    order_id	    STRING,
    order_timestamp	TIMESTAMP,
    order_date	    DATE,
    customer_id	    STRING,
    region	        STRING,
    country	        STRING,
    city	        STRING,
    channel	        STRING,
    sku	            STRING,
    category	    STRING,
    qty	            INT,
    unit_price	    DOUBLE,
    discount_pct	DOUBLE,    
    total_amount	DOUBLE,
    coupon_code	    STRING,

    -- Add Data quality to drop the invalid rows or fail the pipelines
    CONSTRAINT valid_order_timestamp EXPECT(order_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_total_amount EXPECT(total_amount > 0) ON VIOLATION DROP ROW,
    CONSTRAINT valid_qty EXPECT (qty > 0) ON VIOLATION DROP ROW
)
COMMENT "Clean and standardized data from multiple flow bronze table"

--Add liquid clustering to improve performace on common filters
CLUSTER BY AUTO

AS
-- Select and clean data from bronze tabl. Use TRY_CAST to enforce consistent types across all subsidiaries
SELECT 
    subsidiary_id,
    order_id,
    TRY_CAST(order_timestamp    AS TIMESTAMP),
    TRY_CAST(order_date         AS DATE),
    customer_id,
    region,
    country,
    city,
    channel,
    sku,
    category,
    TRY_CAST(qty                AS INT) AS qty,
    TRY_CAST(unit_price         AS DOUBLE) AS unit_price,
    TRY_CAST(discount_pct       AS DOUBLE) AS discount_pct,
    TRY_CAST(total_amount       AS DOUBLE) AS total_amount,
    coupon_code
FROM STREAM multi_flow_1_bronze.orders_bronze_flow_demo
