
-----------------------------------------------------
-- 1. CREATE BRONZE TABLE STRUCTURE
-----------------------------------------------------
CREATE OR REFRESH STREAMING TABLE multi_flow_1_bronze.orders_bronze_flow_demo
(
    subsidiary_id	STRING,
    order_id	    STRING,
    order_timestamp	STRING,
    customer_id	    STRING,
    region	        STRING,
    country	        STRING,
    city	        STRING,
    channel	        STRING,
    sku	            STRING,
    category	    STRING,
    qty	            STRING,
    unit_price	    STRING,
    discount_pct	STRING,
    coupon_code	    STRING,
    total_amount	STRING,
    order_date	    STRING,
    source_file     STRING,
    file_mod_time   TIMESTAMP
)
COMMENT "Creates a bronze streaming table with orders from all subsidiaries using multiple flows"
TBLPROPERTIES(
    "pipelines.reset.allowed" = false  --prevent full refreshes on the bronze tables
);

-----------------------------------------------------
-- 2. BRONZE FLOW - BRIGHT HOME
-----------------------------------------------------
CREATE FLOW bright_home_orders_flow
AS INSERT INTO multi_flow_1_bronze.orders_bronze_flow_demo BY NAME
SELECT
    CAST(subsidiary_id AS STRING) AS subsidiary_id,
    CAST(order_id AS STRING) AS order_id,
    CAST(order_timestamp AS STRING) AS order_timestamp,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(region AS STRING) AS region,
    CAST(country AS STRING) AS country,
    CAST(city AS STRING) AS city,
    CAST(channel AS STRING) AS channel,
    CAST(sku AS STRING) AS sku,
    CAST(category AS STRING) AS category,
    CAST(qty AS STRING) AS qty,
    CAST(unit_price AS STRING) AS unit_price,
    CAST(discount_pct AS STRING) AS discount_pct,
    CAST(coupon_code AS STRING) AS coupon_code,
    CAST(total_amount AS STRING) AS total_amount,
    CAST(order_date AS STRING) AS order_date,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
    '${bright_home_orders_source}',
    format => 'csv',
    header => true
);

-----------------------------------------------------
-- 3. BRONZE FLOW - LUMINA SPORTS
-----------------------------------------------------
CREATE FLOW lumina_sports_orders_flow
AS INSERT INTO multi_flow_1_bronze.orders_bronze_flow_demo BY NAME
SELECT
    CAST(subsidiary_id AS STRING) AS subsidiary_id,
    CAST(order_id AS STRING) AS order_id,
    CAST(order_timestamp AS STRING) AS order_timestamp,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(region AS STRING) AS region,
    CAST(country AS STRING) AS country,
    CAST(city AS STRING) AS city,
    CAST(channel AS STRING) AS channel,
    CAST(sku AS STRING) AS sku,
    CAST(category AS STRING) AS category,
    CAST(qty AS STRING) AS qty,
    CAST(unit_price AS STRING) AS unit_price,
    CAST(discount_pct AS STRING) AS discount_pct,
    CAST(coupon_code AS STRING) AS coupon_code,
    CAST(total_amount AS STRING) AS total_amount,
    CAST(order_date AS STRING) AS order_date,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
    '${lumina_sports_orders_source}',
    format => 'csv',
    header => true
);


-----------------------------------------------------
-- 4. BRONZE FLOW - NORTHSTAR OUTFITTERS SPORTS
-----------------------------------------------------
CREATE FLOW northstar_outfitters_orders_flow
AS INSERT INTO multi_flow_1_bronze.orders_bronze_flow_demo BY NAME
SELECT
    CAST(subsidiary_id AS STRING) AS subsidiary_id,
    CAST(order_id AS STRING) AS order_id,
    CAST(order_timestamp AS STRING) AS order_timestamp,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(region AS STRING) AS region,
    CAST(country AS STRING) AS country,
    CAST(city AS STRING) AS city,
    CAST(channel AS STRING) AS channel,
    CAST(sku AS STRING) AS sku,
    CAST(category AS STRING) AS category,
    CAST(qty AS STRING) AS qty,
    CAST(unit_price AS STRING) AS unit_price,
    CAST(discount_pct AS STRING) AS discount_pct,
    CAST(coupon_code AS STRING) AS coupon_code,
    CAST(total_amount AS STRING) AS total_amount,
    CAST(order_date AS STRING) AS order_date,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS file_mod_time
FROM STREAM read_files(
    '${northstar_outfitters_orders_source}',
    format => 'json'
);

