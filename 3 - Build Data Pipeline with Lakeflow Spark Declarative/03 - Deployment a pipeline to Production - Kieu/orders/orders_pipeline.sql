-- orders_bronze_demo
CREATE OR REFRESH STREAMING TABLE orders_bronze_streaming 
    COMMENT 'Ingest Json order from cloud storage'
    TBLPROPERTIES (
        "quality" = "bronze",
        "pipelines.reset.allowed" = false  --prevent full table refresh from bronze table
    )
AS
SELECT *,
        current_timestamp() AS processing_time,
        _metadata.file_name as source_file
FROM STREAM read_files(
    '${source}/orders',
    format => 'json'
);

-----------Silver - streaming table ---------------
CREATE OR REFRESH STREAMING TABLE order_silver_streaming 
(
        CONSTRAINT valid_notification EXPECT (notifications IN ('Y','x')),
        CONSTRAINT valid_date EXPECT (order_timestamp > '2021-12-25') ON VIOLATION DROP ROW,
        CONSTRAINT valid_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
    COMMENT 'Silver clean order table'
    TBLPROPERTIES ("quality" = "silver")
AS
SELECT order_id,
        timestamp(order_timestamp) as order_timestamp,
        customer_id,
        notifications
FROM STREAM orders_bronze_streaming;

------------- Gold - Materialize table -----------------
CREATE OR REFRESH MATERIALIZED VIEW gold_orders_by_date 
    COMMENT 'aggregate orders by date'
    TBLPROPERTIES ("quality" = "gold")
AS
SELECT date(order_timestamp) as order_date,
        count(1) as total_daily_orders
FROM  order_silver_streaming
GROUP BY date(order_timestamp)