CREATE OR REFRESH STREAMING TABLE customers_bronze_raw_streaming
    COMMENT 'Customer Boronze Table' 
    TBLPROPERTIES(
        "Quality" = "bronze",
        "pipelines.reset.allowed" = false
    )
AS
SELECT *,
        current_timestamp() AS processing_time,
        _metadata.file_name AS source_file
FROM STREAM(read_files(
    '${source}/customers',
    format => 'json'
));


CREATE OR REFRESH STREAMING TABLE customers_bronze_clean_streaming
(
    CONSTRAINT valid_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_name EXPECT(name IS NOT NULL OR operation = "DELETE"),
    CONSTRAINT valid_address EXPECT(
        (address IS NOT NULL AND
        city IS NOT NULL AND
        state IS NOT NULL AND
        customer_id IS NOT NULL) OR
        operation = "DELETE"),
    CONSTRAINT valid_email EXPECT(
        rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$')
        OR operation = "DELETE") ON VIOLATION DROP ROW
)
    COMMENT 'Customer SIlver Table'
    TBLPROPERTIES("quality" = "silver")
AS
SELECT *,
    cast(from_unixtime(timestamp) AS timestamp) as timestamp_datetime
FROM STREAM customers_bronze_raw_streaming;


CREATE OR REFRESH STREAMING TABLE scd_type_customer_silver_streaming 
    COMMENT 'SCD Type 1';
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO scd_type_customer_silver_streaming
FROM STREAM customers_bronze_clean_streaming
    KEYS (customer_id)
    APPLY AS DELETE WHEN operation = 'DELETE'
    SEQUENCE BY timestamp_datetime
    COLUMNS * EXCEPT (timestamp, _rescued_data, operation)
    STORED AS SCD TYPE 1;