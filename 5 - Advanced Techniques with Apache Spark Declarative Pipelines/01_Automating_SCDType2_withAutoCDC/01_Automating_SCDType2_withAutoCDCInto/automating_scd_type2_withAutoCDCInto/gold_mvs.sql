------------------------------------------------------------
-- Create materialized view for current (active) customer
------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW automating_scd_3_gold.current_customers_gold
AS SELECT * EXCEPT(processing_time),
    current_timestamp() AS updated_at
FROM automating_scd_2_silver.customers_scd_type2_silver
WHERE `__END_AT` IS NULL;

------------------------------------------------------------
-- Create materialized view for removed (inactive) customer
------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW automating_scd_3_gold.removed_customers_gold
AS SELECT 
    customer_id,
    MAX_BY(city, `__START_AT`) as city,
    MAX_BY(email, `__START_AT`) as email,
    MAX_BY(first_name, `__START_AT`) as first_name,
    MAX_BY(last_name, `__START_AT`) as last_name,
    MAX_BY(`__START_AT`, `__START_AT`) as `__START_AT`,
    MAX_BY(`__END_AT`, `__START_AT`) as `__END_AT`
FROM automating_scd_2_silver.customers_scd_type2_silver
GROUP BY customer_id
HAVING MAX_BY(`__END_AT`, `__START_AT`) IS NOT NULL;
