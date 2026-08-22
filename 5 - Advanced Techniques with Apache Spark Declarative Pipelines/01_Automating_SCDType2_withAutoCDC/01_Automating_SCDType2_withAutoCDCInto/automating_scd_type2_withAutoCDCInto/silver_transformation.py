from pyspark import pipelines as dp

#######################################################
## Processing CDC Data with auto_cdc_flow
#######################################################
dp.create_streaming_table(
    name="automating_scd.automating_scd_2_silver.customers_scd_type2_silver",
    comment="SCD Type 2 Historical Customer Data"
)

dp.create_auto_cdc_flow(
    name="create_auto_cdc_flow",
    target="automating_scd.automating_scd_2_silver.customers_scd_type2_silver",
    source="automating_scd.automating_scd_1_bronze.customers_scd_type2_bronze_clean",
    keys=["customer_id"],
    sequence_by="timestamp_datetime",
    stored_as_scd_type=2,
    apply_as_deletes = "operation = 'deleted' ",
    except_column_list=["timestamp","operation","_rescued_data"]
)