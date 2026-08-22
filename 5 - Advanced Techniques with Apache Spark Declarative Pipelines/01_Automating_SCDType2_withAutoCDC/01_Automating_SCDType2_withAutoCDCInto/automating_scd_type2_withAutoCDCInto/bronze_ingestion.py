from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_timestamp

########################################
## customers_scd_type2_bronze
########################################
@dp.table(
    name="automating_scd.automating_scd_1_bronze.customers_scd_type2_bronze",
    comment="raw customers data from CDC Feed"
)
def customers_scd_type2_bronze():
    source = spark.conf.get("source")
    customers_cdc_feed = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", True)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source)
        .withColumn("processing_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )
    return customers_cdc_feed

########################################
## customers_scd_type2_bronze_clean
########################################
@dp.table(
    name="automating_scd.automating_scd_1_bronze.customers_scd_type2_bronze_clean",
    comment="raw customers data from CDC Feed"
)
@dp.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_operation","operation IS NOT NULL")
@dp.expect("valid_name", "first_name IS NOT NULL OR last_name IS NOT NULL OR operation = 'deleted'")
@dp.expect("valid_address", "city IS NOT NULL OR operation = 'deleted'")
@dp.expect_or_drop("valid_email", "(email IS NOT NULL AND email LIKE '%@%.%') OR operation = 'deleted'")
def customers_scd_type2_bronze_clean():
    customers_clean = spark.readStream.table("automating_scd.automating_scd_1_bronze.customers_scd_type2_bronze")
    customers_clean = customers_clean.withColumn("timestamp_datetime", to_timestamp(col("timestamp")).try_cast("timestamp"))
    
    return customers_clean