from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, lit, split, hour

######################################################
## CREATE Silver table - marketing_silver_demo
######################################################
@dp.table(
    name="multiplex_2_silver.marketing_silver_demo"
)
def marketing_silver_demo():
    marketing_df = spark.readStream.table("multiplex_1_bronze.marketing_intermediate")
    marketing_transformed = (
        marketing_df
        .withColumn("click_through_rate", when(col("impressions") > 0, col("clicks") / col("impressions")).otherwise(0))
        .withColumn("cost_per_click", when(col("clicks") > 0, col("spend_usd") / col("clicks")).otherwise(0))
    )
    return marketing_transformed

######################################################
## CREATE Silver table - logistics_silver_demo
######################################################
@dp.table(
    name="multiplex_2_silver.logistics_silver_demo"
)
def logistics_silver_demo():
    logistics_df = spark.readStream.table("multiplex_1_bronze.logistics_intermediate")
    logistics_transformed = (
        logistics_df
        .filter((col("warehouse_id").isNotNull()) & (col("batch_id").isNotNull()))
        .withColumn("is_valid_shipment", when(col("num_packages") > 0, lit(True)).otherwise(lit(False)))
        .withColumn("event_date", col("timestamp").try_cast("date"))
    )
    return logistics_transformed

######################################################
## CREATE Silver table - store_ops_silver_demo
######################################################
@dp.table(
    name="multiplex_2_silver.store_ops_silver_demo"
)
def store_ops_silver_demo():
    store_ops_df = spark.readStream.table("multiplex_1_bronze.store_ops_intermediate")
    store_ops_transformed = (
        store_ops_df
        .filter((col("timestamp").isNotNull()) & (col("store_id").isNotNull()) & (col("event_type").isNotNull()))
        .withColumn("event_date", col("timestamp").try_cast("date"))
        .withColumn("event_hour", hour(col("timestamp")))
        .withColumn("store_number", split(col("store_id"),"_").getItem(2))
    )
    return store_ops_transformed
