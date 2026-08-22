from pyspark import pipelines as dp
from pyspark.sql.functions import col

#######################################################
## Create - Bronze Raw Table
#######################################################
@dp.table(
    name="dp.dp_1_bronze.sales_bronze_raw"
)
def sales_bronze_raw():
    source = spark.conf.get("source")
    sales_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # allow schema evolution by adding new columns
        .option("cloudFiles.schemaHints","order_status STRING, shipping_cost STRING")
        .load(f"{source}")
        .withColumn("source_file", col("_metadata.file_name"))
        .withColumn("file_mod_time", col("_metadata.file_modification_time"))        
    )
    return sales_df

#######################################################
## Create - Bronze Clean Table
#######################################################
@dp.table(
    name="dp.dp_1_bronze.sales_bronze_clean"
)
def sales_bronze_clean():
    sales_clean_df = (
        spark.readStream.table("dp.dp_1_bronze.sales_bronze_raw")
        .select(
            "subsidiary_id",
            "order_id",
            col("order_timestamp").try_cast("timestamp").alias("order_timestamp"),
            col("order_timestamp").try_cast("date").alias("order_date"),
            "customer_id",
            "region",
            "country",
            "channel",
            "sku",
            "category",
            col("qty").try_cast("int").alias("qty"),
            col("unit_price").try_cast("double").alias("unit_price"),
            col("discount_pct").try_cast("double").alias("discount_pct"),
            col("total_amount").try_cast("double").alias("total_amount"),
            "coupon_code",
            "order_status",
            col("shipping_cost").try_cast("double").alias("shipping_cost"),
            "source_file"
        )
    )
    return sales_clean_df