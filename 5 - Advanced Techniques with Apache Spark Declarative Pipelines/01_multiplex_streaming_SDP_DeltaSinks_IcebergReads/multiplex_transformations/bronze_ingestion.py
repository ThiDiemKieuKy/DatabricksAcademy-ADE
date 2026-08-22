from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

###################################################
## CREATE BRONZE Table
###################################################
@dp.table(
    name='multiplex_1_bronze.bronze_demo',
    table_properties={
        'pipelines.reset.allowed': 'false',
        'delta.feature.variantType-preview': 'supported'
    }
)
def bronze_demo():
    business_events_source = spark.conf.get("business_events_source")

    bronze_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_group", StringType(), True),        
        StructField("timestamp", TimestampType(), True),        
        StructField("event_type", StringType(), True),
        StructField("subsidiary_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("open_by_employee_id", StringType(), True),
        # logistic
        StructField("warehouse_id", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("num_packages", LongType(), True),
        StructField("destination_region", StringType(), True),
        # Marketing
        StructField("campaign_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("impressions", LongType(), True),
        StructField("clicks", LongType(), True),
        StructField("conversions", LongType(), True),
        StructField("spend_usd", DoubleType(), True)
    ])
    
    bronze_df = (spark.readStream
                .format("json")
                .schema(bronze_schema)
                .load(f"{business_events_source}")
                .withColumn("source_file", col("_metadata.file_name"))
                .withColumn("file_mod_time", col("_metadata.file_modification_time"))
            )
    return bronze_df

###################################################
## FAN OUT - store_ops_intermediate
###################################################
@dp.table(
    name='multiplex_1_bronze.store_ops_intermediate',
    table_properties={
        'delta.feature.variantType-preview': 'supported'
    }
)
def store_ops_intermediate():
    bronze_df = spark.readStream.table("multiplex_1_bronze.bronze_demo")
    store_ops = (
        bronze_df
        .filter(col("event_group") == "store_ops")
        .select(
            "event_id",
            "event_group",
            "timestamp",
            "event_type",
            "subsidiary_id",
            "store_id",
            "city",
            "region",
            "open_by_employee_id",
            "source_file",
            "file_mod_time"
        )
    )
    return store_ops

###################################################
## FAN OUT - logistics_intermediate
###################################################
@dp.table(
    name='multiplex_1_bronze.logistics_intermediate',
    table_properties={
        'delta.feature.variantType-preview': 'supported'
    }
)
def logistics_intermediate():
    bronze_df = spark.readStream.table("multiplex_1_bronze.bronze_demo")
    logistics = (
        bronze_df
        .filter(col("event_group") == "logistics")
        .select(
            "event_id",
            "event_group",
            "timestamp",
            "event_type",
            "subsidiary_id",
            "warehouse_id",
            "carrier",
            "batch_id",
            "num_packages",
            "destination_region",
            "source_file",
            "file_mod_time"
        )
    )
    return logistics

###################################################
## FAN OUT - marketing_intermediate
###################################################
@dp.table(
    name='multiplex_1_bronze.marketing_intermediate',
    table_properties={
        'delta.feature.variantType-preview': 'supported'
    }
)
def marketing_intermediate():
    bronze_df = spark.readStream.table("multiplex_1_bronze.bronze_demo")
    marketing = (
        bronze_df
        .filter(col("event_group") == "marketing")
        .select(
            "event_id",
            "event_group",
            "timestamp",
            "event_type",
            "subsidiary_id",
            "campaign_id",
            "channel",
            "impressions",
            "clicks",
            "conversions",
            "spend_usd",
            "source_file",
            "file_mod_time"
        )
    )
    return marketing
