from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, lit, date_sub, current_date, concat_ws, current_timestamp

#####################################################
## CREATE - sales_silver_dp
#####################################################
@dp.table(
    name="dp.dp_2_silver.sales_silver_dp",
    comment="Quarantined table with 6 expectations - supports inverse logic pattern",
    table_properties={
        "quality.layer": "silver_quarantine", 
        "quality.pattern": "inverse_logic"},
    partition_cols=["is_quarantined"]
)

@dp.expect("check_subsidiary_id", "subsidiary_id IS NOT NULL")
@dp.expect("check_customer_id", "customer_id IS NOT NULL")
@dp.expect("check_sku", "sku IS NOT NULL")
@dp.expect("valid_discount_range", "discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)")
@dp.expect("valid_date_range","order_date IS NULL \
                                OR (order_date >= DATE_SUB(CURRENT_DATE(),1460) AND order_date <= CURRENT_DATE())")
@dp.expect("valid_shipping_cost","CASE WHEN shipping_cost IS NOT NULL \
                                    THEN shipping_cost > 0 AND shipping_cost < 100 ELSE TRUE END")
def sales_silver():
    sales_bronze_clean = (
        spark.readStream.table("dp.dp_1_bronze.sales_bronze_clean")
        # Quarantine tracking for quarantined analysis
        .withColumn(
            "is_quarantined",
            when((
                    (col("subsidiary_id").isNotNull()) &
                    (col("customer_id").isNotNull()) &
                    (col("sku").isNotNull()) &
                    (col("discount_pct").isNull() | ((col("discount_pct") >= 0) & (col("discount_pct") <= 100))) &
                    (col("order_date").isNull() | 
                    ((col("order_date") >= date_sub(current_date(), 1460)) & (col("order_date") <= current_date()))
                    ) &
                    (col("shipping_cost").isNull() | ((col("shipping_cost") > 0) & (col("shipping_cost") < 100)))
                ), lit(False)               
            )
            .otherwise(True)
        )
        .withColumn(
            "quarantined_reason",
            concat_ws(";", 
                when(col("subsidiary_id").isNull(), lit("subsidiary_id is null")),
                when(col("customer_id").isNull(), lit("customer_id is null")),
                when(col("sku").isNull(), lit("sku is null")),
                when(col("discount_pct").isNotNull() & ((col("discount_pct") < 0) | (col("discount_pct") > 100)), lit("discount_pct is not in range")),
                when(col("order_date").isNotNull() & 
                    ((col("order_date") < date_sub(current_date(), 1460)) | (col("order_date") > current_date())), lit("order_date is not in range")),
                when(col("shipping_cost").isNotNull() & ((col("shipping_cost") <= 0) | (col("shipping_cost") >= 100)), lit("shipping_cost is not in range"))
            )
        )
        .withColumn(
            "quarantined_date",
            when(col("is_quarantined"), current_timestamp()).otherwise(lit(None))        
        )        
    )
    return sales_bronze_clean

#####################################################
## VALID RECORDS PATH - clean data for analytics
#####################################################
@dp.table(
    name="dp.dp_2_silver.sales_silver_valid",
    comment="Clean records, passing all 6 quality checks - ready for analytics"
)
def sales_silver_valid():
    valid_df = (
        spark.readStream.table("dp.dp_2_silver.sales_silver_dp")
        .filter(col("is_quarantined") == False)
        .drop("is_quarantined", "quarantined_reason", "quarantined_date")
    )
    return valid_df

############################################################
## QUARANTINED RECORDS PATH - invalid data for remediation
############################################################
@dp.table(
    name="dp.dp_2_silver.sales_silver_quarantined",
    comment="Invalid records with quality violation - requires remediation"
)
def sales_silver_valid():
    valid_df = (
        spark.readStream.table("dp.dp_2_silver.sales_silver_dp")
        .filter(col("is_quarantined") == True)
    )
    return valid_df