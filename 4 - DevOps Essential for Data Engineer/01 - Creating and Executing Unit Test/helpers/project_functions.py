from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, when, current_timestamp

#ID,PII,date,HighCholest,HighBP,BMI,Age,Education,Income
def get_health_csv_schema():
    """
    Returns the schema for the health csv file.
    return StructType
    """
    return StructType([
        StructField('ID', IntegerType(), True),
        StructField('PII', StringType(), True),
        StructField('date', DateType(), True),
        StructField('HighChol', IntegerType(), True),
        StructField('HighBP', DoubleType(), True),
        StructField('BMI', DoubleType(), True),
        StructField('Age', DoubleType(), True),
        StructField('Education', DoubleType(), True),
        StructField('Income', IntegerType(), True),
    ])

def high_cholest_map(col_name):
    """
        Returns a map of cholesterol values to their corresponding string values
        -- 0 -> 'Normal'
        -- 1 -> 'Above Normal'
        -- 2 -> 'High'
        -- any other value 'Unknown'
    """
    return (
        when(col(col_name) == 0, 'Normal')
        .when(col(col_name) == 1, 'Above Normal')
        .when(col(col_name) == 2, 'High')
        .otherwise('Unknown')
    )

def group_age_map(col_name):
    """
        map age value to an age group
        -- 0-9 -> "0-9"
        -- 10-19 -> "10-19"
        -- 20-29 -> "20-29"
        -- 30-39 -> "30-39"
        -- 40-49 -> "40-49"
        -- 50+ -> "50+"
        -- any other value -> "Unknown"
    """
    return (
        when((col(col_name) >= 0) & (col(col_name) <= 9), '0-9')
        .when((col(col_name) >= 10) & (col(col_name) <= 19), '10-19')
        .when((col(col_name) >= 20) & (col(col_name) <= 29), '20-29')
        .when((col(col_name) >= 30) & (col(col_name) <= 39), '30-39')
        .when((col(col_name) >= 40) & (col(col_name) <= 49), '40-49')
        .when((col(col_name) >= 50), '50+')
        .otherwise('Unknown')
    )
    
def generate_bronze(spark, file_directory, bronze_table_name):
    df = (spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_directory)
        .select(
            "*",
            col("_metadata.file_name").alias("source_file"),
            col("_metadata.file_modification_time").alias("source_file_modification_time"),
            current_timestamp().alias("processing_time")
        ))
    df.write.mode("overwrite").saveAsTable(bronze_table_name)

def generate_silver(spark, bronze_table_name, silver_table_name):
    bronze_df = spark.table(bronze_table_name)
    silver_df = (
        bronze_df
        .withColumn("HighCholest_Group", high_cholest_map("HighCholest"))
        .withColumn("Age_Group", group_age_map("Age")))
    
    silver_df.write.mode("overwrite").saveAsTable(silver_table_name)
    
def cholest_age_agg(spark, silver_table_name, gold_table_name):
    df = spark.sql(f"""
                   SELECT HighCholest_Group,
                        Age_Group,
                        count(*) AS count
                   FROM {silver_table_name}
                   GROUP BY HighCholest_Group, Age_Group
                   """)
    df.write.mode("overwrite").saveAsTable(gold_table_name)
