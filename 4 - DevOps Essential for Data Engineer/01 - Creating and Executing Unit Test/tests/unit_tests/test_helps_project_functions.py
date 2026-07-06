import sys
import os

# Get the root folder (two levels up from this test file)
current_file = os.path.abspath(__file__)
test_dir = os.path.dirname(current_file)
tests_dir = os.path.dirname(test_dir)
root_folder = os.path.dirname(tests_dir)

# Add root folder to path so we can import helpers
if root_folder not in sys.path:
    sys.path.insert(0, root_folder)

from pyspark.sql import SparkSession
import pytest

# set up spark session
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, when
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual
from helpers import project_functions

def test_get_health_csv_schema_match():
    actual_schema = project_functions.get_health_csv_schema()
    expected_schema = StructType([
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
    assertSchemaEqual(actual_schema, expected_schema)    
    print('test passed!')

def test_high_cholest_map(spark):
    data = [(0,),(1,),(2,),(3,),(None,)]
    sample_df = spark.createDataFrame(data, ["HighChol"])

    actual_df = sample_df.withColumn("NewHighChol", project_functions.high_cholest_map("HighChol"))

    expected_df = spark.createDataFrame(
        [(0,"Normal"),
         (1,"Above Normal"),
         (2,"High"),
         (3, "Unknown"),
         (None, "Unknown")], 
        ["HighChol","NewHighChol"])
    
    assertDataFrameEqual(actual_df, expected_df)
    print('test passed!')

def test_group_age_map(spark):
    data = [(9,),(15,),(26,),(37,),(48,),(59,),(None,)]
    sample_df = spark.createDataFrame(data, ["Age"])

    actual_df = sample_df.withColumn("NewAge", project_functions.group_age_map("Age"))

    expected_df = spark.createDataFrame(
        [(9, "0-9"),
         (15, "10-19"),
         (26, "20-29"),
         (37, "30-39"),
         (48,"40-49"),
         (59,"50+"),
         (None,"Unknown")], 
        ["Age","NewAge"])
    
    assertDataFrameEqual(actual_df, expected_df)
    print('test passed!')
