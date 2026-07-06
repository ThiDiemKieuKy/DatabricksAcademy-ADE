from helpers import project_functions

file_directory = f"/Volumes/dev_catalog/default/health"
bronze_table_name = "health_bronze"
silver_table_name = "health_silver"
gold_table_name = "cholest_age_agg"

project_functions.generate_bronze(spark, file_directory, bronze_table_name)
project_functions.generate_silver(spark, bronze_table_name, silver_table_name)
project_functions.cholest_age_agg(spark, silver_table_name, gold_table_name)
