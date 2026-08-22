from pyspark import pipelines as dp

dp.create_sink(
    name="delta_sink_logistics",
    format = "delta",
    options = {
        "tableName": "multiplex.multiplex_3_gold.logistics_delta_sink"
    }
)

@dp.append_flow(
    name="logistics_append_flow",
    target="delta_sink_logistics",
)
def logistics_append_flow():
    df = spark.readStream.table("multiplex.multiplex_2_silver.logistics_silver_demo")
    return df