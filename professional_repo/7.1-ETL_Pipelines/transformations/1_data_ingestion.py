from pyspark import pipelines as dp
from pyspark.sql import functions as F

dataset_path = spark.conf.get("dataset_path")

# we will create it as streaming table to support incremental data ingestion from autoloader stream
# This function defines an autoloader stream and returns a streaming dataframe
# We dont performa streaming write in LDP, it will be handled by @table decorator, checkpointing and retries and write

@dp.table(
    name="bronze",
    partition_cols = ["topic","year_month"],
    table_properties = {
        "delta.appendOnly" : "true", # to disable updates and deletes on this bronze table
        "pipelines.reset.allowed" : "false"
    }
)
def process_bronze():
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    bronze_df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format","json")
                 .schema(schema)
                 .load(f"{dataset_path}/kafka-raw-etl")
                 .withColumn("timestamp",(F.col("timestamp")/1000).cast("timestamp"))
                 .withColumn("year_month",F.date_format("timestamp","yyyy-MM"))
    )

    return bronze_df


@dp.temporary_view
def country_lookup():
    countries_df = spark.read.json(f"{dataset_path}/country_lookup")
    return countries_df




