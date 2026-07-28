# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **with static data we can use 'dropDuplicates' to drop duplicate records.**

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC - In spark structured streaming we can also use dropDuplicates function, to ensure no duplicate records are present.
# MAGIC - StructuredStreaming can track state information for the unique keys in the data.
# MAGIC - This ensures duplicate records do not exist within or between microbatches.
# MAGIC - However, overtime, this state information will scale to represent all history.
# MAGIC - We can limit the amount of the state to be maintained by using watermarking.
# MAGIC - **Watermarking allows to only track state information for a window of time in which we expect records could be delayed**

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC - When dealing with streaming duplication, there is another level of complexity compared to static data as each micro-batch is processed.
# MAGIC - We need also to ensure that records to be inserted are not already in the target table.
# MAGIC - We can achieve this using insert only merge. This is ideal for de-duplication.
# MAGIC - It defines logic to match on unique keys and only insert those records for keys that do not exist in the table.

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO dbcertification.udemydata.orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)
    #microBatchDF._jdf.sparkSession().sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbcertification.udemydata.orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")

