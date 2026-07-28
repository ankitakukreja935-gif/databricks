# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

print(bookstore.dataset_path)

# COMMAND ----------

print(bookstore.checkpoint_path)

# COMMAND ----------

files = dbutils.fs.ls(f"{bookstore.dataset_path}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{bookstore.dataset_path}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{bookstore.dataset_path}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", f"{bookstore.checkpoint_path}/bronze")
                      .option("mergeSchema", True)
                      .partitionBy("topic", "year_month")
                      .trigger(availableNow=True)
                      .table("bronze"))  
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **----------------------------------------------------------------------------------------------------------------------**

# COMMAND ----------

# MAGIC %md
# MAGIC **Updated Code : for PySpark-Delta-Autoloader Approach, when schema is not given** 

# COMMAND ----------

# MAGIC %skip
# MAGIC import time
# MAGIC from pyspark.sql.utils import StreamingQueryException
# MAGIC
# MAGIC def start_autoloader_stream():
# MAGIC     """Defines and starts the Auto Loader stream."""
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .option("cloudFiles.schemaLocation", "/mnt/data/schema_checkpoint/")
# MAGIC         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC         .load("/mnt/data/raw_files/")
# MAGIC         .writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "/mnt/data/stream_checkpoint/")
# MAGIC         .start("/mnt/data/bronze_table/")
# MAGIC     )
# MAGIC
# MAGIC max_retries = 3
# MAGIC attempt = 0
# MAGIC
# MAGIC while attempt < max_retries:
# MAGIC     try:
# MAGIC         print(f"Starting stream (Attempt {attempt + 1})...")
# MAGIC         query = start_autoloader_stream()
# MAGIC         
# MAGIC         # This blocks the cell from finishing until the stream terminates
# MAGIC         query.awaitTermination() 
# MAGIC         
# MAGIC     except StreamingQueryException as e:
# MAGIC         # Check if the failure was caused by a schema evolution
# MAGIC         if "UnknownFieldException" in str(e.cause):
# MAGIC             print("Schema evolution detected! RocksDB updated. Restarting stream to pick up the new column...")
# MAGIC             attempt += 1
# MAGIC             time.sleep(5) # Brief pause before restarting
# MAGIC         else:
# MAGIC             # If it failed for a different reason (like corrupted data or network drop), raise the error
# MAGIC             print("Stream failed for an unexpected reason.")
# MAGIC             raise e
# MAGIC             
# MAGIC print("Stream exceeded maximum restart attempts or was manually stopped.")
