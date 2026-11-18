# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC drop table if exists dbcertification.streamschnest.invoices_bz_stateless;
# MAGIC drop table if exists dbcertification.streamschnest.customer_rewards_stateless;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbcertification.streamschnest.customer_rewards_stateless (
# MAGIC     CustomerCardNo STRING,
# MAGIC     TotalAmount DOUBLE,
# MAGIC     TotalPoints DOUBLE
# MAGIC );

# COMMAND ----------

class Bronze():
    def __init__(self):
        self.base_data_dir = "/Volumes/dbcertification/streamschnest/streamingdata/"
        
    def getSchema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
            """

    def readInvoices(self):
        from pyspark.sql.functions import col
        return (spark.readStream
                     .format("json")
                     .schema(self.getSchema())
                     .load(f"{self.base_data_dir}/invoices")
                     .withColumn("InputFile", col("_metadata.file_name"))
                )  

    def process(self):
        print(f"\nStarting Bronze Stream...", end='')
        invoicesDF = self.readInvoices()
        sQuery =  ( invoicesDF.writeStream
                            .queryName("bronze-ingestion")
                            .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices_bz_stateless")
                            .outputMode("append")
                            .trigger(availableNow=True)
                            .toTable("dbcertification.streamschnest.invoices_bz_stateless")           
                    ) 
        print("Done")
        return sQuery   

# COMMAND ----------

brnz = Bronze()
query = brnz.process()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- third load
# MAGIC select CustomerCardNo, sum(TotalAmount) as TotalAmount ,sum(TotalAmount*0.02) as TotalPoints,InputFile 
# MAGIC from dbcertification.streamschnest.invoices_bz_stateless
# MAGIC where CustomerCardNo = '2262471989' 
# MAGIC group by CustomerCardNo,InputFile;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- second load
# MAGIC select CustomerCardNo, sum(TotalAmount) as TotalAmount ,sum(TotalAmount*0.02) as TotalPoints,InputFile 
# MAGIC from dbcertification.streamschnest.invoices_bz_stateless
# MAGIC where CustomerCardNo = '2262471989' 
# MAGIC group by CustomerCardNo,InputFile;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first load
# MAGIC select CustomerCardNo, sum(TotalAmount) as TotalAmount ,sum(TotalAmount*0.02) as TotalPoints 
# MAGIC from dbcertification.streamschnest.invoices_bz_stateless
# MAGIC where CustomerCardNo = '2262471989' 
# MAGIC group by CustomerCardNo;

# COMMAND ----------

class Gold():
    def __init__(self):
        self.base_data_dir = "/Volumes/dbcertification/streamschnest/streamingdata/"
        
    def readBronze(self):
        return spark.readStream.table("dbcertification.streamschnest.invoices_bz_stateless")

    def getAggregates(self, invoices_df):
        from pyspark.sql.functions import sum, expr
        return (invoices_df.groupBy("CustomerCardNo")
                    .agg(sum("TotalAmount").alias("TotalAmount"),
                         sum(expr("TotalAmount*0.02")).alias("TotalPoints"))
        )

    def aggregate_upsert(self, invoices_df, batchId):
        rewards_df = self.getAggregates(invoices_df)
        rewards_df.createOrReplaceTempView("customers_rewards_df_temp_view")

        merge_statement = """MERGE INTO dbcertification.streamschnest.customer_rewards_stateless t
                                USING customers_rewards_df_temp_view s
                                ON s.CustomerCardNo = t.CustomerCardNo
                                WHEN MATCHED THEN
                                UPDATE SET t.TotalAmount =  s.TotalAmount + t.TotalAmount, 
                                           t.TotalPoints = s.TotalPoints + t.TotalPoints
                                WHEN NOT MATCHED
                                THEN INSERT *

        """
        invoices_df.sparkSession.sql(merge_statement)
        
    def saveResults(self, results_df):
        print(f"\nStarting Silver Stream...", end='')
        return (results_df.writeStream
                    .queryName("gold-update")
                    .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/customer_rewards_stateless")
                    .outputMode("update")
                    .trigger(availableNow=True) 
                    .foreachBatch(self.aggregate_upsert)
                    .start() 
                )
        print("Done")

    def process(self):
        invoices_df = self.readBronze()
        sQuery = self.saveResults(invoices_df)
        return sQuery                 

# COMMAND ----------

gdStream = Gold()
gdQuery = gdStream.process()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- third load
# MAGIC select CustomerCardNo, TotalAmount,TotalPoints from dbcertification.streamschnest.customer_rewards_stateless where CustomerCardNo = '2262471989';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- second load
# MAGIC select CustomerCardNo, TotalAmount,TotalPoints from dbcertification.streamschnest.customer_rewards_stateless where CustomerCardNo = '2262471989';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first load
# MAGIC select CustomerCardNo, TotalAmount,TotalPoints from dbcertification.streamschnest.customer_rewards_stateless where CustomerCardNo = '2262471989';