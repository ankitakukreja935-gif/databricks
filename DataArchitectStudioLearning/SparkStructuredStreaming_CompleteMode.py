# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC drop table if exists dbcertification.streamschnest.invoices_bz;
# MAGIC drop table if exists dbcertification.streamschnest.customer_rewards;

# COMMAND ----------

schema = """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>"""
                    
df1 = spark.read.schema(schema).json("/Volumes/dbcertification/streamschnest/streamingdata/invoices/invoices-1.json")
print(df1.count())
df2 = spark.read.schema(schema).json("/Volumes/dbcertification/streamschnest/streamingdata/invoices/invoices-2.json")
print(df2.count())
df3 = spark.read.schema(schema).json("/Volumes/dbcertification/streamschnest/streamingdata/invoices/invoices-3.json")
print(df3.count())
df4 = spark.read.schema(schema).json("/Volumes/dbcertification/streamschnest/streamingdata/invoices/invoices-4.json")
print(df4.count())

# COMMAND ----------

display(df1.join(df2,df1.InvoiceNumber == df2.InvoiceNumber,"left_anti"))

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
                            .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/invoices_bz")
                            .outputMode("append")
                            .trigger(availableNow=True)
                            .toTable("dbcertification.streamschnest.invoices_bz")           
                    ) 
        print("Done")
        return sQuery   

# COMMAND ----------

brnz = Bronze()
query = brnz.process()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fourth load
# MAGIC select count(*) from dbcertification.streamschnest.invoices_bz;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- third load
# MAGIC select count(*) from dbcertification.streamschnest.invoices_bz;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- second load
# MAGIC select count(*) from dbcertification.streamschnest.invoices_bz;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first load
# MAGIC select count(*) from dbcertification.streamschnest.invoices_bz;

# COMMAND ----------

class Gold():
    def __init__(self):
        self.base_data_dir = "/Volumes/dbcertification/streamschnest/streamingdata/"
        
    def readBronze(self):
        return spark.readStream.table("dbcertification.streamschnest.invoices_bz")

    def getAggregates(self, invoices_df):
        from pyspark.sql.functions import sum, expr
        return (invoices_df.groupBy("CustomerCardNo")
                    .agg(sum("TotalAmount").alias("TotalAmount"),
                         sum(expr("TotalAmount*0.02")).alias("TotalPoints"))
        )

    def saveResults(self, results_df):
        print(f"\nStarting Silver Stream...", end='')
        return (results_df.writeStream
                    .queryName("gold-update")
                    .option("checkpointLocation", f"{self.base_data_dir}/checkpoint/customer_rewards")
                    .outputMode("complete")
                    .trigger(availableNow=True)
                    .toTable("dbcertification.streamschnest.customer_rewards") #insert only method, does not have capability to update existing record or implement merge kind of requirement. 
                )
        print("Done")
        #we are calculating aggregates  in each micro batch, and aggregates may vary based on new file ingested so coomplete mode is used to ensure that we are calculating correct aggregates. Here toTable allows insert only in overwrite way.

    def process(self):
        invoices_df = self.readBronze()
        aggregate_df = self.getAggregates(invoices_df)
        sQuery = self.saveResults(aggregate_df)
        return sQuery                 

# COMMAND ----------

gdStream = Gold()
gdQuery = gdStream.process()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fourth load
# MAGIC select TotalAmount from dbcertification.streamschnest.customer_rewards where CustomerCardNo = '2262471989';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- third load
# MAGIC select TotalAmount from dbcertification.streamschnest.customer_rewards where CustomerCardNo = '2262471989';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- second load
# MAGIC select TotalAmount from dbcertification.streamschnest.customer_rewards where CustomerCardNo = '2262471989';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first load
# MAGIC select TotalAmount from dbcertification.streamschnest.customer_rewards where CustomerCardNo = '2262471989' ;