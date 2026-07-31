# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog dbcertification;
# MAGIC use schema bookstore_etl_pro;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze;
