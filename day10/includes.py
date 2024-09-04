# Databricks notebook source
input_path = "dbfs:/mnt/upgradtrendenceadls/delta/files/"

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def add_ingestion_col(input_df):
    final_df=input_df.withColumn("ingestion_date", current_timestamp())
    return final_df
