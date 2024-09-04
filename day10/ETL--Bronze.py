# Databricks notebook source
# MAGIC %run /Workspace/Users/sanjay_1724384809981@npupgradassessment.onmicrosoft.com/day10/day8/includes

# COMMAND ----------

input_path

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable("bronze.products_bronze")

# COMMAND ----------

dbutils.widgets.text("environment","dev")
v=dbutils.widgets.get("environment")

# COMMAND ----------

df=spark.read.csv(f"{input_path}products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df2=df1.withColumn("environment",lit(v))
df2.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("bronze.products_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.products_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## I am cool
