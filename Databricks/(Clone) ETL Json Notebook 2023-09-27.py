# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/saunextadls/raw/json

# COMMAND ----------

#dbutils.fs.mount(
  #source = "wasbs://raw@saunextadls.blob.core.windows.net",
  #mount_point = "/mnt/saunextadls/raw",
  #extra_configs = {"fs.azure.account.key.saunextadls.blob.core.windows.#net":"DsZWJs7JVVHZz1I7GKyclV8ejCdj0V2UkqMlgAp6QyVOw5rvrHvmVTgwcThdHUymWg7MXon65/0z+AStj4Yiug=="})

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/saunextadls/raw/json/")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1 = df.withColumn("ingestion_date",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

df1.display()

# COMMAND ----------

###This will create a managed table as no path is defined
df1.write.mode("overwrite").saveAsTable("bronzejson")

# COMMAND ----------

###This will create an external table in the data lake as path is defined
df1.write.mode("overwrite").option("path","dbfs:/mnt/saunextadls/raw/output/anushka/json").saveAsTable("bronzejson")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronzejson

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronzejson

# COMMAND ----------


