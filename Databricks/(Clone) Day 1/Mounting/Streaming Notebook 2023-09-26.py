# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://inputfiles@saunext.blob.core.windows.net",
  mount_point = "/mnt/saunext/inputfiles",
  extra_configs = {"fs.azure.account.key.saunext.blob.core.windows.net":"UUDMjjk8JYIiTwHNyh8WCs3BShkfIL//HM/cUrbOrRmUH+HaoR/J5bM9MlWTYefbkqNo/bQzgs1M+AStEn3dkA=="})

# COMMAND ----------

user_schema = "timestamp timestamp, event_type string, user_id string, page_id string"

# COMMAND ----------

df = spark.readStream.schema(user_schema).json("dbfs:/mnt/saunext/inputfiles/inputstream/")

# COMMAND ----------

df.display()

# COMMAND ----------

output = "dbfs:/mnt/saunext/inputfiles/outputstream"

# COMMAND ----------

df.writeStream\
    .option("checkpointlocation",f"{output}/naval/checkpoint")\
        .option("path",f"{output}/anushka/output")\
            .table("test.jsonsample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.jsonsample

# COMMAND ----------

for stream in spark.streams.active: stream.stop()

# COMMAND ----------


