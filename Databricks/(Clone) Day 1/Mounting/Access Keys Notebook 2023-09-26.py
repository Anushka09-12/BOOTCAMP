# Databricks notebook source

dbutils.fs.mount(
  source = "wasbs://raw@sanly.blob.core.windows.net",
  mount_point = "/mnt/sanly/raw",
  extra_configs = {"fs.azure.account.key.sanly.blob.core.windows.net":"+wZyMJdwqiETIzCNMc/uvE0AJQ/2+fIGVKKvfx4um7lsUO0EPZjLx3efLhF9OihDdkaV1TBwq77j+AStSZRQ1Q=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw/

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/sanly/raw/Baby_Names.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

output="dbfs:/mnt/sanly/raw/output"

# COMMAND ----------

df.write.mode("overwrite").parquet(f"{output}/anushka/babyname")

# COMMAND ----------

dbutils.fs.unmount("/mnt/sanly/raw")

# COMMAND ----------

dbutils.fs.mount(

  source = "wasbs://inputfiles@saunext.blob.core.windows.net",

  mount_point = "/mnt/saunext/inputfiles",

  extra_configs = {"fs.azure.account.key.saunext.blob.core.windows.net":"UUDMjjk8JYIiTwHNyh8WCs3BShkfIL//HM/cUrbOrRmUH+HaoR/J5bM9MlWTYefbkqNo/bQzgs1M+AStEn3dkA=="})
