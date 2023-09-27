-- Databricks notebook source
-- MAGIC %fs ls 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/emp_table_1-1.txt")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("test.myTableCSV")

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/test.db

-- COMMAND ----------

describe extended test.mytablecsv;

-- COMMAND ----------


