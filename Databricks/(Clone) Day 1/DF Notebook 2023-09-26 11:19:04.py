# Databricks notebook source
users=[(1,'a',30,"Sales"),(2,'b',25,"IT"),(3,'c',28,"Data Science")]

schema="id int, name string, age int, dept string"

df=spark.createDataFrame(data=users, schema=schema)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumnRenamed("id","emp_id").withColumn("current_date",current_date())

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("test.emp_table")

# COMMAND ----------


