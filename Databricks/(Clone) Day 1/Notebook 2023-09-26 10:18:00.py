# Databricks notebook source
# MAGIC %md
# MAGIC #####Practice For All Languages

# COMMAND ----------

print("hello python")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Hey, run scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Three level namespace
# MAGIC ###catalog.database.objectname

# COMMAND ----------

# MAGIC %sql
# MAGIC create database test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table test.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test.demo(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC use test

# COMMAND ----------


