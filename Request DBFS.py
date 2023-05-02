# Databricks notebook source
# all db
dbutils.fs.ls("/user/hive/warehouse/")
# all files
dbutils.fs.ls("/FileStore/tables")


# COMMAND ----------

# all files
dbutils.fs.rm("/FileStore/tables/dep_limitrophe.csv")
