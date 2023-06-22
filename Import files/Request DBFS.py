# Databricks notebook source
# MAGIC %md
# MAGIC ## scan

# COMMAND ----------

# all db
dbutils.fs.ls("/user/hive/warehouse/")
# all files
dbutils.fs.ls("/FileStore/tables")


# COMMAND ----------

# MAGIC %md
# MAGIC ## remove

# COMMAND ----------

# all files
dbutils.fs.rm("/FileStore/tables", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## create

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables")

# COMMAND ----------

dbutils.fs.ls("file:/")
