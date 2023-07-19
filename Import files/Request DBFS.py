# Databricks notebook source
# MAGIC %md
# MAGIC ## scan

# COMMAND ----------

# all db
dbutils.fs.ls("/user/hive/warehouse/bi.db/")
# all files
# dbutils.fs.ls("/FileStore/tables")


# COMMAND ----------

# MAGIC %md
# MAGIC ## remove

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/bi.db/department/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## create

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables")

# COMMAND ----------

dbutils.fs.ls("file:/")
