# Databricks notebook source
# MAGIC %md
# MAGIC ## scan

# COMMAND ----------

# all db
dbutils.fs.ls("/user/hive/warehouse/model.db/")
# all files
# dbutils.fs.ls("/FileStore/tables")


# COMMAND ----------

# MAGIC %md
# MAGIC ## remove

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/model.db/training_em/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## create

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables")

# COMMAND ----------

dbutils.fs.ls("file:/")
