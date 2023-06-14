# Databricks notebook source
# MAGIC %md
# MAGIC # Owner

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# load df
tremi = spark.sql("SELECT * FROM datalake.tremi")
Dictionnary = spark.sql("SELECT * FROM Silver.Dictionary")
