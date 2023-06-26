# Databricks notebook source
# MAGIC %md
# MAGIC # pred Tremi
# MAGIC prdiction of surface, heating production and heating_emission variables for every owner in Tremi dataset using owner variables

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
dpe_2021 = spark.sql("SELECT * FROM Datalake.dpe_france_2021")
municipality = spark.sql("SELECT * FROM Gold.Municipality")

# COMMAND ----------

# dpe.write.mode('overwrite')\
#         .format("parquet") \
#         .saveAsTable("Gold.DPE")
