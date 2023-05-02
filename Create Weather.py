# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.pandas import DataFrame as D

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
df = spark.sql("SELECT * FROM datalake.codebook")
display(df)
