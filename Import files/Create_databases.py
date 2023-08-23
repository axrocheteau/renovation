# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
import zipfile
import io
import os

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# drop databases
# spark.sql("DROP DATABASE IF EXISTS Datalake CASCADE")
# spark.sql("DROP DATABASE IF EXISTS Intermediate CASCADE")
# spark.sql("DROP DATABASE IF EXISTS Silver CASCADE")
# spark.sql("DROP DATABASE IF EXISTS Gold CASCADE")
# spark.sql("DROP DATABASE IF EXISTS Model CASCADE")

# Create Database store raw files
spark.sql("CREATE DATABASE IF NOT EXISTS Datalake")

# Create Database store modified files
spark.sql("CREATE DATABASE IF NOT EXISTS Intermediate")

# Create Database store final modifie files
spark.sql("CREATE DATABASE IF NOT EXISTS Silver")

# Create Database store final modifie files
spark.sql("CREATE DATABASE IF NOT EXISTS Model")

# Create Database store data for BI
spark.sql("CREATE DATABASE IF NOT EXISTS Gold")
