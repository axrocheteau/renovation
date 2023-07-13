# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
from bs4 import BeautifulSoup
import unicodedata

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

code_commune = spark.sql("SELECT * FROM datalake.code_commune")
municipalities = (
    code_commune.select(
        F.col('Code_commune_INSEE')
    )
    .filter(
        (~F.col('Code_commune_INSEE').contains('2A')) &
        (~F.col('Code_commune_INSEE').contains('2B')) &
        (F.col('Code_commune_INSEE') < '96000')
    )
    .orderBy(F.col('Code_commune_INSEE'))
)
display(municipalities.count())
