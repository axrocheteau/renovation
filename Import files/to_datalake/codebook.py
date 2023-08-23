# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
import zipfile
import io
import os
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, Row
import csv

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

import requests
import pandas as pd
from io import BytesIO

# COMMAND ----------

link_excel_array = [
    {'url' : 'https://data.ademe.fr/data-fair/api/v1/datasets/tremi-2017-resultats-bruts/metadata-attachments/TREMI_2017_CodeBook_public.xlsx', 'name': 'codebook', "delimiter": ";"},
]

# COMMAND ----------

for file in link_excel_array:
    r = requests.get(file['url'])
    name = file['name']
    if r.status_code != 200:
        raise Exception(f" unable to get a response from {file['url']} at file : {file['name']}")

    response = requests.get('https://data.ademe.fr/data-fair/api/v1/datasets/tremi-2017-resultats-bruts/metadata-attachments/TREMI_2017_CodeBook_public.xlsx')
    df = pd.read_excel(io = io.BytesIO(response.content))
    sdf = spark.createDataFrame(df)
    sdf.write.mode("overwrite")\
        .format("parquet") \
        .saveAsTable(f"datalake.{name}")
