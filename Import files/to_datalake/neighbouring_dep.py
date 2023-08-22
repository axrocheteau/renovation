# Databricks notebook source
import requests
import re
import pandas as pd
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, Row

warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

url = "https://gist.githubusercontent.com/sunny/13803/raw/7f7f0ad5371e6ebf427f878071f42f8a51cd4eeb/liste-de-departements-limitrophes-francais.txt"
response = requests.get(url)
txt = re.sub(r"[:|,]", ';', response.text)

columns = ['Departement', 'Voisin_1', 'Voisin_2', 'Voisin_3', 'Voisin_4', 'Voisin_5', 'Voisin_6', 'Voisin_7', 'Voisin_8', 'Voisin_9', 'Voisin_10']
expected_line_lenght = len(columns)
df = []
for line in txt.split('\n'):
    try:
        data = [int(x) for x in line.split(';')]
        for _ in range(expected_line_lenght - len(data)):
            data.append(None)
        df.append(data)
    except:
        pass
df = pd.DataFrame(df, columns=columns)
df = spark.createDataFrame(df)


# COMMAND ----------

df.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Datalake.neighbouring_dep")
