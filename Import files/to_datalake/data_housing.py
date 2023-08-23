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

# COMMAND ----------

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType

# COMMAND ----------

try:
    token = dbutils.widgets.get("token")
except:
    token = 'b62243bd-e5d8-30b2-a9c0-c2ee74a03940'
headers = {'Accept': 'application/xml', 'Authorization': f'Bearer {token}'}
schema = StructType([
    StructField("houses", IntegerType(), False),
    StructField("apartments", IntegerType(), False)
])

# get the nb of houses and apartments from a town
def get_housing(insee_code):
    houses = 0
    apartments = 0
    try:
        url = f'https://api.insee.fr/donnees-locales/V0.1/donnees/geo-TYPLR-CATL@GEO2023RP2020/COM-{insee_code}.1%2B2.ENS'
        response = requests.get(url, headers=headers)
        root = ET.fromstring(response.text)
        for Cellule in root.findall('Cellule'):
            if Cellule.find('Mesure').text == 'Nombre de logements':
                if Cellule.find('Modalite').get('code') == "1":
                    houses = int(float(Cellule.find('Valeur').text))
                else:
                    apartments = int(float(Cellule.find('Valeur').text))
    except Exception:
        pass
    return (houses, apartments)

# make it a udf
map_udf = udf(get_housing, schema)

# COMMAND ----------

# use udf to get houses and apartments
df = (
    municipalities
    .select(
        F.col('Code_commune_INSEE'),
        map_udf(F.col('Code_commune_INSEE')).alias('g')
    )
    .select(
        ['Code_commune_INSEE'] + [f"g.{col}" for col in schema.names]
    )
)
print(df.count())
display(df)

# COMMAND ----------

df.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Datalake.housings")
