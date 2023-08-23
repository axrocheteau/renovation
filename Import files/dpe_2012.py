# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
import io
import os
import requests

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

increase_url = 105
print_every = 100 * increase_url
print_now = 0

with open('/dbfs/FileStore/tables/dpe_2012.csv', "w") as f: 
    response = requests.get("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines?size=10000&page=1&format=csv")
    f.write(response.text + '\n')
    number = increase_url
    while response.text != '':
        if number > print_now:
            print(f'request with number {number//increase_url}')
            print_now += print_every
        url = f'https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines?size=10000&format=csv&after={number}&header=false'
        response = requests.get(url)
        f.write(response.text + '\n')
        number += increase_url
