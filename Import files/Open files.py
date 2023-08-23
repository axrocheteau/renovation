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


# create database
# File location and type

info_array = [
    {"location" : "/FileStore/tables/pop_commune_2016.csv", "name": "pop_commune_2016", "delimiter": ";"},
    {"location" : "/FileStore/tables/pop_commune_2020.csv", "name": "pop_commune_2020", "delimiter": ";"},
    {"location" : "/FileStore/tables/pop_dep_2016.csv", "name": "pop_department_2016", "delimiter": ";"},
    {"location" : "/FileStore/tables/construction_licence_2016.csv", "name": "construction_licence_2016", "delimiter": ";"},
    {"location" : "/FileStore/tables/construction_licence_2023.csv", "name": "construction_licence_2023", "delimiter": ";"},
    {"location" : "/FileStore/tables/tremi.csv", "name": "tremi", "delimiter": ";"},
    {"location" : "/FileStore/tables/former_new_region.csv", "name": "former_new_region", "delimiter": ";"},
    {"location" : "/FileStore/tables/code_commune.csv", "name": "code_commune", "delimiter": ";"},
    {"location" : "/FileStore/tables/elec.csv", "name": "elec", "delimiter": ";"},
    {"location" : "/FileStore/tables/weather.csv", "name": "weather", "delimiter": ";"},
    {"location" : "/FileStore/tables/dpe_2012.csv", "name": "dpe_france_2012", "delimiter": ","},
    {"location" : "/FileStore/tables/dpe_2021.csv", "name": "dpe_france_2021", "delimiter": ","},
    {"location" : "/FileStore/tables/development_licence.csv", "name": "development_licence", "delimiter": ";"},
    {"location" : "/FileStore/tables/destruction_licence.csv", "name": "destruction_licence", "delimiter": ";"},
    # {"location" : "/FileStore/tables/TREMI_2017_CodeBook_public8.txt", "name": "codebook", "delimiter": "\t"},
    # {"location" : "/FileStore/tables/dep_limitrophe.csv", "name": "neighbouring_dep", "delimiter": ";"},
    # {"location" : "/FileStore/tables/logements.csv", "name": "housings", "delimiter": ";"}
]         

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"

# The applied options are for CSV files. For other file types, these will be ignored.
for file in info_array:
    delimiter = file["delimiter"]
    file_location = file["location"]
    name = file["name"]
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location)
    if name == "weather":
        new_column_name_list= [column_name.replace(',','') for column_name in df.columns]
        df = df.toDF(*new_column_name_list)
    if 'dpe' in name:
        df = df.dropDuplicates()
    if name == 'code_commune':
        df.withColumnRenamed('#Code_commune_INSEE', 'Code_commune_INSEE')
    df.write.mode("overwrite")\
        .format("parquet") \
        .saveAsTable(f"datalake.{name}")

# COMMAND ----------

# # to recreate only one specific table
# file_location = "/FileStore/tables/logements.csv"
# file_type = "csv"
# name = "housings"

# # CSV options
# infer_schema = "true"
# first_row_is_header = "true"
# delimiter = ";"

# # The applied options are for CSV files. For other file types, these will be ignored.
# df = spark.read.format(file_type) \
#     .option("inferSchema", infer_schema) \
#     .option("header", first_row_is_header) \
#     .option("sep", delimiter) \
#     .load(file_location)
# if name == "weather":
#     new_column_name_list= [name.replace(',','') for name in df.columns]
#     df = df.toDF(*new_column_name_list)
# df.write.mode('overwrite')\
#     .format("parquet") \
#     .saveAsTable(f"Datalake.{name}")
