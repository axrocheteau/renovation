# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# Create Database store raw files
spark.sql("CREATE DATABASE IF NOT EXISTS Datalake")

# Create Database store modified files
spark.sql("CREATE DATABASE IF NOT EXISTS Gold")

# COMMAND ----------


# create database
# File location and type
# create database
# File location and type
info_array = [
    {"location" : "/FileStore/tables/Communes.csv", "name": "pop_commune", "delimiter": ";"},
    {"location" : "/FileStore/tables/Departements.csv", "name": "pop_department", "delimiter": ";"},
    {"location" : "/FileStore/tables/PC_DP_creant_logements_2013_2016.csv", "name": "construction_licence", "delimiter": ";"},
    {"location" : "/FileStore/tables/Regions.csv", "name": "pop_region", "delimiter": ";"},
    {"location" : "/FileStore/tables/TREMI_2017_CodeBook_public8.txt", "name": "codebook", "delimiter": "\t"},
    {"location" : "/FileStore/tables/TREMI_2017_Résultats_enquête_bruts.csv", "name": "tremi", "delimiter": ";"},
    {"location" : "/FileStore/tables/anciennes_nouvelles_regions.csv", "name": "former_new_region", "delimiter": ";"},
    {"location" : "/FileStore/tables/code_commune.csv", "name": "code_commune", "delimiter": ";"},
    {"location" : "/FileStore/tables/conso_elec_gaz_annuelle_par_secteur_dactivite_agregee_commune__1_.csv", "name": "elec", "delimiter": ";"},
    {"location" : "/FileStore/tables/donnees_synop_essentielles_omm.csv", "name": "weather", "delimiter": ";"},
    {"location" : "/FileStore/tables/dpe_france.csv", "name": "dpe_france", "delimiter": ","},
    {"location" : "/FileStore/tables/permis_amenager.csv", "name": "development_licence", "delimiter": ";"},
    {"location" : "/FileStore/tables/permis_demolir.csv", "name": "destruction_licence", "delimiter": ";"},
    {"location" : "/FileStore/tables/dep_limitrophe.csv", "name": "neighbouring_dep", "delimiter": ";"}
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
    df.write.mode("overwrite")\
        .format("parquet") \
        .saveAsTable(f"datalake.{name}")

# COMMAND ----------

# to recreate only one specific table
file_location = "/FileStore/tables/dep_limitrophe.csv"
file_type = "csv"
name = "neighbouring_dep"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
if name == "weather":
    new_column_name_list= [name.replace(',','') for name in df.columns]
    df = df.toDF(*new_column_name_list)
df.write.mode('overwrite')\
    .format("parquet") \
    .saveAsTable(f"Datalake.{name}")
