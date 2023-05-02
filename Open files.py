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
file_location_array = ["/FileStore/tables/Communes.csv",
                        "/FileStore/tables/Departements.csv",
                        "/FileStore/tables/PC_DP_creant_logements_2013_2016.csv",
                        "/FileStore/tables/Regions.csv",
                        "/FileStore/tables/TREMI_2017_CodeBook_public8.txt",
                        "/FileStore/tables/TREMI_2017_Résultats_enquête_bruts.csv",
                        "/FileStore/tables/anciennes_nouvelles_regions.csv",
                        "/FileStore/tables/code_commune.csv",
                        "/FileStore/tables/conso_elec_gaz_annuelle_par_secteur_dactivite_agregee_commune__1_.csv",
                        "/FileStore/tables/donnees_synop_essentielles_omm.csv",
                        "/FileStore/tables/dpe_france.csv",
                        "/FileStore/tables/permis_amenager.csv",
                        "/FileStore/tables/permis_demolir.csv",
                        "/FileStore/tables/dep_limitrophe.csv"          
]
file_type = "csv"
name_array = ["pop_commune",
                "pop_department",
                "construction_licence",
                "pop_region",
                "codebook",
                "tremi",
                "former_new_region",
                "code_commune",
                "elec",
                "weather",
                "dpe_france",
                "development_licence",
                "destruction_licence",
                "neighbouring_dep"
]

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter_array = [";", ";", ";", ";", "\t", ";", ";", ";", ";", ";", ",", ";", ";", ";"]

# The applied options are for CSV files. For other file types, these will be ignored.
for file_location, delimiter, name in zip(file_location_array, delimiter_array, name_array):
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
