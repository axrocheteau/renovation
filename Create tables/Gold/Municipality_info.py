# Databricks notebook source
# MAGIC %md
# MAGIC # Municipality info

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, Row

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

pop_commune_2016 = spark.sql("SELECT * FROM datalake.pop_commune_2016")
pop_commune_2020 = spark.sql("SELECT * FROM datalake.pop_commune_2020")
construction_licence_2016 = spark.sql("SELECT * FROM datalake.construction_licence_2016")
construction_licence_2023 = spark.sql("SELECT * FROM datalake.construction_licence_2023")
destruction_licence = spark.sql("SELECT * FROM datalake.destruction_licence")
development_licence = spark.sql("SELECT * FROM datalake.development_licence")
code_commune = spark.sql("SELECT * FROM datalake.code_commune")
municipality = spark.sql("SELECT * FROM Gold.Municipality")

# COMMAND ----------

column = ['year']
list_year_2018 = [2012, 2013, 2014, 2015, 2016, 2017, 2018]
year_2018 = map(lambda x : Row(x), list_year_2018)

list_year_2022 = [2019, 2020, 2021, 2022]
year_2022 = map(lambda x : Row(x), list_year_2022)


df_year_2018 = spark.createDataFrame(year_2018, column)
df_year_2022 = spark.createDataFrame(year_2022, column)

pop_commune = (
    pop_commune_2020.withColumns({
        'insee_code': F.concat(F.col('CODDEP'), F.col('CODCOM')),
        'population': F.col('PMUN')
    })
    .select(
        'insee_code',
        'population'
    )
    .join(
        df_year_2022
    )
)

print(pop_commune.count(), pop_commune_2020.count())
display(pop_commune)

# COMMAND ----------

# load df
tremi = spark.sql("SELECT * FROM datalake.tremi")
Dictionnary = spark.sql("SELECT * FROM Silver.Dictionary")

# COMMAND ----------


