# Databricks notebook source
# MAGIC %md
# MAGIC # pred Tremi
# MAGIC prdiction of surface, heating production and heating_emission variables for every owner in Tremi dataset using owner variables

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# load df
owner = spark.sql("SELECT * FROM Gold.Owner")
housing = spark.sql("SELECT * FROM Gold.Housing")
municipality = spark.sql("SELECT * FROM Gold.Municipality")
municipality_info = spark.sql("SELECT * FROM Gold.Municipality_info")

# COMMAND ----------

training_tremi = (
    owner.join(
        housing.select(
            F.col('id_owner'),
            F.col('id_municipality'),
            F.col('first_date_renov'),
            F.col('surface')
        ),
        ['id_owner'],
        'inner'
    )
    .join(
        municipality.select(
            F.col('id_municipality'),
            F.col('department_number')
        ),
        ['id_municipality'],
        'inner'
    )
    .join(
        municipality_info,
        F.col('first_date_renov') == F.col('year'),
        'inner'
    )
    .select(
        F.col('gender'),
        F.col('age'),
        F.col('occupation'),
        F.col('home_state'),
        F.col('nb_persons_home'),
        F.col('income_home'),
        F.col('population'),
        F.col('n_development_licence'),
        F.col('n_construction_licence'),
        F.col('n_new_buildings'),
        F.col('n_destruction_licence'),
        F.col('department_number'),
        F.col('surface')
    )
)
display(training_tremi)

# COMMAND ----------

# dpe.write.mode('overwrite')\
#         .format("parquet") \
#         .saveAsTable("Gold.DPE")
