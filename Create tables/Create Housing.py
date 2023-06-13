# Databricks notebook source
# MAGIC %md
# MAGIC # Create Housing

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
tremi = spark.sql("SELECT * FROM datalake.tremi")
display(tremi)

# COMMAND ----------

housing = (
    tremi.select(   
        F.col('Respondent_Serial').alias('id_owner'),
        F.when(F.col('cd_postal') < 10000, F.concat(F.lit("0"), F.col('cd_postal').cast('string')))
            .otherwise(F.col('cd_postal').cast('string'))
            .alias('postal_code'),
        F.col('main_Q101').alias('type'),
        F.col('main_Q102').alias('construction_date'),
        F.col('main_Q103').alias('heating_system'),
        F.col('main_Q104').alias('hot_water_system'),
        F.col('main_Q32').alias('heating_production'),
        F.col('main_Q34').alias('heating_emission'),
        F.col('main_Q35').alias('hot_water_production'),
        F.col('main_Q41q42').alias('surface'),
        F.col('main_Q1_97').alias('has_done_renov'),
        F.col('main_Q43').alias('DPE_before'),
        F.col('main_Q52').alias('thermal_comfort'),
        F.col('main_Q53').alias('energy_reduction'),
        F.col('main_Q38').alias('adjoining'),
        F.col('main_Q39').alias('n_floors'),
        F.col('main_Q40').alias('floor')
    )
    .dropDuplicates()
    .select(F.monotonically_increasing_id().alias('id_housing'),"*")
)
display(housing)

# COMMAND ----------

# save as table
housing.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Housing")
