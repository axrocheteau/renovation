# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Municipality

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
silver_municipality = spark.sql("SELECT * FROM Silver.Municipality")
housings = spark.sql("SELECT * FROM Datalake.housings")

# COMMAND ----------

municipality = (
    silver_municipality.select(
        F.col('department_number'),
        F.col('department_name'),
        F.col('new_region_name'),
        F.col('new_region_number'),
        F.col('insee_code'),
        F.col('municipality_name')
    )
    .filter( # not in corsica or dom tom
        (~F.col('department_number').isin(['2A' ,'2B'])) &
        (F.col('department_number') < 96)
    )
    .join(
        housings.select(
            F.col('Code_commune_INSEE').alias('insee_code'),
            F.col('houses').alias('n_houses'),
            F.col('apartments').alias('n_apartments')
        ),
        ['insee_code'],
        'inner'
    )
    .withColumn('new_region_name', F.when(F.col('new_region_name') == 'Bretagne', 'BZH').otherwise(F.col('new_region_name')))
    .dropDuplicates()
    .select(
        F.monotonically_increasing_id().alias('id_municipality'),
        F.col('insee_code'),
        F.col('municipality_name'),
        F.col('department_number').cast('string'),
        F.col('department_name'),
        F.col('new_region_name'),
        F.col('new_region_number'),
        F.col('n_houses'),
        F.col('n_apartments')
    )
)


print(municipality.count())
display(municipality)

# COMMAND ----------

municipality.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("BI.Municipality")
