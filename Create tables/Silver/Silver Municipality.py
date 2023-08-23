# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Municipality

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
intermediate_municipality = spark.sql("SELECT * FROM Intermediate.Municipality")

# COMMAND ----------

municipality = (
    intermediate_municipality.select(
        F.col('postal_code'),
        F.col('department_number').cast('int'),
        F.col('department_name'),
        F.col('former_region_name'),
        F.col('former_region_number'),
        F.col('new_region_name'),
        F.col('new_region_number')
    )
    .filter( # not in corsica
        ~F.col('department_number').isin(['2A' ,'2B'])
    )
    .dropDuplicates() # to change granularity to postal_code
    .select(
        F.monotonically_increasing_id().alias('id_municipality'),
        '*'
    )
)


# COMMAND ----------

municipality.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Silver.Municipality")
