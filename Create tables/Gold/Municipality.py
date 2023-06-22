# Databricks notebook source
# MAGIC %md
# MAGIC # Municipality

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

# COMMAND ----------

municipality = (
    silver_municipality.select(
        F.col('postal_code'),
        F.col('department_number'),
        F.col('department_name'),
        F.col('former_region_name'),
        F.col('former_region_number'),
        F.col('new_region_name'),
        F.col('new_region_number')
    )
    .filter(
        ~F.col('department_number').isin(['2A' ,'2B'])
    )
    .dropDuplicates()
    .select(
        F.monotonically_increasing_id().alias('id_municipality'),
        '*'
    )
)

print(silver_municipality.count(), municipality.count())
display(municipality)

# COMMAND ----------


test = spark.sql("SELECT * FROM datalake.code_commune")
test2 = spark.sql("SELECT * FROM datalake.pop_commune_2016")
test3 = spark.sql("SELECT * FROM datalake.pop_commune_2020")

without_corsica = test.filter((~F.col('Code_commune_INSEE').contains('2A%')) & (~F.col('Code_commune_INSEE').contains('2B')))
without_dom = without_corsica.filter((F.col('Code_commune_INSEE').astype('int') < 96000))
without_corsica2 = test2.filter((~F.col('DEPCOM').contains('2A')) & (~F.col('DEPCOM').contains('2B')))

print(f"nb_code postaux : {test.select('Code_postal').dropDuplicates().count()}")
print(f"nb_code postaux sans corse : {without_corsica.select('Code_postal').dropDuplicates().count()}")

print(f"nb_code insee : {test.select('Code_commune_INSEE').dropDuplicates().count()}")
print(f"nb_code insee sans corse : {without_corsica.select('Code_commune_INSEE').dropDuplicates().count()}")

print(f"nb_code insee : {test2.select('COM').dropDuplicates().count()}")
print(f"nb_code insee sans corse : {without_corsica2.select('COM').dropDuplicates().count()}")

print(f"nb_code postaux sans dom : {without_dom.select('Code_postal').dropDuplicates().count()}")
