# Databricks notebook source
# MAGIC %md
# MAGIC # pred DPE
# MAGIC prediction of DPE comsumption and GES emission of housings from tremi dataset using Housings from DPE dataset

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
dpe = spark.sql("SELECT * FROM Gold.DPE")
housing = spark.sql("SELECT * FROM Gold.Housing")

# COMMAND ----------

training = (
    dpe.select(
        F.col('Type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('heating_emission'),
        F.col('GES_emission'),
        F.col('DPE_consumption')
    )
)

prediction = (
    housing.select(
        F.col('Type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('heating_emission'),
        F.col('GES_emission'),
        F.col('DPE_consumption')
    )
)
print(training.count(), prediction.count())
display(training)

# COMMAND ----------

training.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Model.training_dpe")
