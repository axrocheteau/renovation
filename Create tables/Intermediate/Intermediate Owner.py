# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate Owner

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

owner = (
    tremi.select(
        F.col('Respondent_Serial').alias('id_owner'),
        F.col('main_rs1').alias('gender'),
        F.col('main_rs2_c').alias('age'),
        F.col('main_rs5').alias('work_state'),
        F.col('main_rs6').alias('job'),
        F.col('main_q100_2').alias('home_state'),
        F.col('main_Q44').alias('arrival_date'),
        F.col('main_Q1_97').alias('has_done_renov'),
        F.col('main_RS102').alias('nb_persons_home'),
        F.col('main_RS182').alias('income'),
        F.col('main_Q73').alias('amount_help'),
        F.col('main_Q75').alias('loan_amount'),
        F.col('main_Q76').alias('loan_duration'),
        F.col('main_q77').alias('loan_rate')
    )
    .dropDuplicates()
)
display(owner)

# COMMAND ----------

# save as table
owner.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Intermediate.Owner")
