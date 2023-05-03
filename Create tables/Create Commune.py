# Databricks notebook source
# MAGIC %md
# MAGIC # Create Commune

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.pandas import DataFrame as D

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
df = spark.sql("SELECT * FROM datalake.pop_commune")

# COMMAND ----------

pop_commune = spark.sql("SELECT * FROM datalake.pop_commune")
former_new_region = spark.sql("SELECT * FROM datalake.former_new_region")
dpe_france = spark.sql("SELECT * FROM datalake.dpe_france")
elec = spark.sql("SELECT * FROM datalake.elec")
construction_licence = spark.sql("SELECT * FROM datalake.construction_licence")
destruction_licence = spark.sql("SELECT * FROM datalake.destruction_licence")
development_licence = spark.sql("SELECT * FROM datalake.development_licence")
code_commune = spark.sql("SELECT * FROM datalake.code_commune")
pop_department = spark.sql("SELECT * FROM datalake.pop_department")
pop_region = spark.sql("SELECT * FROM datalake.pop_region")

# COMMAND ----------

display(dpe_france)

# COMMAND ----------

df_commune = (
    pop_commune.select(F.col('DEPCOM').alias('code_insee'),
                        F.col('COM').alias('commune_name'),
                        F.col('PMUN').alias('population'),
                        F.col('DEPCOM').substr(0,2).alias('department_number')
    )
    .join(
        dpe_france.filter(
                F.col('date_etablissement_dpe').between(F.lit("2014-01-01"), F.lit("2017-01-01")) &
                F.col('classe_consommation_energie') != 'N' &
                F.col('classe_estimation_ges') != 'N' 
            )
            .select(
                F.col('classe_consommation_energie')
                    .when() # transform letter in digits to do avg
                    .alias('dpe'),
                F.col('classe_estimation_ges').alias('ges'),
                F.col('code_insee_commune_actualise').alias('code_insee'),
            )
            .groupBy('code_insee').agg(F.abg('Age'), F.count('Age'))
            select(),
        ['code_insee'],
    )
)
display(df_commune)

# COMMAND ----------

# save as table
Dictionnary.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Dictionnary")
