# Databricks notebook source
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
df = spark.sql("SELECT * FROM datalake.weather")
display(df)

# COMMAND ----------

weather = df.select(F.col("Direction du vent moyen 10 mn").alias('wind_direction'), \
                    F.col("Vitesse du vent moyen 10 mn").alias('wind_speed'), \
                    F.col("Température").alias('temp_kelvin'), \
                    F.col("Humidité").alias('humidity'), \
                    F.col("Hauteur de la base des nuages de l'étage inférieur").alias('heigh_clouds'), \
                    F.col("Température (°C)").alias('temp_degree'), \
                    F.col("Altitude").alias('altitude'), \
                    F.col("department (code)").alias('department_number'), \
                    F.col("mois_de_l_annee").alias('month'), \
                    F.year("Date").alias('year')
)
weather = weather.groupBy('month', 'year', 'department_number').avg()
weather = weather.select('month',
                'year',
                'department_number',
                F.col('avg(wind_direction)').alias('wind_direction'),
                F.col('avg(wind_speed)').alias('wind_speed'),
                F.col('avg(temp_kelvin)').alias('temp_kelvin'),
                F.col('avg(humidity)').alias('humidity'),
                F.col('avg(heigh_clouds)').alias('heigh_clouds'),
                F.col('avg(temp_degree)').alias('temp_degree'),
                F.col('avg(altitude)').alias('altitude')
)
display(weather)



# COMMAND ----------

neigh = spark.sql("SELECT * FROM datalake.neighbouring_dep")
display(neigh)
