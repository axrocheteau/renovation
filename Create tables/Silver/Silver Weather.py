# Databricks notebook source
# MAGIC %md
# MAGIC # Weather

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
df = spark.sql("SELECT * FROM datalake.weather")
display(df)


# COMMAND ----------

neigh = spark.sql("SELECT * FROM datalake.neighbouring_dep") # get neighbouring department
unpivotExpr = "stack(8, Voisin_1, Voisin_2, Voisin_3, Voisin_4, Voisin_5, Voisin_6, Voisin_7, Voisin_8) AS (neigh)"
neigh = neigh.select("Departement", F.expr(unpivotExpr)).where("neigh IS NOT NULL")

weather = (
    df.select(
        F.col("Vitesse du vent moyen 10 mn").alias('wind_speed'),
        F.col("Humidité").alias('humidity'),
        F.col("Température (°C)").alias('temp_degree'),
        F.col("department (code)").alias('department_number'),
        F.year("Date").alias('year')
    )
    .filter( # filter corsica and dom and throw away current year
        (~F.col('department_number').isin(['2a', '2b', '2A', '2B'])) &
        (F.col('department_number').astype('int') < 96) &
        (F.col('year') < 2023)
    )
    .groupBy('year', 'department_number').avg()
    .select(
        'department_number',
        'year',
        F.round(F.col('avg(wind_speed)'),2).alias('wind_speed'),
        F.round(F.col('avg(humidity)'),2).alias('humidity'),
        F.round(F.col('avg(temp_degree)'),2).alias('temp_degree'),
    )
    .dropna()
)

# get all departments that are not present in weather dataset
while True:
    all_possible_not_dep = (
        neigh.join(
            weather.select('year').dropDuplicates()
        )
        .withColumnRenamed("Departement", "department_number")
        .join(
            weather.select('department_number', 'year').dropDuplicates(),
            ['department_number', 'year'],
            'left_anti'
        )
    )

    weather = (
        all_possible_not_dep.withColumnRenamed('department_number', 'Departement') 
        .withColumnRenamed('neigh', 'department_number') # to join on neighbors easier this way
        .join(weather, ['department_number', 'year']) # get existing values for neighbors for every date
        .groupBy('Departement', 'year').avg() # get average for every date and neighbor
        .select(
            F.col('Departement').alias('department_number'),
            'year',
            F.round(F.col('avg(wind_speed)'),2).alias('wind_speed'),
            F.round(F.col('avg(humidity)'),2).alias('humidity'),
            F.round(F.col('avg(temp_degree)'),2).alias('temp_degree'),
        ) # rename columns
        .union(weather) # get data for all departements
        .where("department_number IS NOT NULL") # remove unrelevant data
        .withColumn(
            'department_number', 
            F.when(F.length('department_number') == 1, F.concat(F.lit("0"), F.col('department_number'))) # uniformise department number
            .otherwise(F.col('department_number'))
        )
    )
    if all_possible_not_dep.count() == 0: # break if all departments have values
        break

display(weather.orderBy("year","department_number"))

# COMMAND ----------

# save as table
weather.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Weather")

