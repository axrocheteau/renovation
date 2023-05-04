# Databricks notebook source
# MAGIC %md
# MAGIC # Create Weather

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

weather = (df.select(F.col("Direction du vent moyen 10 mn").alias('wind_direction'),
                    F.col("Vitesse du vent moyen 10 mn").alias('wind_speed'),
                    F.col("Température").alias('temp_kelvin'),
                    F.col("Humidité").alias('humidity'),
                    F.col("Hauteur de la base des nuages de l'étage inférieur").alias('heigh_clouds'),
                    F.col("Température (°C)").alias('temp_degree'),
                    F.col("Altitude").alias('altitude'),
                    F.col("department (code)").alias('department_number'),
                    F.col("mois_de_l_annee").alias('month'),
                    F.year("Date").alias('year'))
    .groupBy('month', 'year', 'department_number').avg()
    .select('department_number',
            'year',
            'month',
            F.col('avg(wind_direction)').alias('wind_direction'),
            F.col('avg(wind_speed)').alias('wind_speed'),
            F.col('avg(temp_kelvin)').alias('temp_kelvin'),
            F.col('avg(humidity)').alias('humidity'),
            F.col('avg(heigh_clouds)').alias('heigh_clouds'),
            F.col('avg(temp_degree)').alias('temp_degree'),
            F.col('avg(altitude)').alias('altitude')
    )
)
display(weather)



# COMMAND ----------

# MAGIC %md
# MAGIC ### get data for departments that are not in the dataset

# COMMAND ----------

neigh = spark.sql("SELECT * FROM datalake.neighbouring_dep") # get neighbouring department
unpivotExpr = "stack(8, Voisin_1, Voisin_2, Voisin_3, Voisin_4, Voisin_5, Voisin_6, Voisin_7, Voisin_8) AS (neigh)"
neigh = neigh.select("Departement", F.expr(unpivotExpr)).where("neigh IS NOT NULL")

# get all departments that are not present in weather dataset
not_dep = neigh.join(weather.select('department_number').dropDuplicates(), neigh.Departement == weather.department_number ,'left_anti')
dates = weather.select('year', 'month').dropDuplicates()
all_possible_not_dep = not_dep.join(dates) # get all neighbors for every date possible
full_weather = (all_possible_not_dep.withColumnRenamed('neigh', 'department_number') # to join on neighbors easier this way
    .join(weather, ['department_number', 'month', 'year']) # get existing values for neighbors for every date
    .groupBy('Departement', 'month', 'year').avg() # get average for every date and neighbor
    .select(F.col('Departement').alias('department_number'),
            'year',
            'month',
            F.col('avg(wind_direction)').alias('wind_direction'),
            F.col('avg(wind_speed)').alias('wind_speed'),
            F.col('avg(temp_kelvin)').alias('temp_kelvin'),
            F.col('avg(humidity)').alias('humidity'),
            F.col('avg(heigh_clouds)').alias('heigh_clouds'),
            F.col('avg(temp_degree)').alias('temp_degree'),
            F.col('avg(altitude)').alias('altitude')
    ) # rename columns
    .union(weather) # get data for all departements
    .where("department_number IS NOT NULL") # remove unrelevant data
    .withColumn('department_number', F.when(F.length('department_number') == 1, F.concat(F.lit("0"), F.col('department_number'))) # uniformise department number
        .otherwise(F.col('department_number'))
    ) 
)
display(full_weather)

# COMMAND ----------

# save as table
full_weather.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Weather")

