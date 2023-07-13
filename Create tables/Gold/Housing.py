# Databricks notebook source
# MAGIC %md
# MAGIC # Housing

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
housing_silver = spark.sql("SELECT * FROM Silver.Housing")
owner_silver = spark.sql("SELECT * FROM Silver.Owner")
renovation = spark.sql("SELECT * FROM Silver.Renovation")
municipality = spark.sql("SELECT * FROM Gold.Municipality")


# COMMAND ----------

housing = (
    housing_silver.join(
        renovation.select(
            F.col('id_owner'),
            F.col('start_date')
        )
        .groupby('id_owner')
        .agg(F.min('start_date').alias('first_date_renov')),
        ['id_owner'],
        'left_outer'
    )
    .join(
        owner_silver.filter( # select owners who responded to every question necessary
            (F.col('nb_persons_home') != 99) & 
            (F.col('nb_persons_home').isNotNull()) &
            (F.col('age').isNotNull()) &
            (~F.col('income').isin([10,11,99])) &
            (F.col('income').isNotNull())  
        )
        .select(F.col('id_owner')),
        ['id_owner'],
        'inner'
    )
    .filter(F.col('type').isin([1,2])) # house or appartment
    .join(
        municipality.select(
            F.col('id_municipality'),
            F.col('postal_code')
        ),
        ['postal_code'],
        'inner'
    )
    .withColumns({ # modify values according to documentation
        'type': (
            F.when(F.col('type') == 1, 0)
            .when(F.col('type') == 2, 1)
        ),
        'construction_date' : F.col('construction_date'),
        'heating_system' : (
            F.when(F.col('heating_system') == 1, 0)
            .when(F.col('heating_system') == 2, 1)
        ),
        'hot_water_system' : (
            F.when(F.col('hot_water_system') == 1, 0)
            .when(F.col('hot_water_system') == 2, 1)
        ),
        'heating_production' : (
            F.when(F.col('heating_production').isin([1, 2]), 6) # elec
            .when(F.col('heating_production').isin([3, 4, 5]), 5) # PAC
            .when(F.col('heating_production').isin([6, 7]), 3) # bois /charbon
            .when(F.col('heating_production').isin([8, 10]), 2) # fioul
            .when(F.col('heating_production').isin([9]), 1) # gaz
            .when(F.col('heating_production').isin([11]), 4) # autre
        ),
        'surface' : (
            F.when(F.col('surface') < 70, 1)
            .when((F.col('surface') >= 70) & (F.col('surface') < 115), 2)
            .when(F.col('surface') >= 115, 3)
        ),
        'first_date_renov' : (
            F.when(F.col('has_done_renov') == 1, 2014)
            .otherwise(
                F.col('first_date_renov') + 2013
            )
        ),
        'has_done_renov' : (
            F.when(F.col('has_done_renov') == 1, 0)
            .when(F.col('has_done_renov') == 2, 1)
        ),
        'DPE_consumption' : F.lit(None).cast('string'),
        'GES_emission' : F.lit(None).cast('string')
    })
    .select(
        F.col('id_owner'),
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('heating_production'),
        F.col('hot_water_system'),
        F.col('surface'),
        F.col('has_done_renov'),
        F.col('first_date_renov'),
        F.col('DPE_consumption'),
        F.col('GES_emission')
    )
)
display(housing)

# COMMAND ----------

housing.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Housing")
