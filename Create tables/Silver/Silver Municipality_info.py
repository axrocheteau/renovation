# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Municipality info

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, Row

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

pop_commune_2016 = spark.sql("SELECT * FROM datalake.pop_commune_2016")
pop_commune_2020 = spark.sql("SELECT * FROM datalake.pop_commune_2020")
construction_licence_2016 = spark.sql("SELECT * FROM datalake.construction_licence_2016")
construction_licence_2023 = spark.sql("SELECT * FROM datalake.construction_licence_2023")
destruction_licence = spark.sql("SELECT * FROM datalake.destruction_licence")
development_licence = spark.sql("SELECT * FROM datalake.development_licence")
code_commune = spark.sql("SELECT * FROM datalake.code_commune")
municipality = spark.sql("SELECT * FROM Silver.Municipality")

# COMMAND ----------

# MAGIC %md
# MAGIC ### population

# COMMAND ----------

# create fake column year for the population count
column = ['year']
list_year_2018 = [2012, 2013, 2014, 2015, 2016, 2017, 2018]
year_2018 = map(lambda x : Row(x), list_year_2018)
df_year_2018 = spark.createDataFrame(year_2018, column)

list_year_2022 = [2019, 2020, 2021, 2022]
year_2022 = map(lambda x : Row(x), list_year_2022)
df_year_2022 = spark.createDataFrame(year_2022, column)

pop_commune = (
    pop_commune_2020.withColumns({
        'CODCOM_corrected': ( # put this codcom as string to be able to create real insse_code
            F.when(F.col('CODCOM') < 10, F.concat(F.lit('00'), F.col('CODCOM').astype('string')))
            .when(F.col('CODCOM') < 100, F.concat(F.lit('0'), F.col('CODCOM').astype('string')))
            .otherwise(F.col('CODCOM').astype('string'))
        ),
        'insee_code': F.concat(F.col('CODDEP').astype('string'), F.col('CODCOM_corrected')),
        'population': F.col('PMUN')
    })
    .select(
        'insee_code',
        'population'
    )
    .join(df_year_2022) # cartesian product with years
    .union( # add population from 2016
        (
            pop_commune_2016.select(
                F.col('DEPCOM').alias('insee_code'),
                F.col('PMUN').alias('population')
            )
            .join(df_year_2018) # cartesian product with years
        )
    )
    .join( # replace postal_code with id_municipality
        (
            code_commune.select(
                F.col('Code_commune_INSEE').alias('insee_code'),
                # correct postal codes interpreted as int
                (
                    F.when(F.col('Code_postal') < 10000, F.concat(F.lit("0"), F.col('Code_postal').cast('string')))
                    .otherwise(F.col('Code_postal').cast('string'))
                ).alias('postal_code') 
            )
            .dropDuplicates()
        ),
        ['insee_code'],
        'inner'
    )
)

print(pop_commune.count(), pop_commune_2020.count(), pop_commune_2016.count())
display(pop_commune)

# COMMAND ----------

# MAGIC %md
# MAGIC ### construction_licence

# COMMAND ----------

construction_licence = ( # all construction licences
    construction_licence_2016.select(
        F.col('NB_LGT_TOT_CREES').alias('nb_housing'),
        F.col('COMM').alias('insee_code'),
        F.year(F.col('DATE_REELLE_AUTORISATION')).alias('year')
    )
    .groupBy('insee_code', 'year')
    .agg(
        F.sum('nb_housing').alias('n_new_buildings'),
        F.count('nb_housing').alias('n_construction_licence')
    )
    .union(
        construction_licence_2023.select(
            F.col('NB_LGT_TOT_CREES').alias('nb_housing'),
            F.col('COMM').alias('insee_code'),
            F.year(F.col('DATE_REELLE_AUTORISATION')).alias('year')
        )
        .groupBy('insee_code', 'year')
        .agg(
            F.sum('nb_housing').alias('n_new_buildings'),
            F.count('nb_housing').alias('n_construction_licence')
        )
        .filter(F.col('year') < 2023) # cut current year
    )
)

display(construction_licence)


# COMMAND ----------

municipality_info = (
    pop_commune.join( # construction licence
        construction_licence,
        ['insee_code', 'year'],
        'left_outer'
    )
    .join( # get destruction licence
        destruction_licence
            .select(
                F.col('COMM').alias('insee_code'),
                F.year(F.col('DATE_REELLE_AUTORISATION')).alias('year')
            )
            .groupBy('insee_code', 'year')
            .agg(F.count('insee_code').alias('n_destruction_licence')),
        ['insee_code', 'year'],
        'left_outer'
    )
    .join( # get development licence
        development_licence
            .select(
                F.col('COMM').alias('insee_code'),
                F.year(F.col('DATE_REELLE_AUTORISATION')).alias('year')
            )
            .groupBy('insee_code', 'year')
            .agg(F.count('insee_code').alias('n_development_licence')),
        ['insee_code', 'year'],
        'left_outer'
    )
    .dropDuplicates()
    .withColumns({ # put null values to 0 to make sense
        'population': F.when(F.col('population').isNull(), 0).otherwise(F.col('population')),
        'n_new_buildings': F.when(F.col('n_new_buildings').isNull(), 0).otherwise(F.col('n_new_buildings')),
        'n_development_licence': F.when(F.col('n_development_licence').isNull(), 0).otherwise(F.col('n_development_licence')),
        'n_destruction_licence': F.when(F.col('n_destruction_licence').isNull(), 0).otherwise(F.col('n_destruction_licence')),
        'n_construction_licence': F.when(F.col('n_construction_licence').isNull(), 0).otherwise(F.col('n_construction_licence'))
    })
    .groupBy('postal_code', 'year') # aggregate to postal_code level
    .agg(
        F.sum('population').alias('population'),
        F.sum('n_construction_licence').alias('n_construction_licence'),
        F.sum('n_new_buildings').alias('n_new_buildings'),
        F.sum('n_destruction_licence').alias('n_destruction_licence'),
        F.sum('n_development_licence').alias('n_development_licence')
    )
    .join( # replace postal_code with id_municipality
        municipality
            .select(
                F.col('id_municipality'),
                F.col('postal_code')
            ),
        ['postal_code'],
        'inner'
    )
    .filter( # get only metropolitan france
        (~F.col('postal_code').contains('2A')) &
        (~F.col('postal_code').contains('2B')) &
        (F.col('postal_code').astype('int') < 96000)
    )
    .dropDuplicates()
    .drop('postal_code')
)    

display(municipality_info)  

# COMMAND ----------

municipality_info.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Silver.Municipality_info")
