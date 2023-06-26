# Databricks notebook source
# MAGIC %md
# MAGIC # pred Tremi
# MAGIC prdiction of surface, heating production and heating_emission variables for every owner in Tremi dataset using owner variables

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
owner = spark.sql("SELECT * FROM Gold.Owner")
housing = spark.sql("SELECT * FROM Gold.Housing")
municipality = spark.sql("SELECT * FROM Gold.Municipality")
municipality_info = spark.sql("SELECT * FROM Gold.Municipality_info")

# COMMAND ----------

training_tremi = (
    owner.join(
        housing.select(
            F.col('id_owner'),
            F.col('id_municipality'),
            F.col('first_date_renov'),
            F.col('surface'),
            F.col('heating_production'),
            F.col('heating_emission')
        ),
        ['id_owner'],
        'inner'
    )
    .join(
        municipality.select(
            F.col('id_municipality'),
            F.col('department_number')
        ),
        ['id_municipality'],
        'inner'
    )
    .join(
        municipality_info,
        [F.col('first_date_renov') == F.col('year'), F.col('housing.id_municipality') == F.col('municipality_info.id_municipality')],
        'inner'
    )
    .select(
        F.col('gender'),
        F.col('age'),
        F.col('occupation'),
        F.col('home_state'),
        F.col('nb_persons_home'),
        F.col('income'),
        F.col('population'),
        F.col('n_development_licence'),
        F.col('n_construction_licence'),
        F.col('n_new_buildings'),
        F.col('n_destruction_licence'),
        F.col('department_number'),
        F.col('surface'),
        F.col('heating_production'),
        F.col('heating_emission')
    )
)
print(training_tremi.count())
display(training_tremi)

# COMMAND ----------

training_surf = training_tremi.filter(F.col('surface').isNotNull()).drop('heating_emission', 'heating_production')
predicting_surf = training_tremi.filter(F.col('surface').isNull()).drop('heating_emission', 'heating_production')

training_prod = training_tremi.filter(F.col('heating_production').isNotNull()).drop('heating_emission', 'surface')
predicting_prod = training_tremi.filter(F.col('heating_production').isNull()).drop('heating_emission', 'surface')

training_em = training_tremi.filter(F.col('heating_emission').isNotNull()).drop('surface', 'heating_production')
predicting_em = training_tremi.filter(F.col('heating_emission').isNull()).drop('surface', 'heating_production')

# trainings = [training_surf, training_prod, training_em]
# prdictions = [predicting_surf, predicting_prod, predicting_em]

# trainings_name = ['training_surf', 'training_prod', 'training_em']
# prdictions_name = ['predicting_surf', 'predicting_prod', 'predicting_em']

# print(training_surf.count(), predicting_surf.count(), training_prod.count(), predicting_prod.count(), training_em.count(), predicting_em.count())
# display(training_surf)
# display(predicting_surf)
# display(training_prod)
# display(predicting_prod)
# display(training_em)
# display(predicting_em)



# COMMAND ----------

training_tremi.write.mode('overwrite')\
        .format("parquet")\
        .saveAsTable("Model.training_tremi")
