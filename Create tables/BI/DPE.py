# Databricks notebook source
# MAGIC %md
# MAGIC # DPE

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np

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
dpe_2021 = spark.sql("SELECT * FROM Datalake.dpe_france_2021")
predicted_renov = spark.sql("SELECT * FROM Model.predicted_renov")
municipality = spark.sql("SELECT * FROM Silver.Municipality")

# COMMAND ----------

def to_categorical(x):
    if(x) ==  'insuffisante':
        return 1
    elif(x) == 'moyenne':
        return 2
    elif(x) == 'bonne':
        return 3
    elif(x) == 'très bonne':
        return 4
    else:
        return x

to_categorical_udf = udf(lambda x: to_categorical(x))

def mean_coalesce(x, y, z):
    if x is None and  y is None and z is None:
        return x
    else:
        return float(np.mean([value for value in [x,y,z] if value is not None and value > 0]))

mean_coalesce_udf = udf(lambda x, y, z: mean_coalesce(x, y, z))

# COMMAND ----------


dpe = (
    dpe_2021.select(
        F.col("N°DPE").alias('id_dpe'),
        F.col("Surface_habitable_logement").alias('surface'),
        F.col("Code_INSEE_(BAN)").alias('insee_code'),
        F.col("Qualité_isolation_enveloppe"),
        F.col("Qualité_isolation_menuiseries"),
        F.col("Qualité_isolation_murs"),
        F.col("Qualité_isolation_plancher_bas"),
        F.col("Qualité_isolation_plancher_haut_toit_terrase"),
        F.col("Qualité_isolation_plancher_haut_comble_aménagé"),
        F.col("Qualité_isolation_plancher_haut_comble_perdu"),
    )
    .withColumns({
        'quality_shell_insulation' : to_categorical_udf(F.col("Qualité_isolation_enveloppe")).cast('int'),
        'quality_walls_insulation' : to_categorical_udf(F.col("Qualité_isolation_murs")).cast('int'), 
        'quality_carpentry_insulation' : to_categorical_udf(F.col("Qualité_isolation_menuiseries")).cast('int'),
        'quality_flooring_insulation' : to_categorical_udf(F.col("Qualité_isolation_plancher_bas")).cast('int'),
        'quality_ceiling_insulation_terrace' : to_categorical_udf(F.col("Qualité_isolation_plancher_haut_toit_terrase")).cast('int'),
        'quality_ceiling_insulation_developed' : to_categorical_udf(F.col("Qualité_isolation_plancher_haut_comble_aménagé")).cast('int'),
        'quality_ceiling_insulation_unused' : to_categorical_udf(F.col("Qualité_isolation_plancher_haut_comble_perdu")).cast('int'),
        'quality_ceiling_insulation' : (
            mean_coalesce_udf(
                F.col('quality_ceiling_insulation_terrace'),
                F.col("quality_ceiling_insulation_developed"),
                F.col("quality_ceiling_insulation_unused")
            )
        ).cast('float')
    })
    .join(
        predicted_renov.select(
            F.col('id_dpe'),
            F.col('type'),
            F.col('construction_date'),
            F.col('heating_system'),
            F.col('hot_water_system'),
            F.col('heating_production'),
            F.col('DPE_consumption'),
            F.col('GES_emission'),
            F.col('has_to_renov')
        ),
        ['id_dpe'],
        'inner'
    )
    .select(
        F.col('id_dpe'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('has_to_renov'),
        F.col('insee_code'),
        F.col('surface'),
        F.col('quality_shell_insulation'),
        F.col('quality_walls_insulation'),
        F.col('quality_carpentry_insulation'),
        F.col('quality_flooring_insulation'),
        F.col('quality_ceiling_insulation')

    )
)

print(f'{dpe_2021.count() = }, {dpe.count() = }')
display(dpe)

# COMMAND ----------

print(dpe.dtypes)

# COMMAND ----------

dpe.select('Qualité_isolation_plancher_bas').dropDuplicates().show()

# COMMAND ----------

dpe.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.DPE")
