# Databricks notebook source
# MAGIC %md
# MAGIC # Gold DPE

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
gold_municipality = spark.sql("SELECT * FROM Gold.Municipality")

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

def to_renov(x):
    if x is None or x < 2:
        return 0
    else:
        return 1
    
to_renov_udf = udf(lambda x: to_renov(x))

def mean_coalesce(x, y, z):
    if x is None and  y is None and z is None:
        return x
    else:
        return float(np.mean([value for value in [x,y,z] if value is not None and value > 0]))

mean_coalesce_udf = udf(lambda x, y, z: mean_coalesce(x, y, z))

def to_dictionary(dictionary, x):
    return dictionary[x]

dpe_ges_dict = {0:'A', 1:'B', 2:'C', 3:'D', 4:'E', 5:'F', 6:'G'}
heating_prod_dict = {1:'Gaz', 2:'fioul, GPL, propane, butane', 3:' bois, charbon', 4:'Autres', 5:'PAC', 6:'electricité'}
quality_dict = {1 : 'insuffisante', 2 : 'moyenne', 3 : 'bonne', 4 : 'très bonne'}
surface_dict = {1 : '<70 m²', 2:'70-115 m²', 3:'>115 m²'}
construction_date_dict = {
    1 : '<1948',
    2 : '1949-1974',
    3 : '1975-1981',
    4 : '1982-1989',
    5 : '1990-2000',
    6 : '2001-2011',
    7 : '>2012',
}

def make_expr(column_name, dictionary):
    return f"""CASE {' '.join([f"WHEN {column_name}='{k}' THEN '{v}'" for k,v in dictionary.items()])} ELSE {column_name} END"""



# COMMAND ----------


dpe = (
    dpe_2021.select(
        F.col("N°DPE").alias('id_dpe'),
        F.col("Code_INSEE_(BAN)").alias('insee_code'),
        F.col("Type_bâtiment").alias('type'),
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
        ).cast('int')
    })
    .join(
        predicted_renov.select(
            F.col('id_dpe'),
            F.col('surface'),
            F.col('construction_date'),
            F.col('heating_production'),
            F.col('DPE_consumption'),
            F.col('GES_emission'),
            F.col('has_to_renov'),
        ),
        ['id_dpe'],
        'inner'
    )
    .withColumns({
        'renov_energy' : (
            F.when(F.col('heating_production') == 2, 1)
            .when(
                (F.col('heating_production') == 1) | (F.col('heating_production') == 3),
                F.when((F.col('DPE_consumption') >= 4) | (F.col('GES_emission') >= 4), 1)
                .otherwise(0)
            )
            .otherwise(0)
        ),
        'heating_production' : F.expr(make_expr('heating_production', heating_prod_dict)),
        'DPE_consumption' : F.expr(make_expr('DPE_consumption', dpe_ges_dict)),
        'GES_emission' : F.expr(make_expr('GES_emission', dpe_ges_dict)),
        'surface' : F.expr(make_expr('surface', surface_dict)),
        'construction_date' : F.expr(make_expr('construction_date', construction_date_dict)),
        'renov_shell' : to_renov_udf(F.col('quality_shell_insulation')).cast('int'),
        'renov_walls' : to_renov_udf(F.col('quality_walls_insulation')).cast('int'),
        'renov_carpentry' : to_renov_udf(F.col('quality_carpentry_insulation')).cast('int'),
        'renov_flooring' : to_renov_udf(F.col('quality_flooring_insulation')).cast('int'),
        'renov_ceiling' : to_renov_udf(F.col('quality_ceiling_insulation')).cast('int'),
    })
    .join(
        gold_municipality.select(
            F.col('insee_code'),
            F.col('id_municipality')
        ),
        ['insee_code'],
        'inner'
    )
)


# COMMAND ----------

stack_expr = "stack(5, 'murs', renov_walls, 'toiture', renov_carpentry, 'plancher', renov_flooring, 'plafond', renov_ceiling, 'énergie', renov_energy) AS (renov_type, renov)"
renov = (
    dpe.select(
        F.col('id_dpe'),
        F.col('id_municipality'),
        F.expr(stack_expr)
    )
    .filter(
        F.col('renov') == 1
    )
    .select(
        F.col('id_dpe'),
        F.col('id_municipality'),
        F.col('renov_type')
    )
)


# COMMAND ----------

renov.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Renovation")

# COMMAND ----------

dpe = (
    dpe.select(
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_production'),
        F.col('surface'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('has_to_renov'),
    )
    .groupBy(
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_production'),
        F.col('surface'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('has_to_renov'),
    )
    .count()
)

# COMMAND ----------

dpe.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.dpe")
