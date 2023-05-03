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

display(construction_licence)

# COMMAND ----------

df_commune = (
    pop_commune.select(F.col('DEPCOM').alias('code_insee'),
                        F.col('COM').alias('commune_name'),
                        F.col('PMUN').alias('population'),
                        F.col('DEPCOM').substr(0,2).alias('department_number')
    )
    .join(
        dpe_france.filter(
                (F.col('date_etablissement_dpe').between(F.lit("2014-01-01"), F.lit("2017-01-01"))) &
                (F.col('classe_consommation_energie') != 'N') &
                (F.col('classe_estimation_ges') != 'N') 
            )
            .select(
                F.when(F.col('classe_consommation_energie') == 'A', 1.0)
                    .when(F.col('classe_consommation_energie') == 'B', 2.0)
                    .when(F.col('classe_consommation_energie') == 'C', 3.0)
                    .when(F.col('classe_consommation_energie') == 'D', 4.0)
                    .when(F.col('classe_consommation_energie') == 'E', 5.0)
                    .when(F.col('classe_consommation_energie') == 'F', 6.0)
                    .when(F.col('classe_consommation_energie') == 'G', 7.0)
                    .otherwise(0.0)
                    .alias('dpe'),
                F.when(F.col('classe_estimation_ges') == 'A', 1.0)
                    .when(F.col('classe_estimation_ges') == 'B', 2.0)
                    .when(F.col('classe_estimation_ges') == 'C', 3.0)
                    .when(F.col('classe_estimation_ges') == 'D', 4.0)
                    .when(F.col('classe_estimation_ges') == 'E', 5.0)
                    .when(F.col('classe_estimation_ges') == 'F', 6.0)
                    .when(F.col('classe_estimation_ges') == 'G', 7.0)
                    .otherwise(0.0)
                    .alias('ges'),
                F.col('code_insee_commune_actualise').alias('code_insee'),
            )
            .groupBy('code_insee').agg(F.avg('dpe'), F.avg('ges'), F.count('code_insee'))
            .select(
                F.col('code_insee'),
                F.round(F.col('avg(dpe)'),2).alias('avg_dpe'),
                F.round(F.col('avg(ges)'),2).alias('avg_ges'),
                F.col('count(code_insee)').alias('n_dpe')
            ),
        ['code_insee'],
        'left_outer'
    )
    .join(
        elec.filter(
                (F.col('Année').between(2014, 2016)) &
                (F.col('Filière') == 'Electricité')
            )
            .select(
                (F.col('Consommation Résidentiel  (MWh)') / F.col('Nombre de points Résidentiel')).alias('consumption_by_residence'),
                F.col('Code Commune').alias('code_insee')
            )
            .where('consumption_by_residence IS NOT NULL')
            .groupBy('code_insee').agg(F.round(F.avg('consumption_by_residence'),2).alias('consumption_by_residence')),
        ['code_insee'],
        'left_outer'
    )
    .join(
        
    )
)
display(df_commune)

# COMMAND ----------

printit = (
)

display(printit)

# COMMAND ----------

# save as table
Dictionnary.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Dictionnary")
