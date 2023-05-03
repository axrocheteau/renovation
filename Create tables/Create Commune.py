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

regions = (
    development_licence.select(
        F.col('REG').alias('former_region_number'),
        F.col('DEP').alias('department_number')
    )
    .dropDuplicates()
    .join(
        pop_department.select(
                F.col('CODDEP').alias('department_number'),
                F.col('DEP').alias('department_name')
            ),
        ['department_number'],
        'inner'
    )
    .join(
        former_new_region.select(
                F.col('Nouveau Code').alias('new_region_number'),
                F.col('Nouveau Nom').alias('new_region_name'),
                F.col('Anciens Code').alias('former_region_number'),
                F.col('Anciens Nom').alias('former_region_name')
            ),
        ['former_region_number'],
        'inner'
    )
    .dropDuplicates()
)

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
                F.col('code_insee_commune_actualise').alias('code_insee')
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
        code_commune.select(
            F.col('Code_commune_INSEE').alias('code_insee'),
            F.when(F.col('Code_postal') < 10000, F.concat(F.lit("0"), F.col('Code_postal').cast('string')))
                .otherwise(F.col('Code_postal').cast('string'))
                .alias('cd_postal')
            ),
        ['code_insee'],
        'left_outer'
    )
    .join(
        construction_licence.filter(
                F.col('DATE_REELLE_AUTORISATION').between(F.lit("2014-01-01"), F.lit("2017-01-01"))
            )
            .select(
                F.col('NB_LGT_TOT_CREES').alias('nb_housing'),
                F.col('COMM').alias('code_insee')
            )
            .groupBy('code_insee').agg(F.sum('nb_housing').alias('n_construction_licence')),
        ['code_insee'],
        'left_outer'
    )
    .join(
        destruction_licence.filter(
                F.col('DATE_REELLE_AUTORISATION').between(F.lit("2014-01-01"), F.lit("2017-01-01"))
            )
            .select(
                F.col('COMM').alias('code_insee')
            )
            .groupBy('code_insee').agg(F.count('code_insee').alias('n_destruction_licence')),
        ['code_insee'],
        'left_outer'
    )
    .join(
        development_licence.filter(
                F.col('DATE_REELLE_AUTORISATION').between(F.lit("2014-01-01"), F.lit("2017-01-01"))
            )
            .select(
                F.col('COMM').alias('code_insee')
            )
            .groupBy('code_insee').agg(F.count('code_insee').alias('n_development_licence')),
        ['code_insee'],
        'left_outer'
    )
    .join(
        regions,
        ['department_number'],
        'inner'
    )
    .withColumns({
        'n_development_licence': F.when(F.col('n_development_licence').isNull(), 0).otherwise(F.col('n_development_licence')),
        'n_destruction_licence': F.when(F.col('n_destruction_licence').isNull(), 0).otherwise(F.col('n_destruction_licence')),
        'n_construction_licence': F.when(F.col('n_construction_licence').isNull(), 0).otherwise(F.col('n_construction_licence')),
        'n_dpe': F.when(F.col('n_dpe').isNull(), 0).otherwise(F.col('n_dpe')),
        "id_commune": F.monotonically_increasing_id()
    })
)

# COMMAND ----------

# reorder columns
df_commune = df_commune.select(
    'id_commune',
    'cd_postal',
    'code_insee',
    'commune_name',
    'department_number',
    'department_name',
    'former_region_name',
    'former_region_number',
    'new_region_name',
    'new_region_number',
    'population',
    'n_development_licence',
    'n_construction_licence',
    'n_destruction_licence',
    'n_dpe',
    'avg_dpe',
    'avg_ges',
    'consumption_by_residence'
)
display(df_commune)

# COMMAND ----------

# save as table
df_commune.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Commune")
