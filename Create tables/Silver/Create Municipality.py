# Databricks notebook source
# MAGIC %md
# MAGIC # Create Commune

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
df = spark.sql("SELECT * FROM datalake.pop_commune_2016")

# COMMAND ----------

pop_commune = spark.sql("SELECT * FROM datalake.pop_commune_2016")
former_new_region = spark.sql("SELECT * FROM datalake.former_new_region")
dpe_france = spark.sql("SELECT * FROM datalake.dpe_france_2012")
elec = spark.sql("SELECT * FROM datalake.elec")
construction_licence = spark.sql("SELECT * FROM datalake.construction_licence_2016")
destruction_licence = spark.sql("SELECT * FROM datalake.destruction_licence")
development_licence = spark.sql("SELECT * FROM datalake.development_licence")
code_commune = spark.sql("SELECT * FROM datalake.code_commune")
pop_department = spark.sql("SELECT * FROM datalake.pop_department_2016")

# COMMAND ----------

# table all regions and dep (to not do nested join)
regions = (
    development_licence.select( #table that link region and dep number
        F.col('REG').alias('former_region_number'),
        F.col('DEP').alias('department_number')
    )
    .dropDuplicates() # to reduce number of value drastically
    .join(
        pop_department.select( # to get department name
                F.col('CODDEP').alias('department_number'),
                F.col('DEP').alias('department_name')
            ),
        ['department_number'],
        'inner'
    )
    .join(
        former_new_region.select( # to get all region info
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

df_municipality = (
    pop_commune.select( # populations info
        F.col('DEPCOM').alias('insee_code'),
        F.col('COM').alias('municipality_name'),
        F.col('PMUN').alias('population'),
        F.col('DEPCOM').substr(0,2).alias('department_number')
    )
    .join( # dpe and ges info
        dpe_france.filter( # only take dpe between 2014 and 2016, ges and dpe that are relevant  
                (F.col('date_etablissement_dpe').between(F.lit("2014-01-01"), F.lit("2017-01-01"))) &
                (F.col('classe_consommation_energie') != 'N') &
                (F.col('classe_estimation_ges') != 'N') # N is not a valid dpe or ges
            )
            .select( # put ges and dpe as float to average them
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
                F.col('code_insee_commune_actualise').alias('insee_code')
            )
            .groupBy('insee_code').agg(F.avg('dpe'), F.avg('ges'), F.count('insee_code'))
            .select(
                F.col('insee_code'),
                F.round(F.col('avg(dpe)'),2).alias('avg_dpe'),
                F.round(F.col('avg(ges)'),2).alias('avg_ges'),
                F.col('count(insee_code)').alias('n_dpe')
            ),
        ['insee_code'],
        'left_outer'
    )
    .join( # elec consumption info
        elec.filter( # only take electricity conumption between 2014 and 2016
                (F.col('Année').between(2014, 2016)) &
                (F.col('Filière') == 'Electricité')
            )
            .select(
                (F.col('Consommation Résidentiel  (MWh)') / F.col('Nombre de points Résidentiel')).alias('consumption_by_residence'),
                F.col('Code Commune').alias('insee_code')
            )
            .where('consumption_by_residence IS NOT NULL')
            .groupBy('insee_code').agg(F.round(F.avg('consumption_by_residence'),2).alias('consumption_by_residence')),
        ['insee_code'],
        'left_outer'
    )
    .join( # get postal codes
        code_commune.select(
            F.col('Code_commune_INSEE').alias('insee_code'),
            # correct postal codes interpreted as int
            F.when(F.col('Code_postal') < 10000, F.concat(F.lit("0"), F.col('Code_postal').cast('string')))
                .otherwise(F.col('Code_postal').cast('string'))
                .alias('postal_code')
            ),
        ['insee_code'],
        'left_outer'
    )
    .join( # get construction licence
        construction_licence.filter(
                F.col('DATE_REELLE_AUTORISATION').between(F.lit("2014-01-01"), F.lit("2017-01-01"))
            )
            .select(
                F.col('NB_LGT_TOT_CREES').alias('nb_housing'),
                F.col('COMM').alias('insee_code')
            )
            .groupBy('insee_code').agg(F.sum('nb_housing').alias('n_construction_licence')),
        ['insee_code'],
        'left_outer'
    )
    .join( # get destruction licence
        destruction_licence.filter(
                F.col('DATE_REELLE_AUTORISATION').between(F.lit("2014-01-01"), F.lit("2017-01-01"))
            )
            .select(
                F.col('COMM').alias('insee_code')
            )
            .groupBy('insee_code').agg(F.count('insee_code').alias('n_destruction_licence')),
        ['insee_code'],
        'left_outer'
    )
    .join( # get development licence
        development_licence.filter(
                F.col('DATE_REELLE_AUTORISATION').between(F.lit("2014-01-01"), F.lit("2017-01-01"))
            )
            .select(
                F.col('COMM').alias('insee_code')
            )
            .groupBy('insee_code').agg(F.count('insee_code').alias('n_development_licence')),
        ['insee_code'],
        'left_outer'
    )
    .join( # get regions info and dep name
        regions,
        ['department_number'],
        'inner'
    )
    .withColumns({ # put null values to 0 to make sense
        'n_development_licence': F.when(F.col('n_development_licence').isNull(), 0).otherwise(F.col('n_development_licence')),
        'n_destruction_licence': F.when(F.col('n_destruction_licence').isNull(), 0).otherwise(F.col('n_destruction_licence')),
        'n_construction_licence': F.when(F.col('n_construction_licence').isNull(), 0).otherwise(F.col('n_construction_licence')),
        'n_dpe': F.when(F.col('n_dpe').isNull(), 0).otherwise(F.col('n_dpe')),
        # add an id for every town
        "id_municipality": F.monotonically_increasing_id()
    })
)

# COMMAND ----------

# reorder columns
df_municipality = df_municipality.select(
    'id_municipality',
    'postal_code',
    'insee_code',
    'municipality_name',
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
display(df_municipality)

# COMMAND ----------

# save as table
df_municipality.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Silver.Municipality")
