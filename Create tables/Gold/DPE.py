# Databricks notebook source
# MAGIC %md
# MAGIC # DPE

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
dpe_2021 = spark.sql("SELECT * FROM Datalake.dpe_france_2021")
municipality = spark.sql("SELECT * FROM Gold.Municipality")

# COMMAND ----------


gaz_polluants = [
    "Fioul domestique",
    "GPL",
	"Propane",
    "Butane"
]
gaz = ["Gaz naturel"]
electricite = [
    "Électricité",
    "Électricité d'origine renouvelable utilisée dans le bâtiment"
]
bois_charbon = [
	"Bois – Plaquettes forestières",
	"Charbon",
	"Bois – Plaquettes d’industrie",
    "Bois – Bûches",
    "Bois – Granulés (pellets) ou briquettes"
]
autres = [
    "Réseau de Chauffage urbain",
    "Réseau de Froid Urbain"
]

energy_list = gaz_polluants + gaz + electricite + bois_charbon + autres

# COMMAND ----------


dpe = (
    dpe_2021.select(
        "N°DPE",
        "Date_établissement_DPE",
        "Année_construction",
        "Type_bâtiment",
        "Type_installation_chauffage",
        "Type_installation_chauffage_n°1",
        "Type_installation_ECS_(général)",
        "Surface_habitable_logement",
        "Code_postal_(BAN)",
        "Conso_5_usages_é_finale",
        "Emission_GES_5_usages",
        "Type_installation_ECS",
        "Type_énergie_principale_chauffage",
        "Type_émetteur_installation_chauffage_n°1",
        "Type_générateur_n°1_installation_n°1"
    )
    .filter(
        # not null values
        (F.col("Surface_habitable_logement").isNotNull()) &
        (F.col("Code_postal_(BAN)").isNotNull()) &
        (F.col("Code_INSEE_(BAN)").isNotNull()) &
        (F.col("Année_construction").isNotNull()) &
        (F.col("Conso_5_usages_é_finale").isNotNull()) &
        (F.col("Emission_GES_5_usages").isNotNull()) &
        (F.col("Type_installation_ECS").isNotNull()) &
        (F.col("Type_énergie_principale_chauffage").isNotNull()) &
        (F.col("Type_émetteur_installation_chauffage_n°1").isNotNull()) &
        (F.col("Type_générateur_n°1_installation_n°1").isNotNull()) &

        # not an irrelevant value
        (F.col("Type_énergie_principale_chauffage").isin(energy_list)) &
        (F.col("Type_installation_chauffage_n°1").cast("float").isNull()) &
        (F.col("Type_énergie_générateur_ECS_n°1").isin(energy_list)) &
        (F.col("Type_bâtiment") != "immeuble") &
        (F.col("Date_établissement_DPE") > F.lit("2014-01-01")) &
        (F.col("Date_établissement_DPE") < F.lit("2024-01-01")) &
        (F.col("Année_construction") < 2024) &
        (F.col("Année_construction") > 1700)
    )
    .withColumns({
        'id_dpe' : F.col('N°DPE'),
        'dpe_date' : (
            F.when(F.year(F.col('Date_établissement_DPE')) == 2023, 2022)
            .otherwise(F.year(F.col('Date_établissement_DPE')))
        ),
        'postal_code' : F.col('Code_postal_(BAN)'),
        'type' : (
            F.when(F.col('Type_bâtiment') == 'maison', 0)
            .otherwise(1)
        ),
        'construction_date' : (
            F.when(F.col('Année_construction') < 1949, 1)
            .when((F.col('Année_construction') >= 1949) & (F.col('Année_construction') < 1975), 2)
            .when((F.col('Année_construction') >= 1975) & (F.col('Année_construction') < 1982), 3)
            .when((F.col('Année_construction') >= 1982) & (F.col('Année_construction') < 1990), 4)
            .when((F.col('Année_construction') >= 1990) & (F.col('Année_construction') < 2001), 5)
            .when((F.col('Année_construction') >= 2001) & (F.col('Année_construction') < 2012), 6)
            .when(F.col('Année_construction') >= 2012, 7)
        ),
        'heating_system' : (
            F.when(F.col('Type_installation_chauffage') == 'individuel', 0)
            .when(F.col('Type_installation_chauffage') == 'collectif', 1)
            .otherwise(
                F.when(F.col('Type_installation_chauffage_n°1') == 'installation individuelle', 0)
                .otherwise(1)
            )
        ),
        'hot_water_system' : (
            F.when(F.col('Type_installation_ECS_(général)') == 'individuel', 0)
            .when(F.col('Type_installation_ECS_(général)') == 'collectif', 1)
            .otherwise(
                F.when(F.col('Type_installation_ECS') == 'installation individuelle', 0)
                .otherwise(1)
            )
        ),
        'heating_production' : (
            F.when(F.col('Type_énergie_principale_chauffage').isin(gaz), 1)
            .when(F.col('Type_énergie_principale_chauffage').isin(gaz_polluants), 2)
            .when(F.col('Type_énergie_principale_chauffage').isin(bois_charbon), 3)
            .when(F.col('Type_énergie_principale_chauffage').isin(autres), 4)
            .when(
                F.col('Type_énergie_principale_chauffage').isin(electricite),
                    F.when(
                        (
                            (F.col('Type_générateur_n°1_installation_n°1').contains('PAC')) |
                            (F.col('Type_générateur_n°1_installation_n°1').contains('pompe à chaleur'))
                        ),
                        5
                    )
                    .otherwise(6)
            )
        ),
        'heating_emission' : (
            F.when(
                (
                    F.col('Type_émetteur_installation_chauffage_n°1').contains('électrique') |
                    F.col('Type_émetteur_installation_chauffage_n°1').contains('rayonnant') |
                    F.col('Type_émetteur_installation_chauffage_n°1').contains('bi-jonction') |
                    F.col('Type_émetteur_installation_chauffage_n°1').contains('joule')
                ),
                6
            )
            .when(F.col('Type_émetteur_installation_chauffage_n°1').contains('gaz'), 1)
            .when(F.col('Type_émetteur_installation_chauffage_n°1').contains('Poêle'), 2)
            .when(
                (
                    F.col('Type_émetteur_installation_chauffage_n°1').contains('thermostatique') |
                    F.col('Type_émetteur_installation_chauffage_n°1').contains('chauffant sur réseau')
                ),
                3
            )
            .when(F.col('Type_émetteur_installation_chauffage_n°1').contains('Soufflage'), 5)
            .when(F.col('Type_émetteur_installation_chauffage_n°1').contains('Autres équipements'), 4)
        ),
        'surface' : F.col('Surface_habitable_logement'),
        'DPE_consumption' : F.col('Conso_5_usages_é_finale'),
        'GES_emission' : F.col('Emission_GES_5_usages'),
        'has_to_renov' : F.lit(None).cast('string')
    })
    .join(
        municipality.select(
            F.col('id_municipality'),
            F.col('postal_code')
        ),
        ['postal_code'],
        'inner'
    )
    .select(
        F.col('id_dpe'),
        F.col('dpe_date'),
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('heating_production'),
        F.col('heating_emission'),
        F.col('hot_water_system'),
        F.col('surface'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('has_to_renov')
    )
)

print(f'{dpe_2021.count() = }, {dpe.count() = }')
display(dpe)

# COMMAND ----------

dpe.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.DPE")
