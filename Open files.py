# Databricks notebook source


# File location and type
file_location_array = ["/FileStore/tables/Communes.csv",
                        "/FileStore/tables/Departements.csv",
                        "/FileStore/tables/PC_DP_creant_logements_2013_2016.csv",
                        "/FileStore/tables/Regions.csv",
                        "/FileStore/tables/TREMI_2017_CodeBook_public8.txt",
                        "/FileStore/tables/TREMI_2017_Résultats_enquête_bruts.csv",
                        "/FileStore/tables/anciennes_nouvelles_regions.csv",
                        "/FileStore/tables/code_commune.csv",
                        "/FileStore/tables/conso_elec_gaz_annuelle_par_secteur_dactivite_agregee_commune__1_.csv",
                        "/FileStore/tables/donnees_synop_essentielles_omm.csv",
                        "/FileStore/tables/dpe_france.csv",
                        "/FileStore/tables/permis_amenager.csv",
                        "/FileStore/tables/permis_demolir.csv"
]
file_type = "csv"
name_array = ["pop_commune",
                "pop_department",
                "construction_licence",
                "pop_region",
                "codebook",
                "tremi",
                "former_new_region",
                "code_commune",
                "elec",
                "weather",
                "dpe_france",
                "development_licence",
                "destruction_licence"
]

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter_array = [";", ";", ";", ";", "\t", ";", ";", ";", ";", ";", ",", ";", ";"]

# The applied options are for CSV files. For other file types, these will be ignored.
for file_location, delimiter, name in zip(file_location_array, delimiter_array, name_array):
    df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location)
    if name == "weather":
        new_column_name_list= [name.replace(',','') for name in df.columns]
        df = df.toDF(*new_column_name_list)
    df.write.format("parquet").saveAsTable(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS pop_commune;
# MAGIC DROP TABLE IF EXISTS pop_department;
# MAGIC DROP TABLE IF EXISTS construction_licence;
# MAGIC DROP TABLE IF EXISTS pop_region;
# MAGIC DROP TABLE IF EXISTS codebook;
# MAGIC DROP TABLE IF EXISTS tremi;
# MAGIC DROP TABLE IF EXISTS former_new_region;
# MAGIC DROP TABLE IF EXISTS code_commune;
# MAGIC DROP TABLE IF EXISTS elec;
# MAGIC DROP TABLE IF EXISTS weather;
# MAGIC DROP TABLE IF EXISTS dpe_france;
# MAGIC DROP TABLE IF EXISTS development_licence;
# MAGIC DROP TABLE IF EXISTS destruction_licence;

# COMMAND ----------

file_location = "/FileStore/tables/dpe_france.csv"
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

df = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location)
display(df)



# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/dpe_france/', True)
