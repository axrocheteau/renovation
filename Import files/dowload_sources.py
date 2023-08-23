# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
import zipfile
import io
import os
import requests

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

link_csv_array = [
    {'url' : 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/donnees-synop-essentielles-omm/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B&select=date%2C%20dd%2C%20ff%2C%20t%2C%20u%2C%20hbas%2C%20tc%2C%20altitude%2C%20code_dep%2C%20mois_de_l_annee', 'name': 'weather', "delimiter": ";"},
    {'url' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'url' : 'https://www.data.gouv.fr/fr/datasets/r/14ff8e51-5a4c-4e22-bd83-a564cfa2c7d5', 'name': 'development_licence', "delimiter": ";"},
    {'url' : 'https://www.data.gouv.fr/fr/datasets/r/ddeaadfd-bb89-4db8-b438-a6235582a45f', 'name': 'tremi', "delimiter": ";"},
    {'url' : 'https://www.data.gouv.fr/fr/datasets/r/5ed9b092-a25d-49e7-bdae-0152797c7577', 'name': 'code_commune', "delimiter": ";"},
    {'url' : 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/anciennes-nouvelles-regions/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B', 'name': 'former_new_region', "delimiter": ";"},
    {'url' : 'https://opendata.agenceore.fr/api/explore/v2.1/catalog/datasets/conso-elec-gaz-annuelle-par-secteur-dactivite-agregee-commune/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B', 'name': 'elec', "delimiter": ";"},
    
]
link_zip_array = [
    {'url' : 'https://www.insee.fr/fr/statistiques/fichier/4265429/ensemble.zip', 'extract': ['Departements.csv', 'Communes.csv'], "name": ['pop_commune_2016.csv', 'pop_dep_2016.csv']},
    {'url' : 'https://www.insee.fr/fr/statistiques/fichier/6683035/ensemble.zip', 'extract': ['donnees_communes.csv'], "name": ['pop_commune_2020.csv']},
    {'url' : 'https://www.data.gouv.fr/fr/datasets/r/67dd4ee1-0d73-4676-a90f-854fe9012f5d', 'extract': ['PC_DP_creant_logements_2013_2016.csv'], "name": ['construction_licence_2016.csv']},
    {'url' : 'https://www.data.gouv.fr/fr/datasets/r/1fa467ef-5e3a-456f-b961-be9032cfa3df', 'extract': ['PC_DP_creant_logements_2017_2023.csv'], "name": ['construction_licence_2023.csv']},
]



# COMMAND ----------

# write csv files in dbfs system

for file in link_csv_array:
    delimiter = file["delimiter"]
    name = file['name']
    print(f'requesting {name} ...')
    response = requests.get(file['url'])
    if response.status_code != 200:
        raise Exception(f" unable to get a response from {file['url']} at file : {file['name']}")
    
    print('request successfull')
    dbutils.fs.put(f"/FileStore/tables/{name}.csv", response.text, True)
    
        
    

# COMMAND ----------

# write csv files contained in zip in dbfs system

zip_path = "/FileStore/tables/zip"
store_path = "/FileStore/tables"
for file in link_zip_array:
    r = requests.get(file['url'])
    if r.status_code != 200:
        raise Exception(f" unable to get a response from {file['url']} at file : {file['name']}")
    
    z = zipfile.ZipFile(io.BytesIO(r.content))
    dbutils.fs.mkdirs(zip_path)
    z.extractall(f"/dbfs{zip_path}")

    for extract, name in zip(file['extract'], file['name']):
        dbutils.fs.cp(f"{zip_path}/{extract}", f"{store_path}/{name}")
    
    dbutils.fs.rm("/FileStore/tables/zip", True)
