# Databricks notebook source
link_array = [
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/14ff8e51-5a4c-4e22-bd83-a564cfa2c7d5', 'name': 'development_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/67dd4ee1-0d73-4676-a90f-854fe9012f5d', 'name': 'construction_licence_2016', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/1fa467ef-5e3a-456f-b961-be9032cfa3df', 'name': 'construction_licence_2023', "delimiter": ";"},
    {'link' : 'https://www.insee.fr/fr/statistiques/fichier/4265429/ensemble.zip', 'name': 'zip_pop_commune_dep_2016', "delimiter": ";"},
    {'link' : 'https://www.insee.fr/fr/statistiques/fichier/6683035/ensemble.zip', 'name': 'zip_pop_commune_2020', "delimiter": ";"},
    {'link' : 'https://data.ademe.fr/data-fair/api/v1/datasets/tremi-2017-resultats-bruts/metadata-attachments/TREMI_2017_CodeBook_public.xlsx', 'name': 'codebook', "delimiter": "?"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/ddeaadfd-bb89-4db8-b438-a6235582a45f', 'name': 'tremi', "delimiter": ";"},
    {'link' : 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/anciennes-nouvelles-regions/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B', 'name': 'former_new_region', "delimiter": ";"},
    {'link' : 'https://opendata.agenceore.fr/api/explore/v2.1/catalog/datasets/conso-elec-gaz-annuelle-par-secteur-dactivite-agregee-commune/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B', 'name': 'elec', "delimiter": ";"},
    {'link' : 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/donnees-synop-essentielles-omm/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B', 'name': 'weather', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},
    {'link' : 'https://www.data.gouv.fr/fr/datasets/r/d1c11212-6c4f-4df2-a0ca-a13dfa40ba4b', 'name': 'destruction_licence', "delimiter": ";"},

]

# COMMAND ----------

info_array = [
    {"location" : "/FileStore/tables/pop_commune_2016.csv", "name": "pop_commune_2016", "delimiter": ";"},
    {"location" : "/FileStore/tables/pop_commune_2020.csv", "name": "pop_commune_2020", "delimiter": ";"},
    {"location" : "/FileStore/tables/pop_dep_2016.csv", "name": "pop_department_2016", "delimiter": ";"},
    {"location" : "/FileStore/tables/permis_construire_2013_2016.csv", "name": "construction_licence_2016", "delimiter": ";"},
    {"location" : "/FileStore/tables/permis_construire_2017_2023.csv", "name": "construction_licence_2023", "delimiter": ";"},
    {"location" : "/FileStore/tables/TREMI_2017_CodeBook_public8.txt", "name": "codebook", "delimiter": "\t"},
    {"location" : "/FileStore/tables/TREMI_2017_Résultats_enquête_bruts.csv", "name": "tremi", "delimiter": ";"},
    {"location" : "/FileStore/tables/anciennes_nouvelles_regions.csv", "name": "former_new_region", "delimiter": ";"},
    {"location" : "/FileStore/tables/code_commune.csv", "name": "code_commune", "delimiter": ";"},
    {"location" : "/FileStore/tables/conso_elec.csv", "name": "elec", "delimiter": ";"},
    {"location" : "/FileStore/tables/meteo.csv", "name": "weather", "delimiter": ";"},
    {"location" : "/FileStore/tables/dpe_france_2012.csv", "name": "dpe_france_2012", "delimiter": ","},
    {"location" : "/FileStore/tables/dpe_france_2021", "name": "dpe_france_2021", "delimiter": ","},
    {"location" : "/FileStore/tables/permis_amenager.csv", "name": "development_licence", "delimiter": ";"},
    {"location" : "/FileStore/tables/permis_demolir.csv", "name": "destruction_licence", "delimiter": ";"},
    {"location" : "/FileStore/tables/dep_limitrophe.csv", "name": "neighbouring_dep", "delimiter": ";"},
    {"location" : "/FileStore/tables/logements.csv", "name": "housings", "delimiter": ";"}
]   
