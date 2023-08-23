# Renovation projet

## Contexte
Ce projet a été réalisé par Axel Rocheteau, stagiaire dans le pôle data de ASI du 6 mars 2023 au 30 août 2023 sous la supervision de ces deux tuteurs : Morgane Maquignon et Frederic André. Ce projet a pour but de montrer le nombre de rénovation possibles en France selon certains critères. Il permet à des entreprises spécialisées dans les rénovations énergétiques d’identifier des clients potentiels.

## Environnement
Tous les notebooks réalisés ont été utilisé via Databricks et sa connectivité à Git. Les notebooks utilisent la technologie Pyspark. Ainsi, les notebook necessiteront des changements si ils sont lancés localement et non depuis l'espace Databricks

## Données
La classification des logements s'appuient sur des données open sources. Dans le but de rendre ce projet entièrement sur le cloud, Des notebooks dédiés au téléchargement des données ont été implémenté. 
Les notebooks dpe_2012 et dpe_2021 s'appuient sur les données de l'adème disponible à cette [adresse](https://data.ademe.fr/datasets?topics=BR8GjsXga). Le téléchargement de ses fichiers passent par de multiple requête renvoyant 10 000 lignes chacune. Il est difficile de comprendre l'organisation de ces requête et leur point d'arrêt. De ce fait, les notebook cités précédemment sont long et non optimisés. Il est conseillé de les téléchargés à la main et de les insérer dans databricks via le DBFS.
Les données centrales de ce projet sont l'enquête Tremi présentant les rénovations des ménages français réalisées entre 2014 et 2016 et la base de données des DPE après 2021 recensant tous les logements ayant effectuées une démarche DPE depuis juillet 2021.

## Organisation
Vous voyez ici tous les notebooks servant à la création des bases de données réalisé lors de ce projet. Les notebooks sont organisés dans deux dossiers :
- `Create tables` recueille les notebooks créant des tables modifiants les données brut. Vous retrouverez dans ce dossier toutes les bases de données créées durant ce projet.
- `Import files` Ce dossier contient le notebooks téléchargeant les fichiers csv et la création du datalake ou base de données bronze stockant les fichiers sans modifications

Durant ce projet 4 bases de données principales ont été créées:
- `Bronze` ou `Datalake` recence les fichiers csv stockées sous forme de table sans modification des informations initiales
- `Intermediate` est une base de données centrée autour de l'enquête TREMI. Elle présente les divers aspects de cette enquête à travers 7 tables (une par notebook).
- Dans la base de données `Silver`, les données sont nettoyés et les données manquantes sont inputées grâce à des modèles de Machine Learning. La base de données met en relation les logements de l'enquête TREMI et les logement ayant effectuées un DPE pour étendre le nombre de données
- `Gold` est la base de données finale. Elle recueille uniquement les données nécessaires au Dashboard créé. 

