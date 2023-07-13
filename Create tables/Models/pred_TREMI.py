# Databricks notebook source
# MAGIC %md
# MAGIC # pred Tremi
# MAGIC prdiction of surface, heating production and heating_emission variables for every owner in Tremi dataset using owner variables

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
owner = spark.sql("SELECT * FROM Gold.Owner")
housing = spark.sql("SELECT * FROM Gold.Housing")
municipality = spark.sql("SELECT * FROM Gold.Municipality")
municipality_info = spark.sql("SELECT * FROM Gold.Municipality_info")

# COMMAND ----------

training_tremi = (
    owner.join(
        housing.select(
            F.col('id_owner'),
            F.col('id_municipality'),
            F.col('first_date_renov'),
            F.col('surface'),
            F.col('heating_production'),
            F.col('type'),
            F.col('construction_date'),
            F.col('heating_system')
        ),
        ['id_owner'],
        'inner'
    )
    .join(
        municipality.select(
            F.col('id_municipality'),
            F.col('department_number')
        ),
        ['id_municipality'],
        'inner'
    )
    .join(
        municipality_info,
        [F.col('first_date_renov') == F.col('year'), F.col('housing.id_municipality') == F.col('municipality_info.id_municipality')],
        'inner'
    )
    .select(
        F.col('gender'),
        F.col('age'),
        F.col('occupation'),
        F.col('home_state'),
        F.col('nb_persons_home'),
        F.col('income'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('population'),
        F.col('n_development_licence'),
        F.col('n_construction_licence'),
        F.col('n_new_buildings'),
        F.col('n_destruction_licence'),
        F.col('department_number'),
        F.col('surface'),
        F.col('heating_production'),
    )
)
print(training_tremi.count())
display(training_tremi)

# COMMAND ----------

training_surf = training_tremi.filter(F.col('surface').isNotNull()).drop('heating_production')
predicting_surf = training_tremi.filter(F.col('surface').isNull()).drop('heating_production')

training_prod = training_tremi.filter(F.col('heating_production').isNotNull()).drop('surface')
predicting_prod = training_tremi.filter(F.col('heating_production').isNull()).drop('surface')


# COMMAND ----------

# librairies
import numpy as np
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler

# random forest
from sklearn.ensemble import RandomForestClassifier

# HistGboost
from sklearn.ensemble import HistGradientBoostingClassifier

# COMMAND ----------

def preprocess(df):
    np_df = df.toPandas().to_numpy()
    X = np_df[:,:-1]
    X = StandardScaler().fit_transform(X)
    y = np_df[:,-1].ravel().astype(int)
    if 0 not in np.unique(y):
        y = y - 1
    return (X, y)

X, y = preprocess(training_surf)
print(X.shape, y.shape)

# COMMAND ----------

# param_surf = {'class_weight': 'balanced', 'max_depth': 15, 'n_estimators': 171}
# param_prod = {'class_weight': 'balanced', 'max_depth': 19, 'n_estimators': 171}
# param_em = {'class_weight': 'balanced', 'max_depth': 16, 'n_estimators': 159}

# model_surf = RandomForestClassifier(**param_surf).fit(*preprocess(training_surf))
# model_prod = RandomForestClassifier(**param_prod).fit(*preprocess(training_prod))
# model_em = RandomForestClassifier(**param_em).fit(*preprocess(training_em))


# COMMAND ----------

trainings = [
    {'dataset' : training_surf, 'name' : 'training_surf'},
    {'dataset' : training_prod, 'name' : 'training_prod'}
]

for training in trainings:
    training['dataset'].write.mode('overwrite')\
            .format("parquet")\
            .saveAsTable(f"Model.{training['name']}")
