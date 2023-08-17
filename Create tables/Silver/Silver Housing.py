# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Housing

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, Row
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
housing_intermediate = spark.sql("SELECT * FROM Intermediate.Housing")
owner_intermediate = spark.sql("SELECT * FROM Intermediate.Owner")
renovation = spark.sql("SELECT * FROM Intermediate.Renovation")
municipality = spark.sql("SELECT * FROM Silver.Municipality")
dpe = spark.sql("SELECT * FROM Silver.DPE")
owner = spark.sql("SELECT * FROM Silver.Owner")
municipality = spark.sql("SELECT * FROM Silver.Municipality")
municipality_info = spark.sql("SELECT * FROM Silver.Municipality_info")


# COMMAND ----------

housing = (
    housing_intermediate.join(
        renovation.select(
            F.col('id_owner'),
            F.col('start_date')
        )
        .groupby('id_owner')
        .agg(F.min('start_date').alias('first_date_renov')),
        ['id_owner'],
        'left_outer'
    )
    .join(
        owner_intermediate.filter( # select owners who responded to every question necessary
            (F.col('nb_persons_home') != 99) & 
            (F.col('nb_persons_home').isNotNull()) &
            (F.col('age').isNotNull()) &
            (~F.col('income').isin([10,11,99])) &
            (F.col('income').isNotNull())  
        )
        .select(F.col('id_owner')),
        ['id_owner'],
        'inner'
    )
    .filter(F.col('type').isin([1,2])) # house or appartment
    .join(
        municipality.select(
            F.col('id_municipality'),
            F.col('postal_code')
        ),
        ['postal_code'],
        'inner'
    )
    .withColumns({ # modify values according to documentation
        'type': (
            F.when(F.col('type') == 1, 0)
            .when(F.col('type') == 2, 1)
        ),
        'construction_date' : F.col('construction_date'),
        'heating_system' : (
            F.when(F.col('heating_system') == 1, 0)
            .when(F.col('heating_system') == 2, 1)
        ),
        'hot_water_system' : (
            F.when(F.col('hot_water_system') == 1, 0)
            .when(F.col('hot_water_system') == 2, 1)
        ),
        'heating_production' : (
            F.when(F.col('heating_production').isin([1, 2]), 6) # elec
            .when(F.col('heating_production').isin([3, 4, 5]), 5) # PAC
            .when(F.col('heating_production').isin([6, 7]), 3) # bois /charbon
            .when(F.col('heating_production').isin([8, 10]), 2) # fioul
            .when(F.col('heating_production').isin([9]), 1) # gaz
            .when(F.col('heating_production').isin([11]), 4) # autre
        ),
        'surface' : (
            F.when(F.col('surface') < 70, 1)
            .when((F.col('surface') >= 70) & (F.col('surface') < 115), 2)
            .when(F.col('surface') >= 115, 3)
        ),
        'first_date_renov' : (
            F.when(F.col('has_done_renov') == 1,
                F.array(
                    F.lit(2014),
                    F.lit(2015),
                    F.lit(2016),
                ).getItem(
                    (F.rand()*3).cast("int")
                )
            )
            .otherwise(
                F.col('first_date_renov') + 2013
            )
        ),
        'has_done_renov' : (
            F.when(F.col('has_done_renov') == 1, 0)
            .when(F.col('has_done_renov') == 2, 1)
        ),
        'DPE_consumption' : F.lit(None).cast('string'),
        'GES_emission' : F.lit(None).cast('string')
    })
    .select(
        F.col('id_owner'),
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('heating_production'),
        F.col('hot_water_system'),
        F.col('surface'),
        F.col('has_done_renov'),
        F.col('first_date_renov'),
        F.col('DPE_consumption'),
        F.col('GES_emission')
    )
)
display(housing)

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from xgboost.sklearn import XGBClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.ensemble import StackingClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
import numpy as np

# COMMAND ----------

param_dpe_RF = {'class_weight': 'balanced', 'max_depth': 39, 'n_estimators': 112}
param_ges_RF = {'class_weight': 'balanced', 'max_depth': 27, 'n_estimators': 134}
param_surf_histgb = {'l2_regularization': 0.17592525267734538, 'learning_rate': 0.03542260908465626, 'max_iter': 241}
param_sub_RF = {'class_weight': 'balanced', 'max_depth': 24, 'n_estimators': 180}
param_over_RF = {'class_weight': 'balanced', 'max_depth': 24, 'n_estimators': 173}
param_stacked_xgb = {'learning_rate': 0.1, 'n_estimators': 50}


# COMMAND ----------

dpe_RF = RandomForestClassifier(**param_dpe_RF)
ges_RF = RandomForestClassifier(**param_ges_RF)
surf_histgb = HistGradientBoostingClassifier(**param_surf_histgb)


# COMMAND ----------

training_dpe_ges = (
    dpe.select(
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('GES_emission'),
        F.col('DPE_consumption')
    )
)
training_surf = spark.sql("SELECT * FROM Model.training_surf")
training_prod = spark.sql("SELECT * FROM Model.training_prod")
training_dpe = training_dpe_ges.drop('GES_emission')
training_ges = training_dpe_ges.drop('DPE_consumption')

# COMMAND ----------

def prepare_target_features(df, col_hot, col_not_hot, target):
    X_hot = np.array(df.select(col_hot).collect())
    encoder = OneHotEncoder(drop="first", sparse=False).fit(X_hot)
    X_hot = encoder.transform(X_hot)
    X_not_hot = np.array(df.select(col_not_hot).collect())
    X = np.column_stack((X_not_hot, X_hot))
    
    y = df.select(target)
    if not 0 in np.array(y.dropDuplicates().collect()):
        y = y.withColumn(target, F.col(target) - 1)
    y = np.array(y.collect()).ravel()
    return X, y, encoder

# COMMAND ----------

# prefit estimators
target = "heating_production"
col_hot = ['occupation', 'department_number']
col_not_hot = [col[0] for col in training_prod.dtypes if col[0] not in col_hot + [target]]

training_prod_subclass = (
    training_prod.filter(
        F.col(target).isin([2, 3, 4, 5])
    )
    .withColumn(target, F.col(target) - 2)
)

training_prod_overclass = (
    training_prod.filter(
        F.col(target).isin([1, 6])
    )
    .withColumn(target, F.when(F.col(target) == 1, 0).otherwise(1))
)
X_sub, y_sub, _ = prepare_target_features(training_prod, col_hot, col_not_hot, target)
RF_sub = RandomForestClassifier(**param_sub_RF).fit(X_sub, y_sub)
X_over, y_over, _ = prepare_target_features(training_prod, col_hot, col_not_hot, target)
RF_over = RandomForestClassifier(**param_over_RF).fit(X_over, y_over)

estimators = [
    ("rf1", RF_sub),
    ("rf2", RF_over),
]
stack = StackingClassifier(estimators, XGBClassifier(**param_stacked_xgb), cv='prefit')

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, r2_score, confusion_matrix, f1_score
import seaborn as sn

# COMMAND ----------

trainings = [training_prod, training_surf, training_dpe, training_ges]
targets = ['heating_production', 'surface', 'DPE_consumption', 'GES_emission']
col_hots = [['occupation', 'department_number'],['occupation', 'department_number'], ['heating_production'], ['heating_production']]
col_not_hots = [
    [col[0] for col in training.dtypes if col[0] not in col_hot + [target]]
    for training, col_hot, target in zip(trainings, col_hots, targets)
]
models = [stack, surf_histgb ,dpe_RF, ges_RF]
f, axs = plt.subplots(1, 4, figsize=(20,5))
encoders = {}

for model, training, target, col_hot, col_not_hot, ax in zip (models, trainings, targets, col_hots, col_not_hots, axs.flatten()):
    X, y, encoder = prepare_target_features(training, col_hot, col_not_hot, target)
    model.fit(X, y)
    y_pred = model.predict(X)
    encoders[target] = encoder
    matrix = confusion_matrix(y, y_pred)
    sn.heatmap(
        (matrix.T / np.sum(matrix, axis=1).T).T,
        ax=ax,
        annot=True,
        fmt=".1%",
    )
    ax.set_ylabel("true")
    ax.set_xlabel("pred")
    ax.set_title(f'{target}\nscore : {round(f1_score(y, y_pred, average="micro"),4)}')



# COMMAND ----------

# MAGIC %md
# MAGIC # pred_tremi

# COMMAND ----------

training_tremi = (
    owner.join(
        housing.select(
            F.col('id_owner'),
            F.col('id_municipality').alias('housing_id_municipality'),
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
        [F.col('housing_id_municipality') == F.col('municipality.id_municipality')],
        'inner'
    )
    .join(
        municipality_info,
        [F.col('first_date_renov') == F.col('year'), F.col('housing_id_municipality') == F.col('municipality_info.id_municipality')],
        'inner'
    )
    .select(
        F.col('id_owner'),
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

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

predicting_surf_prod = np.array(
    training_tremi.select(
        F.col('id_owner'),
        F.col('occupation'),
        F.col('department_number'),
        F.col('gender'),
        F.col('age'),
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
        F.col('n_destruction_licence')
    ).collect()
)

X_hot = encoders['surface'].transform(predicting_surf_prod[:,1:3])
X_not_hot = np.array(predicting_surf_prod[:,3:])
X = np.column_stack((X_not_hot, X_hot))
print(X.shape)
y_pred_surf = models[1].predict(X) + 1
y_pred_prod = models[0].predict(X) + 1

# COMMAND ----------



data = [(id_owner, surf, prod) for id_owner, surf, prod in zip(predicting_surf_prod[:,0].tolist(), y_pred_surf.tolist(), y_pred_prod.tolist())]
schema = StructType([ 
    StructField("id_owner",IntegerType(),True),
    StructField("pred_surface",IntegerType(),True),
    StructField("pred_prod",IntegerType(),True),
  ])
rowData = map(lambda x: Row(*x), data) 
pred_surf_prod = spark.createDataFrame(data=rowData, schema=schema)
display(pred_surf_prod)

# COMMAND ----------


completed = (
    training_tremi.join(
        pred_surf_prod,
        ['id_owner'],
        'inner'
    )  
    .withColumns({
        'surface': (
            F.when(
                F.col('surface').isNull(), 
                F.col('pred_surface')
            )
            .otherwise(F.col('surface'))
        ),
        'heating_production': (
            F.when(
                F.col('heating_production').isNull(), 
                F.col('pred_prod')
            )
            .otherwise(F.col('heating_production'))
        )
    })
    .drop(
        F.col('pred_surface'),
        F.col('pred_prod') 
    )
)
print(completed.count())
display(completed)

# COMMAND ----------

housing_final = (
    housing.join(
        completed.select(
            F.col('id_owner'),
            F.col('surface').alias('predicted_surf'),
            F.col('heating_production').alias('predicted_heating_prod')
        ),
        ['id_owner'],
        'inner'
    )
    .select(
        F.col('id_owner'),
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('predicted_heating_prod').alias('heating_production'),
        F.col('hot_water_system'),
        F.col('predicted_surf').alias('surface'),
        F.col('has_done_renov'),
        F.col('first_date_renov'),
        F.col('DPE_consumption'),
        F.col('GES_emission')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # pred_DPE

# COMMAND ----------

predicting_dpe_ges = np.array(
    housing_final.select(
        F.col('id_owner'),
        F.col('heating_production'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('hot_water_system')  
    ).collect()
)

X_hot = encoders['DPE_consumption'].transform(predicting_dpe_ges[:,1].reshape(-1, 1))
X_not_hot = predicting_dpe_ges[:,2:]
X = np.column_stack((X_not_hot, X_hot))
print(X.shape)
y_pred_dpe = models[2].predict(X)
y_pred_ges = models[3].predict(X)
print(y_pred_dpe.shape, y_pred_ges.shape)


# COMMAND ----------

data = [(id_owner, int(dpe), int(ges)) for id_owner, dpe, ges in zip(predicting_dpe_ges[:,0].tolist(), y_pred_dpe.tolist(), y_pred_ges.tolist())]
schema = StructType([ 
    StructField("id_owner",IntegerType(),True),
    StructField("pred_dpe",IntegerType(),True),
    StructField("pred_ges",IntegerType(),True),
  ])
rowData = map(lambda x: Row(*x), data) 
pred_dpe_ges = spark.createDataFrame(data=rowData, schema=schema)
display(pred_dpe_ges)

# COMMAND ----------

housing_completed = (
    housing_final.join(
        pred_dpe_ges,
        ['id_owner'],
        'inner'
    )
    .withColumns({
        'DPE_consumption': F.col('pred_dpe'),
        'GES_emission': F.col('pred_ges'),
    })
    .drop(
        F.col('pred_dpe'),
        F.col('pred_ges')
    )
    .dropDuplicates()
)
print(housing_completed.count())
display(housing_completed)

# COMMAND ----------

housing_completed.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Silver.Housing")
