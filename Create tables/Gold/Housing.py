# Databricks notebook source
# MAGIC %md
# MAGIC # Housing

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
housing_silver = spark.sql("SELECT * FROM Silver.Housing")
owner_silver = spark.sql("SELECT * FROM Silver.Owner")
renovation = spark.sql("SELECT * FROM Silver.Renovation")
municipality = spark.sql("SELECT * FROM Gold.Municipality")


# COMMAND ----------

housing = (
    housing_silver.join(
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
        owner_silver.filter( # select owners who responded to every question necessary
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
            F.when(F.col('has_done_renov') == 1, 2014)
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

dpe = spark.sql("SELECT * FROM Gold.DPE")
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

owner = spark.sql("SELECT * FROM Gold.Owner")
municipality = spark.sql("SELECT * FROM Gold.Municipality")
municipality_info = spark.sql("SELECT * FROM Gold.Municipality_info")

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

from pyspark.sql.types import IntegerType, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [(id_owner, surf, prod) for id_owner, surf, prod in zip(predicting_surf_prod[:,0].tolist(), y_pred_surf.tolist(), y_pred_prod.tolist())]
schema = StructType([ 
    StructField("id_owner",IntegerType(),True),
    StructField("pred_surface",IntegerType(),True),
    StructField("pred_prod",IntegerType(),True),
  ])
rowData = map(lambda x: Row(*x), data) 
dfFromData3 = spark.createDataFrame(data=rowData, schema=schema)
display(dfFromData3)

# COMMAND ----------

def predict_surface(occupation, department, *features):
    X_hot = encoders['surface'].transform(np.array([[occupation, department]]))
    X_not_hot = np.array([features])
    X = np.column_stack((X_not_hot, X_hot))
    return int(models[1].predict(X)[0]) + 1

udf_surf = F.udf(lambda occupation, department, *features : predict_surface(occupation, department, *features), IntegerType())

def predict_prod(occupation, department, *features):
    X_hot = encoders['heating_production'].transform(np.array([[occupation, department]]))
    X_not_hot = np.array([features])
    X = np.column_stack((X_not_hot, X_hot))
    return int(models[0].predict(X)[0]) + 1

udf_prod = F.udf(lambda occupation, department, *features : predict_prod(occupation, department, *features), IntegerType())

completed = (
    training_tremi.withColumns({
        'surface': (
            F.when(
                F.col('surface').isNull(), 
                udf_surf(
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
                )    
            )
            .otherwise(F.col('surface'))
        ),
        'heating_production': (
            F.when(
                F.col('heating_production').isNull(), 
                udf_prod(
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
                    F.col('n_destruction_licence'),
                )
            )
            .otherwise(F.col('heating_production'))
        )
    })
)

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

from pyspark.sql.types import IntegerType

# COMMAND ----------

predicting = np.array(
    housing_final.select(
        F.col('heating_production'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_system'),
        F.col('hot_water_system')  
    ).collect()
)

X_hot = encoders['DPE_consumption'].transform(predicting[:,0])
X_not_hot = predicting[:,1:]
X = np.column_stack((X_not_hot, X_hot))
y_pred_dpe = models[2].predict(X)
y_pred_ges = models[3].predict(X)
print(y_pred_dpe.shape, y_pred_ges.shape)


# COMMAND ----------

def predict_dpe(heating, *features):
    X_hot = encoders['DPE_consumption'].transform(np.array([[heating]]))
    X_not_hot = np.array([features])
    X = np.column_stack((X_not_hot, X_hot))
    return int(models[2].predict(X))

udf_dpe = F.udf(lambda heating, *features : predict_dpe(heating, *features), IntegerType())

def predict_ges(heating, *features):
    X_hot = encoders['GES_emission'].transform(np.array([[heating]]))
    X_not_hot = np.array([features])
    X = np.column_stack((X_not_hot, X_hot))
    return int(models[3].predict(X))

udf_ges = F.udf(lambda heating, *features : predict_ges(heating, *features), IntegerType())

housing_completed = (
    housing_final
    .withColumns({
        'DPE_consumption': (
            udf_dpe(
                F.col('heating_production'),
                F.col('type'),
                F.col('construction_date'),
                F.col('heating_system'),
                F.col('hot_water_system')  
            )
        ),
        'GES_emission': (
            udf_ges(
                F.col('heating_production'),
                F.col('type'),
                F.col('construction_date'),
                F.col('heating_system'),
                F.col('hot_water_system')  
            )
        ),
    })
)

# COMMAND ----------



# COMMAND ----------

housing_completed.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Housing")

# COMMAND ----------

housing_completed.show()
housing_completed_2 = spark.sql("SELECT * FROM Gold.Housing")
