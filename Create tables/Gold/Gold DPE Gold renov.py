# Databricks notebook source
# MAGIC %md
# MAGIC # Gold DPE

# COMMAND ----------

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
dpe_2021 = spark.sql("SELECT * FROM Datalake.dpe_france_2021")
training_renov = spark.sql("SELECT * FROM Model.training_renov_no_diff")
prediction_renov = spark.sql("SELECT * FROM Model.prediction_renov_no_diff")
gold_municipality = spark.sql("SELECT * FROM Gold.Municipality")

# COMMAND ----------

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost.sklearn import XGBClassifier
from sklearn.ensemble import StackingClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
import numpy as np

# COMMAND ----------

parameters = [
    {'name': 'estimator_RF', 'model_type': 'RandomForestClassifier' , 'parameters' : {'class_weight': 'balanced', 'max_depth': 10, 'n_estimators': 192}},
    {'name': 'estimator_xgb', 'model_type': 'XGBClassifier' , 'parameters' : {'learning_rate': 0.01, 'n_estimators': 70}},
]
models = {
    'RandomForestClassifier' : RandomForestClassifier,
    'LogisticRegression' : LogisticRegression,
    'XGBClassifier' : XGBClassifier,
}

parametrized_model = {model_dict['name']: models[model_dict['model_type']](**model_dict['parameters']) for model_dict in parameters}




# COMMAND ----------

target = "has_done_renov"
col_not_hot = [col[0] for col in training_renov.dtypes if col[0] not in [target, 'id_dpe']]

X = np.array(training_renov.select(col_not_hot).collect())

y = training_renov.select(target)
if not 0 in np.array(y.dropDuplicates().collect()):
    y = y.withColumn(target, F.col(target) - 1)
y = np.array(y.collect()).ravel()



# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, r2_score, confusion_matrix, f1_score
import seaborn as sn

# COMMAND ----------


f, axs = plt.subplots(1,2, figsize=(10,5))

for (model_name, model), ax in zip(parametrized_model.items(), axs.flatten()):
    model.fit(X, y)
    y_pred = model.predict(X)
    matrix = confusion_matrix(y, y_pred)

    sn.heatmap(
        (matrix.T / np.sum(matrix, axis=1).T).T,
        ax=ax,
        annot=True,
        fmt=".1%",
    )
    ax.set_ylabel("true")
    ax.set_xlabel("pred")
    ax.set_title(f'{model_name}\nscore : {round(f1_score(y, y_pred, average="micro"),4)}')



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Row

# COMMAND ----------

renov_array = (
    np.array(
        prediction_renov.drop(
            F.col('has_to_renov')
        ).collect()
    )
)

# COMMAND ----------


print(renov_array.shape)
y_pred_xgb = parametrized_model['estimator_xgb'].predict(renov_array[:,1:].astype(float))
y_pred_rf = parametrized_model['estimator_RF'].predict(renov_array[:,1:].astype(float))


schema = StructType([ 
    StructField("id_dpe", StringType(),True),
    StructField("pred_renov_xgb",IntegerType(),True),
    StructField("pred_renov_rf",IntegerType(),True),
])

pred_xgb_rf = spark.createDataFrame(
    data=[Row(id_dpe, xgb, rf) for id_dpe, xgb, rf in zip(renov_array[:,0].tolist(), y_pred_xgb.tolist(), y_pred_rf.tolist())],
    schema=schema
)

# COMMAND ----------

pred_xgb_rf.select('pred_renov_xgb').groupBy('pred_renov_xgb').count().show()
pred_xgb_rf.select('pred_renov_rf').groupBy('pred_renov_rf').count().show()

# COMMAND ----------

predicted_renov = (
    prediction_renov.select(
        F.col('id_dpe'),
        F.col('surface'),
        F.col('construction_date'),
        F.col('heating_production'),
        F.col('DPE_consumption'),
        F.col('GES_emission')
    )
    .join(
        pred_xgb_rf,
        ['id_dpe'],
        'inner'
    )
)

# COMMAND ----------

def to_categorical(x):
    if(x) ==  'insuffisante':
        return 1
    elif(x) == 'moyenne':
        return 2
    elif(x) == 'bonne':
        return 3
    elif(x) == 'très bonne':
        return 4
    else:
        return x

to_categorical_udf = udf(lambda x: to_categorical(x))

def to_renov(x):
    if x is None or x > 1:
        return 0
    else:
        return 1
    
to_renov_udf = udf(lambda x: to_renov(x))

def mean_coalesce(x, y, z):
    if x is None and  y is None and z is None:
        return x
    else:
        return float(np.mean([value for value in [x,y,z] if value is not None and value > 0]))

mean_coalesce_udf = udf(lambda x, y, z: mean_coalesce(x, y, z))

def to_dictionary(dictionary, x):
    return dictionary[x]

dpe_ges_dict = {0:'A', 1:'B', 2:'C', 3:'D', 4:'E', 5:'F', 6:'G'}
heating_prod_dict = {1:'Gaz', 2:'fioul, GPL, propane, butane', 3:' bois, charbon', 4:'Autres', 5:'PAC', 6:'electricité'}
quality_dict = {1 : 'insuffisante', 2 : 'moyenne', 3 : 'bonne', 4 : 'très bonne'}
surface_dict = {1 : '<70 m²', 2:'entre 70 et 115 m²', 3:'>115 m²'}
construction_date_dict = {
    1 : '1948 ou avant',
    2 : 'Entre 1949 et 1974',
    3 : 'Entre 1975 et 1981',
    4 : 'Entre 1982 et 1989',
    5 : 'Entre 1990 et 2000',
    6 : 'Entre 2001 et 2011',
    7 : '2012 ou après',
}

def make_expr(column_name, dictionary):
    return f"""CASE {' '.join([f"WHEN {column_name}='{k}' THEN '{v}'" for k,v in dictionary.items()])} ELSE {column_name} END"""



# COMMAND ----------


dpe = (
    dpe_2021.select(
        F.col("N°DPE").alias('id_dpe'),
        F.col("Code_INSEE_(BAN)").alias('insee_code'),
        F.col("Type_bâtiment").alias('type'),
        F.col("Qualité_isolation_enveloppe"),
        F.col("Qualité_isolation_menuiseries"),
        F.col("Qualité_isolation_murs"),
        F.col("Qualité_isolation_plancher_bas"),
        F.col("Qualité_isolation_plancher_haut_toit_terrase"),
        F.col("Qualité_isolation_plancher_haut_comble_aménagé"),
        F.col("Qualité_isolation_plancher_haut_comble_perdu"),
    )
    .withColumns({
        'quality_shell_insulation' : to_categorical_udf(F.col("Qualité_isolation_enveloppe")).cast('int'),
        'quality_walls_insulation' : to_categorical_udf(F.col("Qualité_isolation_murs")).cast('int'), 
        'quality_carpentry_insulation' : to_categorical_udf(F.col("Qualité_isolation_menuiseries")).cast('int'),
        'quality_flooring_insulation' : to_categorical_udf(F.col("Qualité_isolation_plancher_bas")).cast('int'),
        'quality_ceiling_insulation_terrace' : to_categorical_udf(F.col("Qualité_isolation_plancher_haut_toit_terrase")).cast('int'),
        'quality_ceiling_insulation_developed' : to_categorical_udf(F.col("Qualité_isolation_plancher_haut_comble_aménagé")).cast('int'),
        'quality_ceiling_insulation_unused' : to_categorical_udf(F.col("Qualité_isolation_plancher_haut_comble_perdu")).cast('int'),
        'quality_ceiling_insulation' : (
            mean_coalesce_udf(
                F.col('quality_ceiling_insulation_terrace'),
                F.col("quality_ceiling_insulation_developed"),
                F.col("quality_ceiling_insulation_unused")
            )
        ).cast('int')
    })
    .join(
        predicted_renov.select(
            F.col('id_dpe'),
            F.col('surface'),
            F.col('construction_date'),
            F.col('heating_production'),
            F.col('DPE_consumption'),
            F.col('GES_emission'),
            F.col('pred_renov_xgb'),
            F.col('pred_renov_rf'),
        ),
        ['id_dpe'],
        'inner'
    )
    .withColumns({
        'renov_energy' : (
            F.when(F.col('heating_production') == 2, 1)
            .when(
                (F.col('heating_production') == 1) | (F.col('heating_production') == 3),
                F.when((F.col('DPE_consumption') >= 4) | (F.col('GES_emission') >= 4), 1)
                .otherwise(0)
            )
            .otherwise(0)
        ),
        'heating_production' : F.expr(make_expr('heating_production', heating_prod_dict)),
        'DPE_consumption' : F.expr(make_expr('DPE_consumption', dpe_ges_dict)),
        'GES_emission' : F.expr(make_expr('GES_emission', dpe_ges_dict)),
        'surface' : F.expr(make_expr('surface', surface_dict)),
        'construction_date' : F.expr(make_expr('construction_date', construction_date_dict)),
        'renov_shell' : to_renov_udf(F.col('quality_shell_insulation')).cast('int'),
        'renov_walls' : to_renov_udf(F.col('quality_walls_insulation')).cast('int'),
        'renov_carpentry' : to_renov_udf(F.col('quality_carpentry_insulation')).cast('int'),
        'renov_flooring' : to_renov_udf(F.col('quality_flooring_insulation')).cast('int'),
        'renov_ceiling' : to_renov_udf(F.col('quality_ceiling_insulation')).cast('int'),
    })
    .join(
        gold_municipality.select(
            F.col('insee_code'),
            F.col('id_municipality')
        ),
        ['insee_code'],
        'inner'
    )
)


# COMMAND ----------

insights = (
    dpe.withColumn(
        'nb_renov',
        F.col('renov_shell') +
        F.col('renov_walls') +
        F.col('renov_carpentry') +
        F.col('renov_flooring') +
        F.col('renov_ceiling')
    )
)

xgb_score = (
    insights.groupBy(
        F.col('pred_renov_xgb').alias('pred_renov'),
        F.col('nb_renov'),
    ).count()
)

rf_score = (
    insights.groupBy(
        F.col('pred_renov_rf').alias('pred_renov'),
        F.col('nb_renov'),
    ).count()
)


# COMMAND ----------

xgb = xgb_score.collect()
rf = rf_score.collect()

# COMMAND ----------

for score, name in zip([xgb, rf], ['xgb', 'rf']):
    final = 0
    for s in score:
        if s['pred_renov'] == 1 and s['nb_renov'] >= 2:
            final += s['count'] * s['nb_renov']
        elif s['pred_renov'] == 1 and s['nb_renov'] < 2:
            final -= s['count'] * (s['nb_renov'] + 1)
        elif s['pred_renov'] == 0 and s['nb_renov'] >= 2:
            final -= s['count'] * s['nb_renov']
        else:
            final += s['count'] * (s['nb_renov'] + 1)
    print(name, final)
    

# COMMAND ----------

stack_expr = "stack(5, 'murs', renov_walls, 'toiture', renov_carpentry, 'plancher', renov_flooring, 'plafond', renov_ceiling, 'énergie', renov_energy) AS (renov_type, renov)"
renov = (
    dpe.select(
        F.col('id_dpe'),
        F.col('id_municipality'),
        F.expr(stack_expr)
    )
    .filter(
        F.col('renov') == 1
    )
    .select(
        F.col('id_dpe'),
        F.col('id_municipality'),
        F.col('renov_type')
    )
)


# COMMAND ----------

print(renov.count())
display(renov)

# COMMAND ----------

renov.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Renovation")

# COMMAND ----------

dpe = (
    dpe.select(
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_production'),
        F.col('surface'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('has_to_renov'),
    )
    .groupBy(
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('heating_production'),
        F.col('surface'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('has_to_renov'),
    )
    .count()
)

# COMMAND ----------

dpe.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.dpe")
