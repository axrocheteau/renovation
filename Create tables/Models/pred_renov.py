# Databricks notebook source
# MAGIC %md
# MAGIC # pred renov
# MAGIC predict has_to_renov variables on DPE housings with Housing variable from tremi dataset

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
dpe = spark.sql("SELECT * FROM Silver.dpe")
housing = spark.sql("SELECT * FROM Silver.Housing")
weather = spark.sql("SELECT * FROM Silver.Weather")
municipality = spark.sql("SELECT * FROM Silver.Municipality")
municipality_info = spark.sql("SELECT * FROM Silver.Municipality_info")


# COMMAND ----------

training = (
    housing.select(
        F.col('id_owner'),
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('surface'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('first_date_renov'),
        F.col('has_done_renov')
    )
    .join(
        municipality_info,
        [F.col('first_date_renov') == F.col('year'), F.col('housing.id_municipality') == F.col('municipality_info.id_municipality')],
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
        weather.select(
            F.col('humidity').alias('humidity_2'),
            F.col('wind_speed').alias('wind_speed_2'),
            F.col('temp_degree').alias('temp_degree_2'),
            F.col('year').alias('year_2'),
            F.col('department_number').alias('department_number_2')
        ),
        (
            (F.col('first_date_renov') == F.col('year_2') + 2) &
            (F.col('municipality.department_number') == F.col('department_number_2'))
        ),
        'inner'
    )
    .join(
        weather.select(
            F.col('humidity').alias('humidity_1'),
            F.col('wind_speed').alias('wind_speed_1'),
            F.col('temp_degree').alias('temp_degree_1'),
            F.col('year').alias('year_1'),
            F.col('department_number').alias('department_number_1')
        ),
        (
            (F.col('first_date_renov') == F.col('year_1') + 1) &
            (F.col('municipality.department_number') == F.col('department_number_1'))
        ),
        'inner'
    )
    .join(
        weather.select(
            F.col('humidity').alias('humidity_0'),
            F.col('wind_speed').alias('wind_speed_0'),
            F.col('temp_degree').alias('temp_degree_0'),
            F.col('year').alias('year_0'),
            F.col('department_number').alias('department_number_0')
        ),
        (
            (F.col('first_date_renov') == F.col('year_0')) &
            (F.col('municipality.department_number') == F.col('department_number_0'))
        ),
        'inner'
    )
    .select(
        F.col('type'),
        F.col('construction_date'),
        F.col('surface'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('humidity_0'),
        F.col('wind_speed_0'),
        F.col('temp_degree_0'),
        F.col('humidity_1'),
        F.col('wind_speed_1'),
        F.col('temp_degree_1'),
        F.col('humidity_2'),
        F.col('wind_speed_2'),
        F.col('temp_degree_2'),
        F.col('population'),
        F.col('n_development_licence'),
        F.col('n_construction_licence'),
        F.col('n_destruction_licence'),
        F.col('n_new_buildings'),
        F.col('department_number'),
        F.col('has_done_renov'),
    )
)

print(training.count())
display(training)


# COMMAND ----------

prediction = (
    dpe.select(
        F.col('id_dpe'),
        F.col('id_municipality'),
        F.col('type'),
        F.col('construction_date'),
        F.col('surface'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('DPE_consumption'),
        F.col('GES_emission'),
        F.col('dpe_date'),
        F.col('has_to_renov')
    )
    .join(
        municipality_info,
        [F.col('dpe_date') == F.col('year'), F.col('dpe.id_municipality') == F.col('municipality_info.id_municipality')],
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
        weather.select(
            F.col('humidity').alias('humidity_2'),
            F.col('wind_speed').alias('wind_speed_2'),
            F.col('temp_degree').alias('temp_degree_2'),
            F.col('year').alias('year_2'),
            F.col('department_number').alias('department_number_2')
        ),
        (
            (F.col('dpe_date') == F.col('year_2') + 2) &
            (F.col('municipality.department_number') == F.col('department_number_2'))
        ),
        'inner'
    )
    .join(
        weather.select(
            F.col('humidity').alias('humidity_1'),
            F.col('wind_speed').alias('wind_speed_1'),
            F.col('temp_degree').alias('temp_degree_1'),
            F.col('year').alias('year_1'),
            F.col('department_number').alias('department_number_1')
        ),
        (
            (F.col('dpe_date') == F.col('year_1') + 1) &
            (F.col('municipality.department_number') == F.col('department_number_1'))
        ),
        'inner'
    )
    .join(
        weather.select(
            F.col('humidity').alias('humidity_0'),
            F.col('wind_speed').alias('wind_speed_0'),
            F.col('temp_degree').alias('temp_degree_0'),
            F.col('year').alias('year_0'),
            F.col('department_number').alias('department_number_0')
        ),
        (
            (F.col('dpe_date') == F.col('year_0')) &
            (F.col('municipality.department_number') == F.col('department_number_0'))
        ),
        'inner'
    )
    .select(
        F.col('id_dpe'),
        F.col('type'),
        F.col('construction_date'),
        F.col('surface'),
        F.col('heating_system'),
        F.col('hot_water_system'),
        F.col('heating_production'),
        F.col('DPE_consumption').cast('int'),
        F.col('GES_emission').cast('int'),
        F.col('humidity_0'),
        F.col('wind_speed_0'),
        F.col('temp_degree_0'),
        F.col('humidity_1'),
        F.col('wind_speed_1'),
        F.col('temp_degree_1'),
        F.col('humidity_2'),
        F.col('wind_speed_2'),
        F.col('temp_degree_2'),
        F.col('population'),
        F.col('n_development_licence'),
        F.col('n_construction_licence'),
        F.col('n_destruction_licence'),
        F.col('n_new_buildings'),
        F.col('department_number'),
        F.col('has_to_renov'),
    )
)

print(prediction.count())
display(prediction)

# COMMAND ----------

# Replace <run-id1> with the run ID you identified in the previous step.
run_id1 = "c5cd522cdd7643d291e1ea6d0fda6f22"
model_uri = "runs:/" + run_id1 + "/model"

import mlflow.sklearn
model = mlflow.sklearn.load_model(model_uri=model_uri)

import mlflow.pyfunc
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

# COMMAND ----------

predicted_df = (
    prediction.withColumn("has_to_renov", pyfunc_udf(
            F.struct(
                'type',
                'construction_date',
                'surface',
                'heating_system',
                'hot_water_system',
                'heating_production',
                'DPE_consumption',
                'GES_emission',
                'humidity_0',
                'wind_speed_0',
                'temp_degree_0',
                'humidity_1',
                'wind_speed_1',
                'temp_degree_1',
                'humidity_2',
                'wind_speed_2',
                'temp_degree_2',
                'population',
                'n_development_licence',
                'n_construction_licence',
                'n_destruction_licence',
                'n_new_buildings',
                'department_number',
            )
        )
    )
)
display(predicted_df)
predicted_df.groupBy('has_to_renov').count().show()

# COMMAND ----------

training.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Model.training_renov_no_diff")

predicted_df.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Model.predicted_renov")
