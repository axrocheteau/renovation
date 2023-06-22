# Databricks notebook source
# MAGIC %md
# MAGIC # Owner

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
silver_owner = spark.sql("SELECT * FROM Silver.Owner")
silver_housing = spark.sql("SELECT * FROM Silver.Housing")


# COMMAND ----------

owner = (
    silver_owner.select(
        F.col('id_owner'),
        F.col('gender'),
        F.col('age'),
        F.col('work_state'),
        F.col('job'),
        F.col('home_state'),
        F.col('has_done_renov'),
        F.col('nb_persons_home'),
        F.col('income')
    )
    .join(
        # détenteur d'une maison ou d'un appartement
        (
            silver_housing.select(
                F.col('id_owner'),
                F.col('type')
            )
            .filter(F.col('type').isin([1,2]))
        ),
        ['id_owner'],
        'inner'
    )
    .filter(
        (F.col('nb_persons_home') != 99) & # a répondu à la question
        (F.col('nb_persons_home').isNotNull()) &
        (F.col('age').isNotNull()) &
        (~F.col('income').isin([10,11,99])) & # a répondu à la question en donnant une valeur
        (F.col('income').isNotNull())
    )
    .withColumns({
        'gender' : (
            F.when(F.col('gender') == 1, 0)
            .otherwise(1)
        ),
        # transform work and job into occupation
        'occupation' : ( 
            F.when(
                ( # is working
                    (F.col('work_state').isin([1,2])) &
                    (F.col('job') != 11)
                ),
                F.col('job') + 5
            )
            .otherwise( # is not working
                F.col('work_state') -2
            )
        ),
        'home_state' : (
            F.when(F.col('home_state') == 1, 0)
            .otherwise(1)
        ),
        'has_done_renov' : (
            F.when(F.col('has_done_renov') == 1, 0)
            .otherwise(1)
        )  
    })
    .select(
        F.col('id_owner'),
        F.col('gender'),
        F.col('age'),
        F.col('occupation'),
        F.col('home_state'),
        F.col('has_done_renov'),
        F.col('nb_persons_home'),
        F.col('income')
    )
)

print(silver_owner.count(), owner.count())
display(owner)

# COMMAND ----------

owner.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Owner")
