# Databricks notebook source
# MAGIC %md
# MAGIC # Create Answers

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
tremi = spark.sql("SELECT * FROM datalake.tremi")
Dictionnary = spark.sql("SELECT * FROM Silver.Dictionary")


# COMMAND ----------

# unpivot the data to get all answers for every interestting question
unpivotExpr = "stack(62, 'main_q2_1', main_q2_1, 'main_q2_2', main_q2_2, 'main_q2_3', main_q2_3, 'main_q2_4', main_q2_4, 'main_q2_5', main_q2_5, 'main_q2_6', main_q2_6, 'main_q2_7', main_q2_7, 'main_q2_8', main_q2_8, 'main_q3_1', main_q3_1, 'main_q3_2', main_q3_2, 'main_q3_3', main_q3_3, 'main_q3_4', main_q3_4, 'main_q3_5', main_q3_5, 'main_q3_6', main_q3_6, 'main_q3_7', main_q3_7, 'main_q4_1', main_q4_1, 'main_q4_2', main_q4_2, 'main_q4_3', main_q4_3, 'main_q4_4', main_q4_4, 'main_q4_5', main_q4_5, 'main_q4_6', main_q4_6, 'main_q4_7', main_q4_7, 'main_q4_8', main_q4_8, 'main_Q70_01', main_Q70_01, 'main_Q70_02', main_Q70_02, 'main_Q70_03', main_Q70_03, 'main_Q70_04', main_Q70_04, 'main_Q70_05', main_Q70_05, 'main_Q70_06', main_Q70_06, 'main_Q70_07', main_Q70_07, 'main_Q70_08', main_Q70_08, 'main_Q70_09', main_Q70_09, 'main_Q70_10', main_Q70_10, 'main_Q70_11', main_Q70_11, 'main_Q70_12', main_Q70_12, 'main_Q70_13', main_Q70_13, 'main_Q70_14', main_Q70_14, 'main_Q71_01', main_Q71_01, 'main_Q71_03', main_Q71_03, 'main_Q71_04', main_Q71_04, 'main_Q71_05', main_Q71_05, 'main_Q71_06', main_Q71_06, 'main_Q71_07', main_Q71_07, 'main_Q71_08', main_Q71_08, 'main_Q71_09', main_Q71_09, 'main_Q71_10', main_Q71_10, 'main_Q71_11', main_Q71_11, 'main_Q71_12', main_Q71_12, 'main_Q71_13', main_Q71_13, 'main_Q71_14', main_Q71_14, 'main_Q74_1', main_Q74_1, 'main_Q74_2', main_Q74_2, 'main_Q74_3', main_Q74_3, 'main_Q74_4', main_Q74_4, 'main_Q74_5', main_Q74_5, 'main_q81_1', main_q81_1, 'main_q81_2', main_q81_2, 'main_q81_3', main_q81_3, 'main_q81_4', main_q81_4, 'main_q81_5', main_q81_5, 'main_q81_6', main_q81_6, 'main_q81_7', main_q81_7) AS (column_name, answer)"
all_answer_df = tremi.select(F.col("Respondent_Serial").alias('id_owner'), F.expr(unpivotExpr)).where("answer == 1").dropDuplicates()
display(all_answer_df)


# COMMAND ----------

# join with dictionnary to do an association table
answer = (
    all_answer_df.join(
        Dictionnary, ['column_name'], 'inner'
    )
    .select('id_owner', 'id_answer')
)
display(answer)


# COMMAND ----------

# save as table
answer.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Silver.Answer")
