# Databricks notebook source
# MAGIC %md
# MAGIC # Create Renovation

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
display(tremi)

# COMMAND ----------

# unpivot datato fit scheme. Answers to questions about different renovation are in line
stack_expr = "stack(21, Respondent_Serial, 'roof_with_insulation', 11, main_Q5_11, main_Q6_11, main_Q18_11, main_Q20_11, Respondent_Serial, 'rooth_without_insulation', 12, main_Q5_12, main_Q6_12, main_Q18_12, main_Q20_12, Respondent_Serial, 'insulation_roof_without_renovation', 13, main_Q5_13, main_Q6_13, main_Q18_13, main_Q20_13, Respondent_Serial, 'insulation_flooring_attic', 14, main_Q5_14, main_Q6_14, main_Q18_14, main_Q20_14, Respondent_Serial, 'roof_terrace_with_insulation', 15, main_Q5_15, main_Q6_15, main_Q18_15, main_Q20_15, Respondent_Serial, 'roof_terrase_without_insulation', 16, main_Q5_16, main_Q6_16, main_Q18_16, main_Q20_16, Respondent_Serial, 'ext_wall_with_insulation', 21, main_Q5_21, main_Q6_21, main_Q18_21, main_Q20_21, Respondent_Serial, 'ext_wall_without_insulation', 22, main_Q5_22, main_Q6_22, main_Q18_22, main_Q20_22, Respondent_Serial, 'int_wall_with_insulation', 23, main_Q5_23, main_Q6_23, main_Q18_23, main_Q20_23, Respondent_Serial, 'int_wall_without_insulation', 24, main_Q5_24, main_Q6_24, main_Q18_24, main_Q20_24, Respondent_Serial, 'down_flooring_with_insulation', 31, main_Q5_31, main_Q6_31, main_Q18_31, main_Q20_31, Respondent_Serial, 'down_flooring_without_insulation', 32, main_Q5_32, main_Q6_32, main_Q18_32, main_Q20_32, Respondent_Serial, 'opening_common_areas', 41, main_Q5_41, main_Q6_41, NULL, NULL, Respondent_Serial, 'opening_accomodation', 42, main_Q5_42, main_Q6_42, main_Q18_42, main_Q20_42, Respondent_Serial, 'ext_doors', 43, main_Q5_43, main_Q6_43, main_Q18_43, main_Q20_43, Respondent_Serial, 'heating_system', 51, main_Q5_51, main_Q6_51, main_Q18_51, main_Q20_51, Respondent_Serial, 'heating_regulation', 52, main_Q5_52, main_Q6_52, main_Q18_52, main_Q20_52, Respondent_Serial, 'hot_water', 53, main_Q5_53, main_Q6_53, main_Q18_53, main_Q20_53, Respondent_Serial, 'insulation', 54, main_Q5_54, main_Q6_54, main_Q18_54, main_Q20_54, Respondent_Serial, 'ventilation_system', 55, main_Q5_55, main_Q6_55, main_Q18_55, main_Q20_55, Respondent_Serial, 'air_consitioning_system', 56, main_Q5_56, main_Q6_56, main_Q18_56, main_Q20_56) AS (id_owner, work_type, work_type_number, start_date, end_date, done_by, amount)"

# COMMAND ----------

renovation = (
    tremi.select(F.expr(stack_expr))
    .where("start_date IS NOT NULL") # select only renovation that have been done
    .dropDuplicates()
    .select(F.monotonically_increasing_id().alias('id_renov'), "*") # add id
)
display(renovation)

# COMMAND ----------

# save as table
renovation.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Silver.Renovation")
