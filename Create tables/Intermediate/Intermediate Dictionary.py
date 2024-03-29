# Databricks notebook source
# MAGIC %md
# MAGIC # Intermediate Dictionnary

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
df = spark.sql("SELECT * FROM datalake.codebook")
display(df)

# COMMAND ----------

# unpivot the data to get all answers for every question
unpivotExpr = "stack(24, '1', _1, '2', _2, '3', _3, '4', _4, '5', _5, '6', _6, '7', _7, '99', _99, '0', _0, '8', _8, '9', _9, '10', _10, '11', _11, '12', _12, '13', _13, '14', _14, '15', _15, '16', _16, '17', _17, '18', _18, '19', _19, '20', _20, '21', _21, '22', _22) AS (answer_number, answer)"
all_answer_df = df.select("Name","VARNUM","LABEL", F.expr(unpivotExpr)).where("answer IS NOT NULL")
count_df = all_answer_df.groupBy("VARNUM").count() # count number of possible answer to change multiplechoice question just for question with 3 answers : Yes, No ,NA
display(all_answer_df)

# COMMAND ----------

# get multiple questions answers and questions 
# template : question - answer 
# possible answer : Yes, No, N
df_final = (
    df.select(
        "VARNUM", 
        F.regexp_extract(df.LABEL, ' - (.*)', 1).alias('answer_char'),
        F.regexp_extract(df.LABEL, '(.*) - ', 1).alias('question_char'),
        F.reverse(F.split(F.reverse(df.NAME),'_').getItem(0)).alias('answer_num')
    )         
    .join(count_df, ["VARNUM"], 'inner')
    .join(all_answer_df, ["VARNUM"], 'inner')
)

# COMMAND ----------

# replace question with 3 answers by only one 
Dictionary = (
    df_final.select(
        F.col('Name').alias('column_name'),
        F.col('VARNUM').alias('varnum'),
        (
            F.when(
                (df_final['question_char'] != '')
                & (df_final['question_char'] != 'Variable filtre')
                & (df_final['question_char'] !='BLOCS Travaux')
                & (df_final['count']<=3), df_final['answer_char']
            )
            .otherwise(df_final['answer'])
        )
        .alias("answer_char"),
        (
            F.when(
                (df_final['question_char'] != '')
                & (df_final['question_char'] != 'Variable filtre')
                & (df_final['question_char'] !='BLOCS Travaux')
                &  (df_final['count']<=3), df_final['answer_num']
            )
            .otherwise(df_final['answer_number'])
        )
        .alias("answer_number"),
        (
            F.when(
                (df_final['question_char'] != '')
                & (df_final['question_char'] != 'Variable filtre')
                & (df_final['question_char'] !='BLOCS Travaux')
                &  (df_final['count']<=3), df_final['question_char']
            )
            .otherwise(df_final['LABEL'])
        )
        .alias("question")
    )
    .drop_duplicates()
    # Rename to fit scheme and order and add id
    .select(
        F.monotonically_increasing_id().alias('id_answer'),
        "*"
    )
)

# COMMAND ----------

# save as table
Dictionary.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Intermediate.Dictionary")
