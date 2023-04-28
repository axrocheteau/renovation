# Databricks notebook source
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.pandas import DataFrame as D

# spark session to warehouse
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dictionnary

# COMMAND ----------

# load df
df = spark.sql("SELECT * FROM datalake.codebook")
display(df)

# COMMAND ----------

# unpivot the data to get all answers for every question
unpivotExpr = "stack(24, '1', _1, '2', _2, '3', _3, '4', _4, '5', _5, '6', _6, '7', _7, '99', _99, '0', _0, '8', _8, '9', _9, '10', _10, '11', _11, '12', _12, '13', _13, '14', _14, '15', _15, '16', _16, '17', _17, '18', _18, '19', _19, '20', _20, '21', _21, '22', _22) AS (answer_number, answer)"
all_answer_df = df.select("Name","VARNUM","LABEL", F.expr(unpivotExpr)).where("answer IS NOT NULL")
count_df = all_answer_df.groupBy("VARNUM").count()
display(all_answer_df)

# COMMAND ----------

mcq_df = df.select("VARNUM", 
                F.regexp_extract(df.LABEL, ' - (.*)', 1).alias('answer_char'),
                F.regexp_extract(df.LABEL, '(.*) - ', 1).alias('question_char'),
                F.reverse(F.split(F.reverse(df.NAME),'_').getItem(0)).alias('answer_num')
)         
mcq_df = mcq_df.join(count_df, ["VARNUM"], 'inner') \
        .orderBy('VARNUM')
display(mcq_df)

# COMMAND ----------

df_final = all_answer_df.join(mcq_df, ["VARNUM"], 'inner')
display(df_final)

# COMMAND ----------

Dictionnary=df_final.withColumns({'final_answer': F.when((df_final['question_char'] != '') \
                                    & (df_final['question_char'] != 'Variable filtre') \
                                    & (df_final['question_char'] !='BLOCS Travaux') \
                                    & (df_final['count']<=3), df_final['answer_char'])\
                                .otherwise(df_final['answer']),
                            'final_question': F.when((df_final['question_char'] != '') \
                                    & (df_final['question_char'] != 'Variable filtre') \
                                    & (df_final['question_char'] !='BLOCS Travaux') \
                                    &  (df_final['count']<=3), df_final['question_char'])\
                                .otherwise(df_final['LABEL']),
                            'final_answer_number': F.when((df_final['question_char'] != '') \
                                    & (df_final['question_char'] != 'Variable filtre') \
                                    & (df_final['question_char'] !='BLOCS Travaux') \
                                    &  (df_final['count']<=3), df_final['answer_num'])\
                                .otherwise(df_final['answer_number'])
})
Dictionnary = Dictionnary.drop('answer_char','answer','question_char','LABEL','answer_num','answer_number', 'count')
Dictionnary = Dictionnary.drop_duplicates()
Dictionnary = Dictionnary.withColumnRenamed('VARNUM', 'varnum')\
                            .withColumnRenamed('Name', 'column_name')\
                            .withColumnRenamed('final_answer', 'answer_char')\
                            .withColumnRenamed('final_question', 'question')\
                            .withColumnRenamed('final_answer_number', 'answer_number')

Dictionnary = Dictionnary.withColumn("id_answer",F.monotonically_increasing_id())

    

# COMMAND ----------

Dictionnary.write.mode('overwrite')\
        .format("parquet") \
        .saveAsTable("Gold.Dictionnary")
