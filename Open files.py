# Databricks notebook source
# MAGIC %md
# MAGIC ## Licence

# COMMAND ----------

# File location and type
file_location_array = ["/FileStore/tables/PC_DP_creant_logements_2013_2016.csv",
                    "/FileStore/tables/permis_demolir.csv",
                    "/FileStore/tables/permis_amenager.csv"]
file_type = "csv"

# CSV options
infer_schema_array = ["true", "true", "true"]
first_row_is_header_array = ["true", "true", "true"]
delimiter_array = [";", ";", ";"]

# The applied options are for CSV files. For other file types, these will be ignored.
licences_df = []
for file_location, infer_schema, first_row_is_header, delimiter \
in zip(file_location_array, infer_schema_array, first_row_is_header_array, delimiter_array):
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
    licences_df.append(df.alias('df'))

df_construction_licence, df_destruction_licence, df_devlopment_licence = licences_df
display(df_construction_licence)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Tremi

# COMMAND ----------

dbutils.fs.ls ("/FileStore/tables/")

# COMMAND ----------

# File location and type
file_location_array = ["/FileStore/tables/TREMI_2017-Résultats enquête bruts.csv",
                        "/FileStore/tables/TREMI_2017_CodeBook_public8.txt"]
file_type = "csv"

# CSV options
infer_schema_array = ["true", "true"]
first_row_is_header_array = ["true", "true"]
delimiter_array = [";", "\t"]

# The applied options are for CSV files. For other file types, these will be ignored.
tremi_df = []
for file_location, infer_schema, first_row_is_header, delimiter \
in zip(file_location_array, infer_schema_array, first_row_is_header_array, delimiter_array):
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
    tremi_df.append(df.alias('df'))

df_tremi, df_dictionnary = tremi_df
display(df_tremi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population

# COMMAND ----------

# MAGIC %md
# MAGIC ## DPE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elec

# COMMAND ----------

# MAGIC %md
# MAGIC ## Localisation

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `PC_DP_creant_logements_2013_2016_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "PC_DP_creant_logements_2013_2016_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
