# Databricks notebook source
# MAGIC %md
# MAGIC #Breweries Project Pipeline Bronze
# MAGIC
# MAGIC This project aims to create a pipeline for breweries of the AB InBev Group.
# MAGIC
# MAGIC
# MAGIC **Responsible Engineer: Ozeas Gomes <p>
# MAGIC Created on: 02/12/2025 <p>
# MAGIC Last updated: 02/12/2025 <p>**

# COMMAND ----------

# MAGIC %md
# MAGIC ##Connecting to the API

# COMMAND ----------

# MAGIC %md
# MAGIC ####Installing Required Dependencies
# MAGIC
# MAGIC For a cleaner environment, two functions were created and saved in an external notebook, which can be accessed at the link below.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Importing Dependencies
# MAGIC

# COMMAND ----------

# MAGIC %run /Users/ozeasjgomes@gmail.com/brewery_data_pipeline/pipeline_functions
# MAGIC
from pipeline_functions import get_breweries, save_df_to_bronze # ... importe outras funções que você usa
from pyspark.sql import SparkSession # Importe SparkSession se necessário aqui
from pyspark.sql.functions import * # Importe funções do PySpark que você usa
from pyspark.sql.types import * # Importe tipos do PySpark que você usa
from datetime import datetime
import sys
# COMMAND ----------

# MAGIC %md
# MAGIC ####Starting Code
spark = SparkSession.builder.appName("BronzePipeline").getOrCreate() # Ajuste o nome do app
# COMMAND ----------

#url target
base_url = 'https://api.openbrewerydb.org/breweries'

# COMMAND ----------

# MAGIC %md
# MAGIC #### functions fetch_daily_breweries
# MAGIC I try a new function to try retrieving data by date from the API, but since the API does not organize data by date, <p>
# MAGIC it will not be possible to perform this segregation and save time and space.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#function that gets data from the API
api_data = get_breweries(base_url)

# COMMAND ----------

df_api = spark.createDataFrame(api_data)

# COMMAND ----------

df_tabela = spark.read.format('delta').load('dbfs:/dbfs/FileStore/project_breweries/bronze/breweries_202502121744')

# COMMAND ----------

# First, register your tables as temporary views
# df_api.createOrReplaceTempView("tabela_delta")
# tabela_delta.createOrReplaceTempView("df_api")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attention: For some reason, I couldn't use MERGE INTO in Databricks Community.
# MAGIC ## So, right below the commented code, I will use subtract for Incremental Batch Ingestion.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# %sql
# MERGE INTO tabela_delta AS delta
# USING df_api AS api
# ON delta.id = api.id
# WHEN MATCHED THEN
#   UPDATE SET
#     address_1 = api.address_1,
#     address_2 = api.address_2,
#     address_3 = api.address_3,
#     brewery_type = api.brewery_type,
#     city = api.city,
#     country = api.country,
#     latitude = api.latitude,
#     longitude = api.longitude,
#     name = api.name,
#     phone = api.phone,
#     postal_code = api.postal_code,
#     state = api.state,
#     state_province = api.state_province,
#     street = api.street,
#     website_url = api.website_url
# WHEN NOT MATCHED THEN
#   INSERT (
#     id,
#     address_1,
#     address_2,
#     address_3,
#     brewery_type,
#     city,
#     country,
#     latitude,
#     longitude,
#     name,
#     phone,
#     postal_code,
#     state,
#     state_province,
#     street,
#     website_url
#   )
#   VALUES (
#     api.id,
#     api.address_1,
#     api.address_2,
#     api.address_3,
#     api.brewery_type,
#     api.city,
#     api.country,
#     api.latitude,
#     api.longitude,
#     api.name,
#     api.phone,
#     api.postal_code,
#     api.state,
#     api.state_province,
#     api.street,
#     api.website_url
#   );

# -- Para visualizar os registros que foram alterados/inseridos
# SELECT * 
# FROM tabela_delta
# WHERE _change_type IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating a condition to save new data in the Delta table if available

# COMMAND ----------

df_broze = df_api.subtract(df_tabela)
if df_broze.isEmpty():
    saida = "There are no new data in the API"
    sys.exit(saida)
else:
    #function to save the data in bronze layer
    save_df_to_bronze(api_data, path='/dbfs/FileStore/project_breweries/bronze')
