# Databricks notebook source
# MAGIC %md
# MAGIC #Breweries Project Pipeline Silver
# MAGIC
# MAGIC This project aims to create a pipeline for breweries of the AB InBev Group.
# MAGIC
# MAGIC
# MAGIC **Responsible Engineer: Ozeas Gomes <p>
# MAGIC Created on: 02/12/2025 <p>
# MAGIC Last updated: 02/13/2025 <p>**

# COMMAND ----------

# MAGIC %md
# MAGIC ####Installing Required Dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ####Importing Dependencies

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

from pyspark.sql.functions import col, trim, when, lit, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


#informations reading e writing
file_path = 'write the path to the file' 
save_path = 'write the save path to the file'

spark = SparkSession.builder.appName("SilverPipeline").getOrCreate()
# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading data
# MAGIC Here again, **MERGE INTO** should have been used to validate all columns at once, but Databricks Community did not allow it.

# COMMAND ----------

spark.read.format("delta").load(file_path).createOrReplaceTempView('delta_bronze')

# COMMAND ----------

spark.read.format("delta").load("/dbfs/FileStore/project_breweries/silver/breweries_delta").createOrReplaceTempView('delta_silver')

# COMMAND ----------

silver_breweries_df = spark.sql("select * from delta_bronze where id not in (select id from delta_silver) and id not in ('e58d60d7-92f7-4f8d-8a1a-6d02c25a32ee','307847a0-c60b-43e8-a42d-b8f6b5fb3092', 'c75eb363-ba15-4f96-a3cf-d6462867a4e3')")

# COMMAND ----------

if silver_breweries_df.isEmpty():
    saida = "There are no new data in the Bronze layer."
    sys.exit(saida)

# COMMAND ----------

# First transformation: Removing columns 'address_1', 'address_2', and 'address_3' 
# After analysis, this was determined to be the best decision.
# Reordered the columns to follow a more standardized format.
silver_breweries_df = silver_breweries_df.select(['id', 'name', 'brewery_type','phone', 'street', 'city', 'state', 
                                                            'country', 'postal_code', 'latitude', 'longitude','website_url'])

# COMMAND ----------

# Ensuring there are no spaces within the columns.
colunas_texto = silver_breweries_df.columns
for coluna in colunas_texto:
    silver_breweries_df = silver_breweries_df.withColumn(coluna, trim(col(coluna)))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Checking and Removing Duplicate Data
# MAGIC It is important to mention that when I created the Silver layer, I identified that the data was duplicated despite the ID.
# MAGIC
# MAGIC In this case, I was able to visualize and understand the situation. However, for a job that will run without human supervision, this technique will not be effective.
# MAGIC
# MAGIC A valid option would be to identify duplicate data and generate an alert within Databricks itself. This way, the alert would send an email to the responsible engineer whenever duplicate data is detected.

# COMMAND ----------

# I checked the possibility of duplicate data by first using the distinct() method.
silver_breweries_df = silver_breweries_df.distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardizing Null Data

# COMMAND ----------

#Handle Missing Data (Example for a few columns, extend as needed)
silver_breweries_df = silver_breweries_df.fillna({
    'brewery_type': 'Unknown',  # Unknown category
    'street': 'Unknown', #Unknown category
    'phone': 'N/A',  # Unknown category
    'city': 'Unknown',  # Unknown category
    'postal_code': 'N/A',  # 'Not Applicable' or default value for postal code
    'website_url': 'N/A',  # 'Not Applicable' or 'no website'
    'latitude': 0.0,  # Default value for latitude (can be adjusted depending on context)
    'longitude': 0.0  # Default value for longitude (can be adjusted depending on context)
})



# COMMAND ----------

# MAGIC %md
# MAGIC #### Removing Strings from the Phone Number

# COMMAND ----------

# Remove the '+' symbol; chose not to remove '-'
silver_breweries_df = silver_breweries_df.withColumn("phone", regexp_replace(col("phone"), "[+\\-\\s]", ""))

# COMMAND ----------

silver_breweries_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding Load Date

# COMMAND ----------

silver_breweries_df = silver_breweries_df.withColumn("_loadDate", lit(datetime.now()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardizing Data Types

# COMMAND ----------

# Converting data to a specific data format
silver_breweries_df = (silver_breweries_df \
    .withColumn("id", col("id").cast(StringType()))\
    .withColumn("name", col("name").cast(StringType()))\
    .withColumn("brewery_type", col("brewery_type").cast(StringType()))\
    .withColumn("phone", col("phone").cast(StringType()))\
    .withColumn("street", col("street").cast(StringType()))\
    .withColumn("city", col("city").cast(StringType()))\
    .withColumn("state", col("state").cast(StringType()))\
    .withColumn("country", col("country").cast(StringType()))\
    .withColumn("postal_code", col("postal_code").cast(StringType()))\
    .withColumn("latitude", col("latitude").cast(DoubleType()))\
    .withColumn("longitude", col("longitude").cast(DoubleType()))\
    .withColumn("website_url", col("website_url").cast(StringType()))
    .withColumn("_loadDate", col("_loadDate").cast(DateType()))
)


# COMMAND ----------

#printing the Schema
silver_breweries_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing the DataFrame as Delta in the Silver Layer

# COMMAND ----------

caminho_silver_delta = save_path # Define the Path in Your Databricks Environment

silver_breweries_df.write.format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .partitionBy("state") \
    .save(caminho_silver_delta)

print(f"DataFrame Silver Layer criado e salvo em Delta Lake: {caminho_silver_delta}")

# COMMAND ----------

# Materialize the Delta table as a table
#spark.sql("CREATE TABLE IF NOT EXISTS default.silver_breweries USING delta LOCATION '{0}'".format(caminho_silver_delta))
