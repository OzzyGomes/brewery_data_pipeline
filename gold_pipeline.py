# Databricks notebook source
# MAGIC %md
# MAGIC #Breweries Project Pipeline Silver
# MAGIC
# MAGIC This project aims to create a pipeline for breweries of the AB InBev Group.
# MAGIC
# MAGIC
# MAGIC **Responsible Engineer: Ozeas Gomes <p>
# MAGIC Created on: 02/14/2025 <p>
# MAGIC Last updated: 02/14/2025 <p>**

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

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, lit, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder.appName("GoldPipeline").getOrCreate()
# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading data

# COMMAND ----------

gold_breweries_df = spark.read.format("delta").load("/dbfs/FileStore/project_breweries/silver/breweries_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations for the Gold Layer:
# MAGIC ### Count of Breweries by Type (brewery_type):

# COMMAND ----------

# Aggregation by brewery type

gold_brewery_type = gold_breweries_df.groupBy("brewery_type").count() \
                             .withColumnRenamed("count", "quantidade_cervejarias")

# Displaying the result

#gold_brewery_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count of Breweries by Location (city, state, and country)

# COMMAND ----------

# Aggregation by location (city, state, country)
gold_location = gold_breweries_df.groupBy("city", "state", "country").count() \
                          .withColumnRenamed("count", "quantidade_cervejarias")


# Displaying the result
#gold_location.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count of Breweries by Type and Location

# COMMAND ----------

# Aggregation by brewery type and location
gold_type_location = gold_breweries_df.groupBy("brewery_type", "city", "state", "country").count() \
                               .withColumnRenamed("count", "quantidade_cervejarias")


# Displaying the result
#gold_type_location.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving the Gold Layer in Delta Lake
# MAGIC Here, since it is a small dataset, I will keep **overwrite** for updates. However, depending on future data volume, **append** can be adopted with minor code modifications.

# COMMAND ----------

# Saving the aggregation by brewery type to the Gold layer
gold_brewery_type.write.format("delta").mode("overwrite").saveAsTable("gold_cervejarias_por_tipo")

# Saving the aggregation by location to the Gold layer
gold_location.write.format("delta").mode("overwrite").saveAsTable("gold_cervejarias_por_localizacao")

# Saving the combined aggregation (brewery type and location) to the Gold layer
gold_type_location.write.format("delta").mode("overwrite").saveAsTable("gold_cervejarias_tipo_localizacao")
