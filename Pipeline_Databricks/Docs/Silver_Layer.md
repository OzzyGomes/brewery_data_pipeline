## Detailed Documentation for Silver Layer Notebook - AB InBev Breweries Project

This document details the silver layer notebook for the AB InBev breweries project pipeline. The silver layer focuses on data cleaning, standardization, and enrichment, preparing the data for analysis and consumption in the gold layer.

**Responsible Engineer:** Ozeas Gomes

**Creation Date:** 02/12/2025

**Last Updated:** 02/13/2025

### Objective

The silver layer notebook refines the raw data ingested in the bronze layer. This involves cleaning inconsistencies, handling missing values, standardizing data types, and removing duplicates, ultimately creating a more robust and reliable dataset.

### Dependencies

Python

```python
%run /Users/ozeasjgomes@gmail.com/brewery_data_pipeline/pipeline_functions

from pyspark.sql.functions import col, trim, when, lit, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
```

This section imports the necessary functions from the `pipeline_functions` notebook (detailed in its own documentation) and PySpark SQL functions for data manipulation.

### Data Reading and Initial Transformations

Python

```python
silver_breweries_df = spark.read.format("delta").load("dbfs:/dbfs/FileStore/project_breweries/bronze/breweries_202502121744")

silver_breweries_df = silver_breweries_df.select(['id', 'name', 'brewery_type','phone', 'street', 'city', 'state', 'state_province',
                                                'country', 'postal_code', 'latitude', 'longitude','website_url'])

colunas_texto = silver_breweries_df.columns
for coluna in colunas_texto:
    silver_breweries_df = silver_breweries_df.withColumn(coluna, trim(col(coluna)))

silver_breweries_df.where(col('state')!= col('state_province')).display()
silver_breweries_df = silver_breweries_df.drop(col('state_province'))
```

This section reads the data from the bronze layer (specifically, a Delta Lake table), selects relevant columns, removes `address_1`, `address_2`, and `address_3` (justified by prior analysis), and trims whitespace from all string columns. The `state_province` column is dropped after confirming its redundancy with the `state` column.

### Duplicate Data Handling

Python

```python
silver_breweries_df.count()
silver_breweries_df = silver_breweries_df.distinct()
silver_breweries_df.count()

silver_breweries_df.select(['name', 'brewery_type', 'phone', 'street', 'city', 'state', 'country', 'postal_code', 'latitude', 'longitude', 'website_url']).distinct().count()

silver_breweries_df.groupBy(['name', 'brewery_type', 'phone', 'street', 'city', 'state', 'country', 'postal_code', 'latitude', 'longitude', 'website_url']).count().filter("count > 1").display()

silver_breweries_df.where((col('name') == 'BrewDog Berlin Mitte') | (col('name') == 'BRLO Brwhouse') | (col('name') == 'Berlin Craft Beer Experience')).display()

valores_remover = ["e58d60d7-92f7-4f8d-8a1a-6d02c25a32ee", "307847a0-c60b-43e8-a42d-b8f6b5fb3092", "c75eb363-ba15-4f96-a3cf-d6462867a4e3"]
silver_breweries_df = silver_breweries_df.filter(~silver_breweries_df.id.isin(valores_remover))
```

This section addresses duplicate records. It first checks for distinct rows and then investigates duplicates based on all relevant columns (excluding the `id`). Duplicates are identified, displayed, and then removed based on a selection of `id` values corresponding to the duplicate entries. This approach assumes a business rule or data quality process has determined which `id` to retain for duplicate entries.

### Missing Latitude and Longitude Data



```python
silver_breweries_df.where("latitude is null").display()
ltlg_df = silver_breweries_df.where("latitude is null")
ltlg_df = ltlg_df.sample(0.01, 123)
df_corrigido = fill_missing_lat_long(ltlg_df)
```

This section acknowledges the presence of missing latitude and longitude data. Due to the volume of missing data, external geocoding services are deemed impractical for the full dataset. A sample of the missing data is geocoded using the `fill_missing_lat_long` function (from `pipeline_functions`), demonstrating a potential approach if a smaller subset of data needed enrichment. It's important to note that this sample is not merged back into the main DataFrame in this notebook. A more robust solution would be to integrate a paid geocoding API or handle the missing data in a later stage (e.g., imputation or specific analysis techniques that account for missingness).

### Addressing Missing Street and Phone Data



```python
silver_breweries_df.sample(0.02, 123).display()
silver_breweries_df.where("phone is null").display()
```

This section briefly explores missing `street` and `phone` data. It suggests the possibility of filtering and automating data collection from the brewery websites, but no concrete implementation is included in this notebook.

### Standardizing Null Data


```python
silver_breweries_df = silver_breweries_df.fillna({
    'brewery_type': 'Unknown',
    'street': 'Unknown',
    'city': 'Unknown',
    'postal_code': 'N/A',
    'website_url': 'N/A',
    'latitude': 0.0,
    'longitude': 0.0
})
```

This section replaces null values in several columns with standardized values, ensuring consistency and preventing errors in downstream processing.

### Phone Number Cleaning


```python
silver_breweries_df = silver_breweries_df.withColumn("phone", regexp_replace(col("phone"), "[+\\-\\s]", ""))
```

This section removes specific characters (+, -, and whitespace) from phone numbers to standardize the format.

### Data Type Standardization


```python
silver_breweries_df = (silver_breweries_df
  .withColumn("id", col("id").cast(StringType()))
  .withColumn("name", col("name").cast(StringType()))
  .withColumn("brewery_type", col("brewery_type").cast(StringType()))
  .withColumn("phone", col("phone").cast(StringType()))
  .withColumn("street", col("street").cast(StringType()))
  .withColumn("city", col("city").cast(StringType()))
  .withColumn("state", col("state").cast(StringType()))
  .withColumn("country", col("country").cast(StringType()))
  .withColumn("postal_code", col("postal_code").cast(StringType()))
  .withColumn("latitude", col("latitude").cast(DoubleType()))
  .withColumn("longitude", col("longitude").cast(DoubleType()))
  .withColumn("website_url", col("website_url").cast(StringType()))
)

silver_breweries_df.printSchema()
```

This section explicitly casts each column to its appropriate data type, ensuring data consistency and compatibility with downstream systems.

### Writing to Silver Layer


```python
caminho_silver_delta = "/dbfs/FileStore/project_breweries/silver/breweries_delta"

silver_breweries_df.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(caminho_silver_delta)

print(f"Silver Layer DataFrame created and saved in Delta Lake: {caminho_silver_delta}")

spark.sql("CREATE TABLE IF NOT EXISTS default.silver_breweries USING delta LOCATION '{0}'".format(caminho_silver_delta))
```

Finally, the cleaned and transformed DataFrame is written as a Delta Lake table to the silver layer in DBFS. The schema is overwritten to reflect the changes made in the notebook. A Spark SQL table is also created, making the data easily accessible for querying and analysis.