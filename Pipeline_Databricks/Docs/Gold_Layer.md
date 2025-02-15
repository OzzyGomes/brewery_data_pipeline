## Detailed Documentation for Gold Layer Notebook - AB InBev Breweries Project

This document details the gold layer notebook for the AB InBev breweries project pipeline. The gold layer focuses on creating aggregated and summarized data from the silver layer, optimized for reporting, analysis, and business intelligence.

**Responsible Engineer:** Ozeas Gomes

**Creation Date:** 02/14/2025

**Last Updated:** 02/14/2025

### Objective

The gold layer notebook transforms the cleaned and standardized data from the silver layer into valuable insights through aggregations and summaries. These aggregations are designed to answer specific business questions and provide a high-level overview of the brewery data.

### Dependencies


```python
%run /Users/ozeasjgomes@gmail.com/brewery_data_pipeline/pipeline_functions

from pyspark.sql.functions import col, trim, when, lit, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
```

This section imports necessary functions from the `pipeline_functions` notebook and PySpark SQL functions for data manipulation and aggregation.

### Data Reading


```python
gold_breweries_df = spark.read.format("delta").load("/dbfs/FileStore/project_breweries/silver/breweries_delta")
```

This section reads the cleaned and standardized data from the silver layer (stored as a Delta Lake table) into a Spark DataFrame.

### Aggregations for the Gold Layer

This section defines and calculates the key aggregations for the gold layer. Each aggregation addresses a specific analytical need.

#### 1. Count of Breweries by Type (`brewery_type`)


```python
gold_brewery_type = gold_breweries_df.groupBy("brewery_type").count() \
  .withColumnRenamed("count", "quantidade_cervejarias")

#gold_brewery_type.display() # Uncomment to display the results
```

This aggregation counts the number of breweries for each `brewery_type`. The resulting DataFrame `gold_brewery_type` provides a summary of the distribution of brewery types.

#### 2. Count of Breweries by Location (city, state, and country)


```python
gold_location = gold_breweries_df.groupBy("state", "country").count() \
  .withColumnRenamed("count", "quantidade_cervejarias")

#gold_location.display() # Uncomment to display the results
```

This aggregation counts the number of breweries for each combination of `state` and `country`. The `gold_location` DataFrame provides insights into the geographical distribution of breweries. Note: Including `city` in this aggregation might create too many granular groups, potentially making the analysis less insightful at this high level. If city-level aggregation is needed, it could be a separate aggregation.

#### 3. Count of Breweries by Type and Location


```python
gold_type_location = gold_breweries_df.groupBy("brewery_type", "state", "country").count() \
  .withColumnRenamed("count", "quantidade_cervejarias")

#gold_type_location.display() # Uncomment to display the results
```

This aggregation combines the previous two, counting the number of breweries for each combination of `brewery_type`, `state`, and `country`. The `gold_type_location` DataFrame allows for a more detailed analysis of brewery distribution, considering both type and location.

### Saving the Gold Layer in Delta Lake


```python
# Saving the aggregation by brewery type to the Gold layer
gold_brewery_type.write.format("delta").mode("overwrite").saveAsTable("gold_cervejarias_por_tipo")

# Saving the aggregation by location to the Gold layer
gold_location.write.format("delta").mode("overwrite").saveAsTable("gold_cervejarias_por_localizacao")

# Saving the combined aggregation (brewery type and location) to the Gold layer
gold_type_location.write.format("delta").mode("overwrite").saveAsTable("gold_cervejarias_tipo_localizacao")
```

This section saves the aggregated DataFrames as Delta Lake tables in the gold layer. Using `saveAsTable` makes these tables readily available for querying and analysis using SQL or other data exploration tools. The tables are named descriptively to reflect their contents. The `mode("overwrite")` ensures that the tables are updated with the latest data each time the notebook is run.

### Key Considerations and Potential Enhancements

- **Data Governance:** Consider adding metadata to the gold layer tables to document their purpose, data lineage, and any data quality metrics.
- **Performance Optimization:** For very large datasets, explore techniques like partitioning and bucketing to improve query performance on the gold layer tables.
- **Business Logic:** The aggregations in this notebook represent a starting point. Additional aggregations and calculations can be added based on specific business requirements. For example, calculating average brewery size, revenue, or other relevant metrics.
- **Data Visualization:** The gold layer data is ideal for creating dashboards and reports. Consider integrating with a data visualization tool to present the insights in a user-friendly way.
- **Scheduling:** This notebook should be scheduled to run regularly (e.g., daily or weekly) to keep the gold layer data up-to-date. This can be done using Databricks workflows or other scheduling tools.
- **Data Quality Checks:** Implement data quality checks in the gold layer to ensure the accuracy and reliability of the aggregated data. This might include checks for null values, data range checks, and comparisons with previous results.