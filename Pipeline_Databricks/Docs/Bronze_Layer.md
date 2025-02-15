## Detailed Documentation of the Bronze Layer Notebook - AB InBev Breweries Project

This document details the bronze layer notebook of the AB InBev breweries project pipeline. The bronze layer represents the initial stage of ingestion and storage of raw data, directly from the source (in this case, an API).

**Responsible Engineer:** Ozeas Gomes

**Creation Date:** 02/12/2025

**Last Updated:** 02/12/2025

### Objective

This project aims to create a pipeline for brewery data from the AB InBev Group. The bronze layer notebook is responsible for extracting data from the Open Brewery DB API and storing it in raw format, preparing it for the next stages of the pipeline.

### Connecting to the API

The connection to the Open Brewery DB API is done through the following steps:

#### Installing Required Dependencies

To maintain an organized and modular environment, two crucial functions (`fetch_breweries` and `save_to_bronze`) were created and stored in an external notebook. This external notebook promotes code reuse and facilitates maintenance. Access to these functions is done by running the following command:

Python

```
%run /Users/ozeasjgomes@gmail.com/brewery_data_pipeline/pipeline_functions
```

This command imports the functions defined in the `pipeline_functions` notebook, making them available for use in the bronze layer notebook.

#### Importing Dependencies

After running the external notebook, the `fetch_breweries` and `save_to_bronze` functions are available. There are no other dependencies explicitly imported in this notebook, as the main functionalities are encapsulated in the external functions.

#### Code

The main code of the bronze layer notebook consists of three steps:

1. **Definition of the Target URL:**

Python

```
base_url = 'https://api.openbrewerydb.org/breweries'
```

This line defines the base URL of the Open Brewery DB API, from which the data will be extracted.

2. **Data Extraction from the API:**

Python

```
api_data = fetch_breweries(base_url)
```

This line calls the `fetch_breweries` function, defined in the external notebook, passing the base URL as an argument. The `fetch_breweries` function is expected to make the request to the API, handle any connection errors, and return the raw API data in a suitable format (likely a dictionary or a JSON object).

1. **Saving Data to the Bronze Layer:**

Python

```
save_to_bronze(api_data, path='/dbfs/FileStore/project_breweries/bronze')
```

This line calls the `save_to_bronze` function, also defined in the external notebook, passing the raw API data (`api_data`) and the path to the bronze layer storage (`/dbfs/FileStore/project_breweries/bronze`) as arguments. The `save_to_bronze` function is expected to save the raw data to the specified location, possibly in Parquet, CSV, or another format suitable for raw data storage. The use of the `/dbfs` path suggests that this code is being executed in a Databricks environment.

### Next Steps

After executing the bronze layer notebook, the raw data will be stored and ready to be processed in the next layers of the pipeline (silver and gold). The next steps will involve cleaning, transforming, and aggregating the data to make it suitable for analysis and consumption.

### Observations

- It is crucial that the `pipeline_functions` notebook be documented separately, detailing the implementation of the `fetch_breweries` and `save_to_bronze` functions.
- The choice of storage format in the bronze layer (Parquet, CSV, etc.) should be justified and documented.
- Error handling (exceptions) in the connection to the API and in saving the data should be considered and implemented in the functions of the external notebook.
- The scalability of the extraction and storage process should be considered, especially for larger data volumes. Mechanisms such as API pagination and data partitioning may be necessary.