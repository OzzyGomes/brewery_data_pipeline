## Detailed Documentation for Functions Notebook - AB InBev Breweries Project

This document details the functions notebook for the AB InBev breweries project pipeline. This notebook contains reusable functions for data extraction, storage, and enrichment.

**Responsible Engineer:** Ozeas Gomes

**Creation Date:** 02/12/2025

**Last Updated:** 02/14/2025

### Objective

This notebook provides modular functions used across the AB InBev breweries project pipeline. These functions handle interactions with the Open Brewery DB API, data persistence to the bronze layer, and enriching data with latitude and longitude coordinates.

### Installing and Importing Dependencies

```python
from datetime import datetime
import requests
import json
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StructType, StructField, DoubleType
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
```

This section imports necessary Python libraries. `datetime` is used for timestamping files, `requests` for interacting with the API, `json` for handling JSON data, `pyspark.sql` for Spark DataFrame operations, `geopy` for geocoding addresses, and `time` for managing request timing.


