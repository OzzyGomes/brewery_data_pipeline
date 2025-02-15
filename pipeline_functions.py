# Databricks notebook source
# MAGIC %md
# MAGIC #Functios Project Pipeline
# MAGIC Notebook with the main functions for the project <p>
# MAGIC
# MAGIC **Responsible Engineer: Ozeas Gomes <p>
# MAGIC Created on: 02/12/2025 <p>
# MAGIC Last updated: 02/14/2025 <p>**

# COMMAND ----------

# MAGIC %md
# MAGIC ####Installing Required Dependencies

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ####Importing Dependencies
# MAGIC

# COMMAND ----------





# COMMAND ----------

# MAGIC %md 
# MAGIC ### Function for the Bronze Notebook

# COMMAND ----------
from loguru import logger
import requests
import logging
from datetime import datetime
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("BronzePipeline").getOrCreate()
# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_breweries(base_url: str = 'https://api.openbrewerydb.org/breweries') -> list[dict]:
    """
        Extrai informações de cervejarias da API Open Brewery DB.

        Esta função itera pelas páginas da API, recuperando lotes de 50 cervejarias por página
        até que não haja mais páginas ou a API retorne um código de status diferente de 200.

        Parâmetros:
            base_url (str, optional): URL base da API Open Brewery DB.
                                            Padrão é 'https://api.openbrewerydb.org/breweries'.

        Retorna:
            list[dict]: Uma lista de dicionários, onde cada dicionário representa uma cervejaria.
                        Retorna uma lista vazia se ocorrer um erro ou nenhuma cervejaria for encontrada.

        Exemplo de uso:
            >>> breweries = fetch_breweries()
            >>> if breweries:
            >>>     print(f"Número de cervejarias extraídas: {len(breweries)}")
            >>> else:
            >>>     print("Falha ao extrair cervejarias da API.")

        Documentação da API de origem:
            https://www.openbrewerydb.org/documentation

        Exemplo de uso em um Databricks Notebook:
        >>> breweries_data = fetch_breweries()
    """
    logging.info(f"Função fetch_breweries iniciada em: {datetime.now()}")
    page = 1
    breweries = []
    while True:
        url = f"{base_url}?page={page}&per_page=50" #Building the Paginated URL
        logging.debug(f"Fazendo requisição GET para URL: {url}")
        try:
            response = requests.get(url) # GET Request to the API
            response.raise_for_status()  # Levanta um HTTPError para status de erro (e.g., 404, 500)
        except requests.exceptions.HTTPError as http_err:
            logging.warning(f"Erro HTTP ao requisitar {url}: {http_err}")
            break # Para o loop em caso de erro HTTP
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Erro ao requisitar {url}: {req_err}")
            break # Para o loop em caso de erro de requisição geral
        except Exception as e:
            logging.error(f"Erro inesperado ao requisitar {url}: {e}")
            break # Para o loop em caso de erro inesperado

        if response.status_code != 200: # Verificação redundante, já tratada pelo raise_for_status, mas mantida para clareza
            logging.warning(f"Status da requisição não foi 200 para URL: {url}, status code: {response.status_code}")
            break # Stops the Loop se o Status não for OK
        if not response.json(): # Checking the Content of the Response
            logging.warning(f"Resposta JSON vazia recebida para URL: {url}")
            break # Stops the Loop se a página estiver vazia
        current_breweries = response.json()
        breweries.extend(current_breweries) # Adds the Breweries from the Current Page to the List
        logging.debug(f"Extraídas {len(current_breweries)} cervejarias da página {page}")
        page += 1 # Increments the Page Number for the Next Iteration
    logging.info(f"Função fetch_breweries finalizada em: {datetime.now()}, Total de cervejarias extraídas: {len(breweries)}")
    return breweries

# COMMAND ----------

# api = get_breweries('https://api.openbrewerydb.org/breweries')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function for the Daily Execution Job
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Function for the Bronze Notebook

# COMMAND ----------

def save_to_bronze(data: list[dict], path: str = '/dbfs/FileStore/bronze') -> None:
    """
    Saves brewery data in JSON format to DBFS, following the Bronze layer architecture standards.

    This function receives a list of data (expected to be dictionaries representing breweries)
    and saves it as a Delta file at the specified DBFS path. The filename includes
    a timestamp to ensure uniqueness and traceability of ingested data in the Bronze layer.

    Parameters:
        data (list[dict]): Data to be saved. Expected to be a list of dictionaries representing breweries.
        path (str, optional): Full path in DBFS where the Delta file will be saved.
                              Default is '/dbfs/FileStore/bronze'. It is recommended to adjust this path
                              to match your directory structure in DBFS.

    Returns:
        None: This function does not explicitly return a value.
              On success, it logs an info message and on failure, it logs an error message.

    Important notes for the Databricks environment:
        - The default 'path' assumes you have write permissions in '/dbfs/FileStore/bronze'.
          Check permissions if you encounter write errors.
        - Consider adjusting the 'path' to a more appropriate location within your workspace
          or data lake in DBFS, following your project's naming and organizational conventions.
        - To verify if the file was saved correctly, use Databricks magic commands like '%fs ls <path>'
          in a notebook cell or navigate DBFS through the Databricks UI.

    Example usage in a Databricks Notebook:
        >>> save_to_bronze(breweries_data) # Saves to the default path /dbfs/FileStore/bronze
        >>> save_to_bronze(breweries_data, path="/dbfs/user/your_user/bronze_breweries") # Saves to a custom path
    """
    logging.info(f"Função save_to_bronze iniciada em: {datetime.now()}, salvando dados para o path: {path}")
    filename = f"breweries_{datetime.now().strftime('%Y%m%d%H%M')}" # Gera nome de arquivo com timestamp
    dbfs_file_path = f"{path}/{filename}" # Concatena o caminho e o nome do arquivo DBFS
    logging.debug(f"Caminho completo do arquivo DBFS: {dbfs_file_path}")

    try:
        raw_breweries_df = spark.createDataFrame(data)
        logging.debug("DataFrame criado a partir dos dados.")
        raw_breweries_df.write.format("delta").mode("overwrite").save(dbfs_file_path)
        logging.info(f"Dados salvos com sucesso no DBFS em: {dbfs_file_path}") # Mensagem de sucesso via log
    except Exception as e:
        logging.error(f"Erro ao salvar dados no DBFS em: {dbfs_file_path}: {e}") # Mensagem de erro com a exceção via log
        logging.error(f"Por favor, verifique se o caminho '{path}' está correto e se você tem permissões de escrita no DBFS.") # Instrução para verificação via log


# COMMAND ----------

# MAGIC %md
# MAGIC ####Função para o Job bronze
# MAGIC The only difference is that it already receives the data as a DataFrame.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame

def save_df_to_bronze(df: DataFrame, path: str = '/dbfs/FileStore/bronze') -> None:
    """
    Saves a PySpark DataFrame in Delta format to DBFS, following the Bronze layer architecture standards.

    This function receives a PySpark DataFrame and saves it as a Delta file at the specified DBFS path.  
    The filename includes a timestamp to ensure uniqueness and traceability of ingested data in the Bronze layer.

    Parameters:
        df (DataFrame): PySpark DataFrame to be saved.
        path (str, optional): Full path in DBFS where the Delta file will be saved.  
                              Default is '/dbfs/FileStore/bronze'. It is recommended to adjust this path  
                              to match your directory structure in DBFS.

    Returns:
        None: This function does not explicitly return a value.  
              On success, it prints a confirmation message to the console.  
              On failure, it prints an error message.

    Important notes for the Databricks environment:
        - The default 'path' assumes you have write permissions in '/dbfs/FileStore/bronze'.  
          Check permissions if you encounter write errors.
        - Consider adjusting the 'path' to a more appropriate location within your workspace  
          or data lake in DBFS, following your project's naming and organizational conventions.
        - To verify if the file was saved correctly, use Databricks magic commands like '%fs ls <path>'  
          in a notebook cell or navigate DBFS through the Databricks UI.

    Example usage in a Databricks Notebook:
        >>> save_to_bronze(df) # Saves to the default path /dbfs/FileStore/bronze
        >>> save_to_bronze(df, path="/dbfs/user/your_user/bronze_data") # Saves to a custom path
    """

    logging.info(f"Função save_to_bronze iniciada em: {datetime.now()}, salvando dados para o path: {path}")
    filename = f"breweries_{datetime.now().strftime('%Y%m%d%H%M')}" # Gera nome de arquivo com timestamp
    dbfs_file_path = f"{path}/{filename}" # Concatena o caminho e o nome do arquivo DBFS
    logging.debug(f"Caminho completo do arquivo DBFS: {dbfs_file_path}")

    try:
        
        logging.debug("DataFrame criado a partir dos dados.")
        DataFrame.write.format("delta").mode("overwrite").save(dbfs_file_path)
        logging.info(f"Dados salvos com sucesso no DBFS em: {dbfs_file_path}") # Mensagem de sucesso via log
    except Exception as e:
        logging.error(f"Erro ao salvar dados no DBFS em: {dbfs_file_path}: {e}") # Mensagem de erro com a exceção via log
        logging.error(f"Por favor, verifique se o caminho '{path}' está correto e se você tem permissões de escrita no DBFS.") # Instrução para verificação via log


# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to Get Latitude and Longitude
# MAGIC The missing latitude and longitude data is extensive, making it impossible to process with free tools like Geopy or Geocoding. Processing 2,325 rows would take a very long time. The ideal solution would be to use paid APIs such as Google Maps or OpenCage.

# COMMAND ----------


# COMMAND ----------

from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StructType, StructField, DoubleType
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Define a geocoding function that takes address data and returns a tuple (latitude, longitude)
def geocode_address(postal_code, street, city, state):
    """
    Geocodes an address using Nominatim and returns a tuple (latitude, longitude).

    Parameters:
        postal_code (str): Postal code of the address.
        street (str): Street name.
        city (str): City name.
        state (str): State name.

    Returns:
        tuple: (latitude, longitude) if successful, otherwise (None, None).
    """
    geolocator = Nominatim(user_agent="geo_app")
    address = f"{street}, {city}, {state}, {postal_code}"
    
    try:
        location = geolocator.geocode(address, timeout=10)  # Set timeout for robustness
        time.sleep(1)  # Prevent exceeding request limits
        if location:
            return location.latitude, location.longitude
    except (GeocoderTimedOut, GeocoderServiceError):
        return None, None
    
    return None, None

# Define the UDF return schema (two columns: latitude and longitude)
schema = StructType([
    StructField("latitude_new", DoubleType(), True),
    StructField("longitude_new", DoubleType(), True)
])

# Register the function as a UDF
geocode_udf = udf(geocode_address, schema)

# Function to fill missing latitude and longitude values in the DataFrame
def fill_missing_lat_long(df):
    """
    Fills missing latitude and longitude values in a DataFrame using geocoding.

    For each row in the DataFrame, if latitude or longitude is missing,
    it uses the address (postal code, street, city, state) to obtain coordinates.

    Parameters:
        df (DataFrame): Input DataFrame with address details.

    Returns:
        DataFrame: DataFrame with missing latitude and longitude values filled.
    """
    df = df.withColumn("new_coords", geocode_udf("postal_code", "street", "city", "state"))

    df = df.withColumn(
            "latitude",
            when(col("latitude").isNull(), col("new_coords.latitude_new")).otherwise(col("latitude"))
         ).withColumn(
            "longitude",
            when(col("longitude").isNull(), col("new_coords.longitude_new")).otherwise(col("longitude"))
         ).drop("new_coords")

    return df

