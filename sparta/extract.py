from typing import Dict
from pyspark.sql import SparkSession, DataFrame
import yaml
from smart_open import open

spark = SparkSession.builder.master("local[*]").getOrCreate()

def read_with_schema(path: str, schema: str, options:Dict[str, str] = None, format: str = 'csv', spark: SparkSession = spark) -> DataFrame:
    """Function to read DataFrames with predefined schema.

    Args:
        path (str): Path where the file is located.
        schema (str): Pre-defined schema for reading.
        options (dict): Configuration options for reading the DataFrame.
        format (str, optional): Format of the file to be read. Defaults to 'parquet'.
        spark (SparkSession, optional): Spark session. Defaults to spark.

    Returns:
        DataFrame: DataFrame read with predefined schema.
        
    Example:
        >>> schema = 'epidemiological_week LONG, date DATE, order_for_place INT, state STRING, city STRING, city_ibge_code LONG, place_type STRING, last_available_confirmed INT'
            path = '/content/sample_data/covid19-e0534be4ad17411e81305aba2d9194d9.csv'
            df = read_with_schema(path, schema, {'header': 'true'}, 'csv')
    """
    return spark.read.format(format).schema(schema).options(**options).load(path)


def read_yaml_df(path:str, spark: SparkSession = spark) -> DataFrame:
    """Function to read a yaml file as a DataFrame.

    Args:
        path (str): Path of the yaml file.
        spark (SparkSession, optional): Spark session. Defaults to spark.

    Returns:
        DataFrame: Yaml file read and converted to a DataFrame.
        
    Example:
        >>> path = '/content/sample_data/schema_ingestao.yaml'
            df = read_yaml_df(path)
    """
    with open(path) as f:
      try:
          Loader = yaml.CSafeLoader
      except AttributeError:  
          Loader = yaml.SafeLoader
      yaml_dict = list(yaml.load_all(f, Loader=Loader))

    return spark.createDataFrame(yaml_dict)