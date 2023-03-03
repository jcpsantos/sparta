from typing import Any, Dict
from pyspark.sql import SparkSession, DataFrame
import yaml
from smart_open import open
from sparta.logs import getlogger
from pyspark.sql.types import _parse_datatype_string

def read_with_schema(path: str, schema: str, options: Dict[Any, Any] = None, format: str = 'csv', spark: SparkSession = None) -> DataFrame:
    """Function to read DataFrames with predefined schema.
    Args:
        path (str): Path where the file is located.
        schema (str): Pre-defined schema for reading.
        options (dict): Configuration options for reading the DataFrame.
        format (str, optional): Format of the file to be read. Defaults to 'csv'.
        spark (SparkSession, optional): Spark session. Defaults to None.
    Returns:
        DataFrame: DataFrame read with predefined schema.

    Example:
        >>> schema = 'epidemiological_week LONG, date DATE, order_for_place INT, state STRING, city STRING, city_ibge_code LONG, place_type STRING, last_available_confirmed INT'
        >>> path = '/content/sample_data/covid19-e0534be4ad17411e81305aba2d9194d9.csv'
        >>> df = read_with_schema(path, schema, {'header': 'true'}, 'csv')
    """
    if options is None:
        options = {}

    if spark is None:
        with SparkSession.builder.master("local[*]").getOrCreate() as spark:
            return read_with_schema(path, schema, options, format, spark)

    try:
        ddl_schema = _parse_datatype_string(schema)
        logger = getlogger('read_with_schema')
        logger.info(f'Schema created -> {ddl_schema}')

        return spark.read.format(format).schema(ddl_schema).options(**options).load(path)
    except Exception as e:
        logger.error(f'Error reading DataFrame with schema. Error message: {str(e)}')
        raise e


def read_yaml_df(path: str, spark: SparkSession = None) -> DataFrame:
    """
    Read a YAML file as a Spark DataFrame.

    Args:
        path (str): Path of the YAML file.
        spark (SparkSession, optional): Spark session to use. Defaults to spark.

    Returns:
        DataFrame: YAML file converted to a DataFrame.
    """
    
    if spark is None:
        with SparkSession.builder.master("local[*]").getOrCreate() as spark:
            return read_yaml_df(path, spark)
        
    with open(path) as f:
        try:
            Loader = yaml.CSafeLoader
        except AttributeError:
            Loader = yaml.SafeLoader
        yaml_list = list(yaml.load_all(f, Loader=Loader))

    logger = getlogger('read_yaml_df')
    logger.info(f'YAML file {path} converted to a list.')

    return spark.createDataFrame(yaml_list)