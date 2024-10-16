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
            spark = SparkSession.builder.master("local[*]").getOrCreate()
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
    Reads a YAML file and converts it into a Spark DataFrame.

    Args:
        path (str): The path of the YAML file to read.
        spark (SparkSession, optional): The Spark session to use for creating the DataFrame. 
                                        If not provided, a new local Spark session is created by default.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the YAML file.

    This function reads the YAML file at the specified `path`, converts it into a Python list, and then
    loads it into a Spark DataFrame. It also logs the conversion process. The function attempts to use 
    `CSafeLoader` for faster YAML parsing but defaults to `SafeLoader` if unavailable.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("YAML Reader").getOrCreate()
        >>> df = read_yaml_df("data/sample.yaml", spark)
        >>> df.show()

    In this example, the function reads a YAML file located at "data/sample.yaml" and converts it 
    into a Spark DataFrame using the provided Spark session.

    Raises:
        FileNotFoundError: If the YAML file at the specified path does not exist.
        ValueError: If the YAML file contains invalid data that cannot be converted to a DataFrame.
    """
    
    if spark is None:
        spark = SparkSession.builder.master("local[*]").getOrCreate()
        return read_with_schema(path, schema, options, format, spark)
        
    with open(path) as f:
        try:
            Loader = yaml.CSafeLoader
        except AttributeError:
            Loader = yaml.SafeLoader
        yaml_list = list(yaml.load_all(f, Loader=Loader))

    logger = getlogger('read_yaml_df')
    logger.info(f'YAML file {path} converted to a list.')

    return spark.createDataFrame(yaml_list)