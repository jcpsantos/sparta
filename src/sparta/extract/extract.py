from pyspark.sql import SparkSession, DataFrame
import yaml
from smart_open import open

spark = SparkSession.builder.master("local[*]").getOrCreate()

def read_with_schema(path: str, schema: str, options:dict, format: str = 'parquet', spark: SparkSession = spark) -> DataFrame:
    """_summary_

    Args:
        path (str): _description_
        schema (str): _description_
        options (dict): _description_
        format (str, optional): _description_. Defaults to 'parquet'.
        spark (SparkSession, optional): _description_. Defaults to spark.

    Returns:
        DataFrame: _description_
    """
    return spark.read.format(format).schema(schema).options(**options).load(path)


def read_yaml_df(path:str, spark: SparkSession = spark) -> DataFrame:
    """_summary_

    Args:
        path (str): _description_
        spark (SparkSession, optional): _description_. Defaults to spark.

    Returns:
        DataFrame: _description_
    """
    with open(path) as f:
      try:
          Loader = yaml.CSafeLoader
      except AttributeError:  
          Loader = yaml.SafeLoader
      yaml_dict = list(yaml.load_all(f, Loader=Loader))

    return spark.createDataFrame(yaml_dict)