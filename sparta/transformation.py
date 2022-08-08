from typing import Any, List, Dict
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import *
from sparta.log import getlogger

def drop_duplicates(df:DataFrame, col_order: str, cols_partition:List[Any]) -> DataFrame:
    """Function that performs the deletion of duplicate data according to key columns.

    Args:
        df (DataFrame): DataFrame.
        col_order (str): Column to be used in sorting.
        cols_partition (list): List of key columns, for partitioning.

    Returns:
        DataFrame: DataFrame without the duplicate data, according to the key columns.
        
    Example:
        >>> cols = ['longitude','latitude']
            df = drop_duplicates(df, 'population', cols)
    """
    win = Window.partitionBy(cols_partition).orderBy(F.col(col_order).desc())
    return df.withColumn("col_rank", F.row_number().over(win)).filter(F.col('col_rank') == 1).drop('col_rank')

def aggregation(df:DataFrame, col_order: str, cols_partition: List[str], aggregations:Dict[Any, str]) -> DataFrame:
    """This function performs aggregations on columns.

    Args:
        df (DataFrame): DataFrame.
        col_order (str): Column to be used in sorting.
        cols_partition (list): List of key columns, for partitioning.
        aggregations (dict): A dictionary with the columns and the type of aggregation that will be used.

    Returns:
        DataFrame: DataFrame with aggregated columns.
       
    Example:
        >>> agg = {F.sum:'new_confirmed', F.first:'order_for_place'}
            cols = ['state', 'city']
            df = aggregation(df, 'date', cols, agg)
    """
    win = Window.partitionBy(cols_partition).orderBy(F.col(col_order).desc())
    for k in aggregations:
        df = df.withColumn(aggregations.get(k), k(F.col(aggregations.get(k))).over(win))
        logger = getlogger('aggregation')
        logger.debug(f'Performed {k} in column {aggregations.get(k)}')
    return df.withColumn("col_rank", F.row_number().over(win)).filter(F.col('col_rank') == 1).drop('col_rank')

def format_timestamp(df: DataFrame, cols: List[str], timestamp: str = '"yyyy-MM-dd HH:mm:ss"') -> DataFrame:
    """Function that performs a conversion from the date format to a pre-defined timestamp format.

    Args:
        df (DataFrame): DataFrame.
        cols (list): List of columns that will be converted.
        timestamp (_type_, optional): Timestamp format.. Defaults to '"yyyy-MM-dd HH:mm:ss"'.

    Returns:
        DataFrame: DataFrame with the columns converted to a predefined timestamp format.
        
    Example:
        >>> df = format_timestamp(df, ['date'])
    """
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(F.date_format(F.col(c), timestamp), timestamp))
        logger = getlogger('format_timestamp')
        logger.debug(f'Date formatting performed in column {c}')
    return df

def create_col_list (df: DataFrame, col: str) -> List[str]:
    """Function that creates a list with unique values from a column.

    Args:
        df (DataFrame): DataFrame.
        col (str): Column that will become a list.

    Returns:
        list: A list of unique values for a given DataFrame column.
        
    Example:
        >>> create_col_list(df, 'city')
    """
    return [str(value) for value in df.select(col).distinct().rdd.flatMap(lambda x:x).collect()]