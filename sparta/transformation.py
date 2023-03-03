from typing import Any, List, Dict
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import *
from sparta.logs import getlogger
from sparta.validator import validator_typed_columns, validator_dataframe_columns
from time import time
from datetime import timedelta

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
        >>> df = drop_duplicates(df, 'population', cols)
    """
    validator_dataframe_columns(df, cols_partition, 'Key columns for partitioning')
    validator_dataframe_columns(df, [col_order], 'Column for ordering')
    
    win = Window.partitionBy(cols_partition).orderBy(F.col(col_order).desc())
    return df.withColumn("col_rank", F.row_number().over(win)).filter(F.col('col_rank') == 1).drop('col_rank')


def aggregation(df:DataFrame, col_order: str, cols_partition: List[str], aggregations:Dict[Any, Any]) -> DataFrame:
    """This function performs aggregations on columns.

    Args:
        df (DataFrame): DataFrame.
        col_order (str): Column to be used in sorting.
        cols_partition (list): List of key columns, for partitioning.
        aggregations (dict): A dictionary with the columns and the type of aggregation that will be used. When the same type of aggregation occurs, a list with the column names must be passed.

    Returns:
        DataFrame: DataFrame with aggregated columns.
       
    Example:
        >>> agg = {F.sum:'new_confirmed', F.first:'order_for_place'}
        >>> cols = ['state', 'city']
        >>> df = aggregation(df, 'date', cols, agg)
        
    When the same type of aggregation occurs on different columns.
    
    Example:
        >>> agg = {F.sum:['new_confirmed','order_for_place']}
        >>> cols = ['state', 'city']
        >>> df = aggregation(df, 'date', cols, agg)
    """
    logger = getlogger('aggregation')

    start_time = time()

    win = Window.partitionBy(cols_partition).orderBy(F.col(col_order).desc())
    final_cols = cols_partition
    cols_partition = tuple(cols_partition)
    items = set()
    identificator = 1
    for k in aggregations:
        if type(aggregations.get(k)) == list:
            for item in aggregations.get(k):
                if item in items:
                    final_cols.append(k(F.col(item)).over(win).alias(f"{item}_{identificator}"))
                    logger.info(f'Performed {k} in column {item}')
                    identificator =+1
                else:
                    final_cols.append(k(F.col(item)).over(win).alias(item))
                    logger.info(f'Performed {k} in column {item}')
                    items.add(item)
        elif aggregations.get(k) in items:
            final_cols.append(k(F.col(aggregations.get(k))).over(win).alias(f"{aggregations.get(k)}_{identificator}"))
            logger.info(f'Performed {k} in column {aggregations.get(k)}')
            identificator =+1
        else:
            final_cols.append(k(F.col(aggregations.get(k))).over(win).alias(aggregations.get(k)))
            logger.info(f'Performed {k} in column {aggregations.get(k)}')
            items.add(aggregations.get(k))
    final_cols.append(F.monotonically_increasing_id().alias('id'))
    df = df.select(final_cols)
    win_drop = Window.partitionBy(list(cols_partition)).orderBy(F.col('id').desc())
    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")
    return df.select("*", F.row_number().over(win_drop).alias("col_rank")).filter(F.col('col_rank') == 1).drop('col_rank').drop('id')

def format_timestamp(df: DataFrame, cols: List[str], timestamp: str = '"yyyy-MM-dd HH:mm:ss"') -> DataFrame:
    """Function that performs a conversion from the date format to a pre-defined timestamp format.

    Args:
        df (DataFrame): DataFrame.
        cols (list): List of columns that will be converted.
        timestamp (_type_, optional): Timestamp format. Defaults to '"yyyy-MM-dd HH:mm:ss"'.

    Returns:
        DataFrame: DataFrame with the columns converted to a predefined timestamp format.
        
    Example:
        >>> df = format_timestamp(df, ['date'])
    """
    logger = getlogger('format_timestamp')
    
    start_time = time()
    
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(F.date_format(F.col(c), timestamp), timestamp))
        logger.info(f'Date formatting performed in column {c}')
    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")
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

def typed_columns(df:DataFrame, typecase:str = 'lower') -> DataFrame:
    """Function that transforms DataFrame columns into lowercase or uppercase.

    Args:
        df (DataFrame): DataFrame that will have the columns converted to lowercase or uppercase.
        typecase (str, optional): The column transformation type can be lower (lowercase) or upper (uppercase). Defaults to 'lower'.

    Raises:
        ValueError: If the value of the typecase argument is not upper or lower.

    Returns:
        DataFrame: DataFrame with columns converted to lowercase or uppercase.
        
    Example:
        >>> typed_columns(df, 'upper')
    """
    logger = getlogger('typed_columns')
    start_time = time()
    if validator_typed_columns(typecase) == 'Error':
        raise ValueError(f'This {typecase} is wrong. Typecase values must be lower or upper')
    if typecase.lower() == 'lower':
        df = df.toDF(*[c.lower() for c in df.columns])
    elif typecase.lower() == 'upper':
        df = df.toDF(*[c.upper() for c in df.columns])
    logger.info(f'Columns have been changed to {typecase.lower()}')
    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")
    return df