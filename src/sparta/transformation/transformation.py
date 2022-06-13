from pyspark.sql.window import Window
from pyspark.sql import DataFrame, functions as F

def drop_duplicates(df:DataFrame, col_order: str, cols_partition:list) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        col_order (str): _description_
        cols_partition (list): _description_

    Returns:
        DataFrame: _description_
    """
    win = Window.partitionBy(cols_partition).orderBy(F.col(col_order).desc())
    return df.withColumn("col_rank", F.row_number().over(win)).filter(F.col('col_rank') == 1).drop('col_rank')

def aggregation(df:DataFrame, col_order: str, cols_partition: list, aggregations:dict) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        col_order (str): _description_
        cols_partition (list): _description_
        aggregations (dict): _description_

    Returns:
        DataFrame: _description_
    """
    win = Window.partitionBy(cols_partition).orderBy(F.col(col_order).desc())
    for k in aggregations.keys():
        df = df.withColumn(aggregations.get(k), k(F.col(aggregations.get(k))).over(win))
    return df.withColumn("col_rank", F.row_number().over(win)).filter(F.col('col_rank') == 1).drop('col_rank')

def format_timestamp(df: DataFrame, cols: list, timestamp: str = '"yyyy-MM-dd HH:mm:ss"') -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        cols (list): _description_
        timestamp (_type_, optional): _description_. Defaults to '"yyyy-MM-dd HH:mm:ss"'.

    Returns:
        DataFrame: _description_
    """
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(F.date_format(F.col(c), timestamp), timestamp))
    return df