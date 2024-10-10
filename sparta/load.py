from pyspark.sql import DataFrame, SparkSession
from sparta.logs import getlogger
from time import time
from datetime import timedelta

def save_df_azure_dw(
    df: DataFrame,
    url_jdbc: str,
    tempdir: str,
    table: str,
    mode: str = 'overwrite',
    max_str_length: int = 4000
) -> None:  # sourcery skip: raise-specific-error
    """Write a PySpark DataFrame to an Azure SQL DW table.

    Args:
        df (DataFrame): The PySpark DataFrame to be written.
        url_jdbc (str): The JDBC connection URL for the Azure SQL DW.
        tempdir (str): Path for writing temporary files.
        table (str): The name of the table where the DataFrame will be written.
        mode (str, optional): The mode for writing the table (overwrite or append). Defaults to 'overwrite'.
        max_str_length (int, optional): The maximum string length for all NVARCHAR columns. Defaults to 4000.

    Raises:
        ValueError: If the mode parameter is not 'overwrite' or 'append'.
        Exception: If the DataFrame cannot be written to the Azure SQL DW.
    """
    logger = getlogger('save_df_azure_dw')
    logger.info(f'Start writing DataFrame to {table} in Azure SQL DW')
    
    if mode not in ['overwrite', 'append']:
        raise ValueError(f"mode '{mode}' not supported. Choose 'overwrite' or 'append'.")
    
    start_time = time()
    
    try:
        df.write.format('com.databricks.spark.sqldw') \
            .option('url', url_jdbc) \
            .option('dbTable', table) \
            .option('tempDir', tempdir) \
            .option('forwardSparkAzureStorageCredentials', 'true') \
            .option('maxStrLength', max_str_length) \
            .option('numPartitions', 32) \
            .option('batchsize', '100000') \
            .option('truncate', mode == 'overwrite') \
            .mode(mode) \
            .save()
        logger.info(f"Writing of DataFrame to {table} in Azure SQL DW is complete.")
        logger.info(f"Execution time: {timedelta(seconds=time() - start_time)}")
    except Exception as e:
        logger.error(f"Failed to write DataFrame to {table} in Azure SQL DW. Error: {str(e)}")
        raise Exception(f"Error writing DataFrame to {table} in Azure SQL DW: {str(e)}") from e


def create_hive_table(df: DataFrame, table: str, num_buckets: int, *grouping_columns: str) -> None:
    """Transform a DataFrame into a table in the Spark Warehouse.

    Args:
        df (DataFrame): The DataFrame to transform.
        table (str): The name of the table to create.
        num_buckets (int): The number of buckets to save the table.
        *grouping_columns (str): The names of the columns to group by.

    Returns:
        None.

    Example:
        >>> create_hive_table(df, "table_name", 5, "col1", "col2", "col3")
    """
    logger = getlogger('create_hive_table')
    
    start_time = time()
    
    df.write.format('parquet').bucketBy(num_buckets, grouping_columns).mode("overwrite").saveAsTable(table)
    logger.info(f'Table {table} was successfully created in Hive.')
    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")
    
def create_delta_table(df: DataFrame, table: str, *grouping_columns: str) -> None:
    logger = getlogger('create_delta_table')
    
    start_time = time()
    
    if spark is None:
        spark = SparkSession.builder.master("local[*]").getOrCreate()
    
    df.write.format('delta').saveAsTable(table)
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({grouping_columns})")
    
    logger.info(f'Table {table} was successfully created in DeltaHive.')
    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")
    
    