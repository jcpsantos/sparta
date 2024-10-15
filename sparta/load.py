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
    """
    Writes a PySpark DataFrame to an Azure SQL Data Warehouse (DW) table.

    Args:
        df (DataFrame): The PySpark DataFrame to be written to Azure SQL DW.
        url_jdbc (str): The JDBC connection URL for the Azure SQL Data Warehouse.
        tempdir (str): The path to the temporary directory used during the write operation.
        table (str): The name of the table in Azure SQL DW where the DataFrame will be written.
        mode (str, optional): The mode for writing the data ('overwrite' or 'append'). Defaults to 'overwrite'.
        max_str_length (int, optional): The maximum string length for NVARCHAR columns. Defaults to 4000.

    Raises:
        ValueError: If the `mode` argument is not 'overwrite' or 'append'.
        Exception: If the DataFrame cannot be written to the Azure SQL DW due to connection or write errors.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.read.csv("data/sample.csv", header=True)
        >>> save_df_azure_dw(df, url_jdbc="jdbc:sqlserver://...", tempdir="/mnt/tmp", table="sales_data")
    
    In this example, a DataFrame is read from a CSV file and then written to the `sales_data` table in Azure SQL DW
    using the JDBC connection URL provided, with an overwrite mode.

    Logs:
        - Logs the start and completion of the write process.
        - Logs the total execution time.
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
    
def create_delta_table(df: DataFrame, spark: SparkSession, table: str, *grouping_columns: str) -> None:
    """
    Creates a Delta table in Hive using the provided DataFrame and optimizes it by ZORDER using the specified grouping columns.

    Args:
        df (DataFrame): The DataFrame to be written as a Delta table.
        spark (SparkSession): The Spark session used for creating the Delta table and running optimization commands.
        table (str): The name of the Delta table to be created in Hive.
        grouping_columns (str): Column names by which the table should be optimized using ZORDER. 
                                One or more column names can be passed as arguments.

    Returns:
        None: This function writes the DataFrame as a Delta table and optimizes it.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("Delta Table").getOrCreate()
        >>> df = spark.read.parquet("data/parquet_data")
        >>> create_delta_table(df, spark, table="sales_delta", "date", "customer_id")
    
    In this example, the function writes a DataFrame as a Delta table named `sales_delta` in Hive and optimizes it 
    using ZORDER by the `date` and `customer_id` columns.

    Logs:
        - Logs the successful creation of the Delta table.
        - Logs the execution time of the table creation and optimization process.
    """
    
    logger = getlogger('create_delta_table')
    
    start_time = time()
    
    if spark is None:
        spark = SparkSession.builder.master("local[*]").getOrCreate()
    
    df.write.format('delta').saveAsTable(table)
    # Assuming grouping_columns is a tuple or list of column names
    columns_str = ", ".join(grouping_columns)  
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({columns_str})")
    
    logger.info(f'Table {table} was successfully created in DeltaHive.')
    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")