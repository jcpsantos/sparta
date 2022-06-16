from pyspark.sql import DataFrame

def save_df_azure_dw(df: DataFrame, url_jdbc: str, tempdir: str, table: str, mode:str ='overwrite', max_str_length:int =4000):
    """Function to write to an Azure SQL DW.

    Args:
        df (DataFrame): DataFrame that will be written.
        url_jdbc (str): JDBC connection URL.
        tempdir (str): Path for writing temporary file.
        table (str): Name of the table where the DataFrame will be written in the Database.
        mode (str, optional): Write mode, can be append or overwrite.. Defaults to 'overwrite'.
        max_str_length (int, optional): Set string length for all NVARCHAR. Defaults to 4000.

    Raises:
        ValueError: In case the mode values are not -> append or overwrite.
    """

    if mode == 'overwrite':
        df.write.format('com.databricks.spark.sqldw').option('url', url_jdbc)\
            .option('forwardSparkAzureStorageCredentials', 'true').option('dbTable', table)\
                .option('maxStrLength', max_str_length)\
            .option("batchsize", "100000").option("numPartitions", 32).option("truncate", True)\
                .option("tempDir", tempdir).mode(mode).save()
    elif mode == 'append':
        df.write.format('com.databricks.spark.sqldw').option('url', url_jdbc)\
            .option('forwardSparkAzureStorageCredentials', 'true').option('dbTable', table)\
                .option('maxStrLength', max_str_length)\
            .option("batchsize", "100000").option("numPartitions", 32).option("tempDir", tempdir)\
                .mode(mode).save()
    else:
        raise ValueError(f"mode {mode} doesn't exist. Use overwrite or append.")


def create_hive_table(df: DataFrame, table: str, value: int,*keys:str):
    """Function to transform DataFrame into tables in Spark Warehouse.

    Args:
        df (DataFrame): DataFrame that will be transformed
        table (str): The name of the table to be created.
        value (int): The number of buckets to save table.
        *keys (str): Names of the columns to be grouped.
        
    Example:
        >>> create_hive_table(df, "table_name", 5, "col1", "col2", "col3")
    """
    df.write.format('parquet').bucketBy(value, keys).mode("overwrite").saveAsTable(table)