from pyspark.sql import functions as F, DataFrame

def save_df_azure_dw(df: DataFrame, url_jdbc: str, tempdir: str, table: str, mode='overwrite', maxStrLength=4000):
    """_summary_

    Args:
        df (DataFrame): _description_
        url_jdbc (str): _description_
        tempdir (str): _description_
        table (str): _description_
        mode (str, optional): _description_. Defaults to 'overwrite'.
        maxStrLength (int, optional): _description_. Defaults to 4000.

    Raises:
        ValueError: _description_
    """

    if mode == 'overwrite':
        df.write.format('com.databricks.spark.sqldw').option('url', url_jdbc).option('forwardSparkAzureStorageCredentials', 'true').option('dbTable', table).option('maxStrLength', maxStrLength)\
            .option("batchsize", "100000").option("numPartitions", 32).option("truncate", True).option("tempDir", tempdir).mode(mode).save()
    elif mode == 'append':
        df.write.format('com.databricks.spark.sqldw').option('url', url_jdbc).option('forwardSparkAzureStorageCredentials', 'true').option('dbTable', table).option('maxStrLength', maxStrLength)\
            .option("batchsize", "100000").option("numPartitions", 32).option("tempDir", tempdir).mode(mode).save()
    else:
        raise ValueError(f"mode {mode} doesn't exist. Use overwrite or append.")

