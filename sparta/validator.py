from sparta.logs import getlogger, send_error_to_teams
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, max
from datetime import datetime, timedelta
from typing import Any, Dict, List

def validator_typed_columns(typecase:str) -> str:
    """
    Validates the `typecase` argument to check if it is 'upper' or 'lower', 
    which are accepted values for converting column case in a DataFrame.

    Args:
        typecase (str): The case type to be validated. Accepted values are 'upper' or 'lower'.

    Returns:
        str: 'Ok' if the validation is successful. If the value is valid but improperly cased, 
             a warning is logged and 'Ok' is returned. If invalid, 'Error' is returned.

    Example:
        >>> validator_typed_columns('upper')
        'Ok'

        >>> validator_typed_columns('UPPER')
        # Logs a warning about case sensitivity
        'Ok'

        >>> validator_typed_columns('capitalize')
        'Error'
    """
    logger = getlogger('validator_typed_columns')
    if typecase in {'upper', 'lower'}:
        return 'Ok'
    elif typecase.lower() in {'upper', 'lower'}:
        logger.warning("Note that the values must be passed in lowercase. Example: upper or lower.")
        return 'Ok'
    else:
        return "Error"
    
def validator_dataframe_columns(df: DataFrame, columns: List[Any], log: str = 'validator_dataframe_columns') -> None:
    """
    Validates whether the specified columns exist in the given DataFrame. 
    It supports both string column names and PySpark's `F.col()` objects.

    Args:
        df (DataFrame): The DataFrame in which the columns will be checked.
        columns (List[Any]): A list of column names to be validated. Can be strings or `F.col()` objects.
        log (str, optional): The logger identifier. Defaults to 'validator_dataframe_columns'.

    Raises:
        ValueError: If one or more of the specified columns do not exist in the DataFrame.
        TypeError: If an invalid column type (neither string nor `F.col()` object) is provided.

    Example:
        >>> from pyspark.sql import functions as F
        >>> df = spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'value'])
        >>> validator_dataframe_columns(df, ['id', 'value'])
        # Logs a success message since both columns exist.

        >>> validator_dataframe_columns(df, [F.col('id'), 'value'])
        # Works with both string and F.col objects.

        >>> validator_dataframe_columns(df, ['non_existing_col'])
        # Raises a ValueError: "The columns {'non_existing_col'} do not exist in the dataframe."
    """
    logger = getlogger(log)
    
    # Convert columns to strings if they are Column objects
    column_names = []
   
    for col in columns:
        if isinstance(col, Column):
            try:
                # Attempt to extract column name from F.col()
                col_name = col._jc.toString().split('.')[-1]
                column_names.append(col_name)
            except AttributeError:
                # Catch the error if col is not a valid F.col()
                raise TypeError(f"Invalid column type: {col}. Make sure to pass the correct F.col() object.")
        elif isinstance(col, str):
            column_names.append(col)
        else:
            raise TypeError(f"Invalid column type: {type(col)}. Only strings or F.col() objects are accepted.")
    
    # Convert both column names from DataFrame and input to lowercase for case-insensitive comparison
    df_columns_lower = [c.lower() for c in df.columns]  # All DataFrame column names to lowercase
    column_names_lower = [c.lower() for c in column_names]
    
    # Check if the columns exist in the DataFrame (case-insensitive)
    missing_columns = set(column_names_lower).difference(set(df_columns_lower))
    
    if missing_columns:
        # Raise a clear error if columns are missing
        raise ValueError(f"The columns {missing_columns} do not exist in the dataframe. Please verify the column names.")
    else:
        logger.info(f'The columns {column_names} exist in the dataframe, validation successful!')
   
def validate_column_types(df:DataFrame, expected_columns:Dict[Any, Any]) -> bool:
    """
    Validates that a DataFrame has the expected column names and data types.

    This function checks if the given DataFrame contains all the expected columns and whether
    their data types match the expected types. If any column is missing or has a different
    data type than expected, it logs an error message and returns `False`. If all columns
    match the expectations, it returns `True`.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        expected_columns (dict): A dictionary where the keys are the expected column names 
                                 and the values are the expected data types (as strings).

    Returns:
        bool: `True` if the DataFrame has all the expected columns with the correct data types, 
              `False` otherwise.

    Raises:
        None: This function does not raise exceptions, but logs errors using the logger
        in case of validation failure.

    Example:
        >>> expected_columns = {
            'name': 'string',
            'age': 'int',
            'salary': 'double'
        }
        >>> validate_column_types(df, expected_columns)
    """
    logger = getlogger('validate_column_types')
    actual_columns = dict(df.dtypes)
    for column, dtype in expected_columns.items():
        if column not in actual_columns:
            logger.error(f"Column {column} does not exist in the dataframe.")
            return False
        if actual_columns[column] != dtype:
            logger.error(f"Column {column} has type {actual_columns[column]} but expected {dtype}.")
            return False
    return True

def compare_dataframes(df1: DataFrame, df2: DataFrame) -> bool:
    """
    Compares two PySpark DataFrames to check if they are different in terms of schema and content.
    
    Args:
    - df1: The first DataFrame.
    - df2: The second DataFrame.
    
    Return:
    - bool: False if the DataFrames are different, True otherwise.
    """
    
    logger = getlogger('compare_dataframes')
    # 1. Compare the schemas (columns and data types)
    if df1.schema != df2.schema:
        logger.info("The DataFrames have different schemas.")
        return False

    # 2. Compare the content row by row (independent of ordering)
    df1_sorted = df1.sort(df1.columns)
    df2_sorted = df2.sort(df2.columns)
    
    # Compare content row by row
    diff_count = df1_sorted.subtract(df2_sorted).count() + df2_sorted.subtract(df1_sorted).count()
    
    if diff_count > 0:
        logger.info(f"The DataFrames have {diff_count} differences in content.")
        return False

    logger.info("The DataFrames are identical.")
    return True

def monitoring_datetime(df, name, column_name, final_hour, webhook_url):
  """
    Monitors the most recent timestamp in a specified column of a DataFrame 
    and sends an alert via Microsoft Teams if the data is outdated.

    This function calculates the maximum value of a timestamp column in a given DataFrame,
    compares it with a threshold (current time minus a specified number of hours), and sends
    a notification to Microsoft Teams if the data has not been updated within the threshold.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame containing the data to monitor.
        name (str): The name of the process being monitored, used in the alert message.
        column_name (str): The name of the timestamp column to monitor.
        final_hour (int): The threshold in hours; data older than this will trigger an alert.
        webhook_url (str): The Microsoft Teams webhook URL for sending alerts.

    Raises:
        ValueError: If the specified column does not exist in the DataFrame.

    Side Effects:
        - Sends a notification to Microsoft Teams using the provided webhook URL.
        - Logs an informational message if an alert is triggered.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.functions import lit
        >>> spark = SparkSession.builder.getOrCreate()
        >>> df = spark.createDataFrame([(1, "2024-12-01 10:00:00")], ["id", "timestamp"])
        >>> df = df.withColumn("timestamp", lit("2024-12-01 10:00:00").cast("timestamp"))
        >>> monitoring_datetime(df, "MyProcess", "timestamp", 24, "https://example.webhook.url")
  """
  logger = getlogger('monitoring_datetime')
  validator_dataframe_columns(df, [column_name])
  max_value = df.agg(max(col(column_name).cast('timestamp')).alias("max_value")).collect()[0]["max_value"]
  data = (datetime.now() - timedelta(hours=final_hour))
  if data > max_value:
    message = f'Process {name} has not updated in the last {final_hour} hours. Last update: {max_value}.'
    message_json = {
            "type": "message",
            "attachments": [
                {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [
                    {
                        "type": "TextBlock",
                        "text": "📊 *Monitoring Notification*",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": "Job Monitoring",
                        "weight": "Bolder"
                    },
                    {
                        "type": "TextBlock",
                        "text": message,
                        "wrap": True
                    }
                    ],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4"
                }
                }
            ]
            }
    send_error_to_teams(message_json, webhook_url)
    logger.info(message)