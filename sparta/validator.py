from sparta.logs import getlogger
from pyspark.sql import DataFrame
from typing import Any, Dict

def validator_typed_columns(typecase:str) -> str:
    """Function to validate the value received in the typecase argument of the typed_columns function.

    Args:
        typecase (str): The value of the typecase argument received in the typed_columns function.

    Returns:
        str: Validation indicating if the argument is ok or if it contains an error.
    """
    logger = getlogger('validator_typed_columns')
    if typecase in {'upper', 'lower'}:
        return 'Ok'
    elif typecase.lower() in {'upper', 'lower'}:
        logger.warning("Note that the values must be passed in lowercase. Example: upper or lower.")
        return 'Ok'
    else:
        return "Error"
    
def validator_dataframe_columns(df:DataFrame, columns:list, log:str='validator_dataframe_columns') -> None:
    """Function to validate if the reported columns really exist in the dataframe.

    Args:
        df (DataFrame): The dataframe to be checked.
        columns (list): List with the name of the columns that will be checked in the dataframe.
        log (str): Information that will be recorded in the validator log.
    """
    logger = getlogger(log)
    if all(item in df.columns for item in columns):
        logger.info(f'The columns {columns} exist in the dataframe, validation successful!')
    else:
        raise ValueError(f"The columns'{set(columns).difference(set(df.columns))}' do not exist in the dataframe. Please verify.")
    
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