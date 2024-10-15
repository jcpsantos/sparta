from sparta.logs import getlogger
from pyspark.sql import DataFrame, Column
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