from sparta.logs import getlogger
from pyspark.sql import DataFrame

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
