from sparta.logs import getlogger

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