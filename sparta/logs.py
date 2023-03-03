import logging
import os

def getlogger(name: str, level: int = logging.INFO, 
              format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
              file: str = None) -> logging.Logger:
    """Function that generates custom logs.

    Args:
        name (str): Run name.
        level (int, optional): Log level. Defaults to logging.INFO.
        format (str, optional): Log format. Defaults to '%(asctime)s - %(name)s - %(levelname)s - %(message)s'.
        file (str, optional): Log file location. Defaults to None.

    Returns:
        logging.Logger: Custom log.

    Example:
        >>> logger = getlogger('test')
        >>> logger.info('test logs')
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(format)

    # Create a console handler that prints log messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create a file handler that logs messages to a file
    if file is not None:
        os.makedirs(os.path.dirname(file), exist_ok=True)
        file_handler = logging.FileHandler(file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
