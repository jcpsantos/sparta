import logging
import sys

def getlogger(name:str, level:logging=logging.INFO):
    """Function that generates custom logs.
    Args:
        name (str): Run name.
        level (logging, optional): Log level. Defaults to logging.INFO.
    Returns:
        logging: Custom log.
        
    Example:
        >>> logger = getlogger('test')
        >>> logger.info('test logs')
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger