import logging
import sys
import math
from typing import Any, Dict

def getlogger(name:str, level:logging=logging.INFO) -> logging:
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


def spark_property_calculator(number_of_nodes: int, cores_per_node: int, total_memory_per_node: int, 
                              spark_executors_cores: int = 5, memory_fraction: float = 0.9) -> Dict[str, Any]:
    """
    Calculates the optimal Spark property configuration based on the number of nodes, cores per node, 
    and the total available memory per node.

    Args:
        number_of_nodes (int): Total number of nodes in the cluster.
        cores_per_node (int): Number of cores per node.
        total_memory_per_node (int): Total amount of memory per node in GB.
        spark_executors_cores (int, optional): Number of cores per executor. Default is 5.
        memory_fraction (float, optional): Fraction of the total memory to be used per executor. Default is 0.9 (90%).

    Returns:
        dict: A dictionary with the calculated Spark configuration, including --executor-cores, --executor-memory, and --num-executors.
        
    Example:
        >>> config = spark_property_calculator(6, 16, 64)
        >>> print(config)
    """
    
    # Input validation
    if number_of_nodes <= 0 or cores_per_node <= 1 or total_memory_per_node <= 1:
        raise ValueError("Number of nodes, cores per node, and memory per node must be greater than zero.")

    # Calculate the number of executors per node
    number_of_executors_per_node = (cores_per_node - 1) // spark_executors_cores
    if number_of_executors_per_node <= 0:
        raise ValueError(f"The cores per node ({cores_per_node}) are insufficient to support {spark_executors_cores} cores per executor.")
    
    # Calculate memory per executor
    memory_per_executor = (total_memory_per_node - 1) // number_of_executors_per_node
    spark_executor_memory = math.ceil(memory_per_executor * memory_fraction)
    
    # Calculate total number of executors
    spark_executor_instances = math.ceil(number_of_executors_per_node * number_of_nodes) - 1

    # Result
    result = {
        '--executor-cores': spark_executors_cores,
        '--executor-memory': f"{spark_executor_memory}G",
        '--num-executors': spark_executor_instances
    }

    return result