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

    This function provides the recommended Spark settings for `--executor-cores`, `--executor-memory`, 
    and `--num-executors` based on cluster configuration.

    Args:
        number_of_nodes (int): The total number of nodes in the Spark cluster.
        cores_per_node (int): The number of CPU cores available on each node.
        total_memory_per_node (int): The total amount of memory available on each node (in GB).
        spark_executors_cores (int, optional): The number of cores to be allocated per Spark executor. Defaults to 5.
        memory_fraction (float, optional): The fraction of total memory per node to be allocated to each executor. 
                                           Defaults to 0.9 (i.e., 90%).

    Returns:
        dict: A dictionary containing the calculated Spark configuration with the following keys:
            - `--executor-cores`: The number of cores to allocate per executor.
            - `--executor-memory`: The amount of memory to allocate per executor (in GB).
            - `--num-executors`: The total number of executor instances.

    Raises:
        ValueError: If any of the input parameters are invalid (e.g., non-positive values or insufficient cores per executor).

    Example:
        >>> config = spark_property_calculator(
                number_of_nodes=10, 
                cores_per_node=16, 
                total_memory_per_node=128, 
                spark_executors_cores=4, 
                memory_fraction=0.8
            )
        >>> print(config)
        {
            '--executor-cores': 4,
            '--executor-memory': '25G',
            '--num-executors': 39
        }

    In this example, the function calculates the optimal Spark configuration for a cluster with 10 nodes,
    each having 16 cores and 128 GB of memory. Each executor is allocated 4 cores, and 80% of the available memory 
    is used per executor, resulting in 39 executors, each with 25 GB of memory and 4 cores.
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
