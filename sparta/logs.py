import logging
import sys
import math
import requests
import json
import traceback
from typing import Any, Dict
from contextlib import contextmanager

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

# Function to send error messages to Microsoft Teams via webhook
def send_error_to_teams(error_message:json, webhook_url:str) -> None:
    """
    Sends error messages to a Microsoft Teams channel using a webhook.

    This function formats and sends a given error message to a specified Microsoft Teams channel 
    through its webhook URL. It logs the response from the Teams API and handles unsuccessful requests 
    by logging an error message.

    Args:
        error_message (json): A JSON object containing the error message payload to be sent. 
                              Ensure it follows the correct schema for Microsoft Teams messages.
        webhook_url (str): The webhook URL for the target Microsoft Teams channel.

    Example:
        >>> error_payload = {
        >>>     "text": "🚨 An error occurred during the process.",
        >>>     "attachments": [
        >>>         {
        >>>             "contentType": "application/vnd.microsoft.card.adaptive",
        >>>             "content": {
        >>>                 "type": "AdaptiveCard",
        >>>                 "body": [
        >>>                     {"type": "TextBlock", "text": "Error details", "weight": "Bolder"},
        >>>                     {"type": "TextBlock", "text": "An unexpected error occurred.", "wrap": True}
        >>>                 ],
        >>>                 "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        >>>                 "version": "1.4"
        >>>             }
        >>>         }
        >>>     ]
        >>> }
        >>> send_error_to_teams(error_payload, "https://your-webhook-url.com")

    Raises:
        None: This function does not raise exceptions directly. It logs errors for unsuccessful HTTP requests.
    """
    logger = getlogger('handle_exceptions')
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(webhook_url, json=error_message, headers=headers)
    if response.status_code != 200:
        logger.error(f"Failed to send message to Teams: {response.status_code}, {response.text}")
        

# Context manager for handling exceptions and sending them to Teams
@contextmanager
def handle_exceptions(process:str, notebook_url: str, webhook_url:str) -> Any:
    """
    Context manager for handling exceptions and notifying Microsoft Teams.

    This context manager wraps a block of code and captures any exceptions raised during its execution. 
    If an exception occurs, it logs the error details and sends a formatted error message to a Microsoft Teams 
    channel using the specified webhook. The error message includes the process name, error details, 
    and a link to the relevant Databricks notebook.

    Args:
        process (str): The name of the process being executed. This is included in the error message for context.
        notebook_url (str): The URL of the Databricks notebook where the process is running. This URL is included 
                            in the error message as an actionable link.
        webhook_url (str): The webhook URL for the Microsoft Teams channel to receive the error notification.

    Example:
        Use the context manager as follows:
        
        >>> with handle_exceptions('Data Processing Job', 'https://databricks.com/job/123', 'https://your-webhook-url.com'):
        >>>     # Code block that may raise exceptions
        >>>     process_data()

    How it works:
        - Captures any exception raised within the `with` block.
        - Logs the error details including the exception type, message, and traceback.
        - Sends a notification to Microsoft Teams using an Adaptive Card.
        - Re-raises the exception to allow further handling or stop execution.

    Notes:
        - The error message sent to Teams is truncated to 450 characters to ensure compatibility with Teams' rendering.
        - Ensure that the `send_error_to_teams` function is properly configured to send the payload to Teams.

    Raises:
        Any exception raised within the `with` block is re-raised after being logged and sent to Teams.
    """
    logger = getlogger('handle_exceptions')
    try:
        yield
    except Exception as e:
        error_message = f"Exception: {type(e).__name__}, {str(e)}\nTraceback: {traceback.format_exc()}"
        logger.error(f"Error occurred: {error_message}")
        
        truncated_error_message = (
            error_message[:450] + "[...]" if len(error_message) > 450 else error_message
        )
        error_message_json = {
            "type": "message",
            "attachments": [
                {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [
                    {
                        "type": "TextBlock",
                        "text": "🚨 *Error in Databricks!*",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": "Error details:",
                        "weight": "Bolder"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"**Process**: {process}\n**Error**: {truncated_error_message}",
                        "wrap": True
                    },
                    {
                        "type": "TextBlock",
                        "text": "Check the log for more details.",
                        "wrap": True
                    }
                    ],
                    "actions": [
                    {
                        "type": "Action.OpenUrl",
                        "title": "Open Job in Databricks",
                        "url": notebook_url
                    }
                    ],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4"
                }
                }
            ]
            }
        send_error_to_teams(error_message_json, webhook_url)
        raise  # Re-raise the exception to stop execution