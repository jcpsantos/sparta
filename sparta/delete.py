from azure.storage.blob import ContainerClient
from sparta.logs import getlogger
from time import time
from datetime import timedelta

def delete_blob(dir_name: str, container_name: str, connect: str) -> None:
    """
    Function to delete all files in a specified directory within a blob storage container.

    Args:
        dir_name (str): Directory name or blob prefix to delete files from.
        container_name (str): Name of the Azure Blob Storage container.
        connect (str): Azure Blob Storage connection string.

    This function connects to the specified blob container using the provided connection string 
    and deletes all blobs (files) that are located within the directory specified by `dir_name`.

    Example:
        >>> delete_blob("data/archive/", "my-container", "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net")

    This example deletes all blobs that start with "data/archive/" in the container "my-container".
    It logs the deletion success or failure for each blob found.

    Raises:
        Any exception raised during deletion is logged as an error.
    """
    logger = getlogger('delete_blob')
    start_time = time()

    with ContainerClient.from_connection_string(conn_str=connect, container_name=container_name) as container_client:
        for blob in container_client.list_blobs(name_starts_with=dir_name):
            if container_client.get_blob_client(blob.name).exists():
                try:
                    container_client.delete_blob(blob.name)
                    logger.info(f"Deleted {blob.name}")
                    logger.info(f"Execution time: {timedelta(seconds = time()-start_time)}")
                except Exception:
                    logger.error(f"Failed to delete {blob.name}")
            else:
                logger.warning(f"{blob.name} not found")
