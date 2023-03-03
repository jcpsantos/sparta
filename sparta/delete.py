from azure.storage.blob import ContainerClient
from sparta.logs import getlogger
from time import time
from datetime import timedelta

def delete_blob(dir_name: str, container_name: str, connect: str) -> None:
    """Function to delete files in a blob.
    Args:
        dir_name (str): Directory name.
        container_name (str): Container name.
        connect (str): Blob connection string.
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