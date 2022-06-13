from azure.storage.blob import ContainerClient

def delete_blob(dir_name:str, container_name:str, connect:str):
    """_summary_

    Args:
        dir_name (str): _description_
        container_name (str): _description_
        connect (str): _description_
    """
    container_client = ContainerClient.from_connection_string(conn_str=connect, container_name=container_name)
    for blob in container_client.list_blobs(name_starts_with=dir_name):
        try:
            container_client.delete_blob(blob.name)
        except Exception:
            print(f"{blob.name} not found")