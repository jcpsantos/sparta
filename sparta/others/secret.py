import boto3
import json

def get_secret_aws(name:str, region: str="sa-east-1") -> dict:
    """Function to capture the data registered in Secret Manager.

    Args:
        name (str): Secret Manager name.
        region (str, optional): Region code where Secret Manager is located. Defaults to "sa-east-1".

    Returns:
        dict: Dictionary with the captured data
    
    Example:
        >>> get_secret_aws('Nome_Secret', 'sa-east-1')
    """
    secret_name = name
    region_name = region
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    return  json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])
