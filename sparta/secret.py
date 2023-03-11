from typing import Any, Dict
import boto3
import json

def get_secret_aws(name: str, region: str = "sa-east-1") -> Dict[str, Any]:
    """
    Function to retrieve data stored in AWS Secrets Manager.

    Args:
        name (str): The name of the secret to retrieve.
        region (str, optional): The name of the region where the secret is stored. Defaults to 'sa-east-1'.

    Returns:
        dict: A dictionary with the retrieved data.

    Example:
        >>> secret_data = get_secret_aws('my_secret', 'us-west-2')
    """
    secret_name = name
    region_name = region

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    return json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])
