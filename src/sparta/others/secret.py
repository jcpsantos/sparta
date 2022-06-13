import boto3
import json

def get_secret_aws(name:str, region: str="sa-east-1") -> dict:
    """
    Função para capturar os dados registrados no Secret Manager.
    Parameter:
    name = Nome do Secret Manager.
    region = Código da região onde se encontra o Secret Manager (Default: sa-east-1)
    Return:
        Dicinonário com os dados capturados
    Example:
        get_secret('Nome_Secret', 'sa-east-1')
    """
        
    secret_name = name
    region_name = region
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    return  json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])