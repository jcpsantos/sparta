from pyspark.sql import DataFrame

def save_df_azure_dw(df: DataFrame, url_jdbc: str, tempdir: str, table: str, mode='overwrite', max_str_length=4000):
    """_summary_

    Args:
        df (DataFrame): _description_
        url_jdbc (str): _description_
        tempdir (str): _description_
        table (str): _description_
        mode (str, optional): _description_. Defaults to 'overwrite'.
        max_str_length (int, optional): _description_. Defaults to 4000.

    Raises:
        ValueError: _description_
    """

    if mode == 'overwrite':
        df.write.format('com.databricks.spark.sqldw').option('url', url_jdbc)\
            .option('forwardSparkAzureStorageCredentials', 'true').option('dbTable', table)\
                .option('maxStrLength', max_str_length)\
            .option("batchsize", "100000").option("numPartitions", 32).option("truncate", True)\
                .option("tempDir", tempdir).mode(mode).save()
    elif mode == 'append':
        df.write.format('com.databricks.spark.sqldw').option('url', url_jdbc)\
            .option('forwardSparkAzureStorageCredentials', 'true').option('dbTable', table)\
                .option('maxStrLength', max_str_length)\
            .option("batchsize", "100000").option("numPartitions", 32).option("tempDir", tempdir)\
                .mode(mode).save()
    else:
        raise ValueError(f"mode {mode} doesn't exist. Use overwrite or append.")


def create_hive_table(df: DataFrame, table: str, value: int,*keys):
    """
      Função para ler os arquivos e transformar em tabelas no Spark Warehouse.

      :param path: Caminho onde estar o arquivo que será lido.
      :type path: string
      :param table: O nome da tabela que será criada.
      :type table: string
      :param value: O número de buckets para salvar tabela.
      :type value: int
      :param *keys: Nomes das colunas que serão agrupadas.
      :type *keys: string
      Retorno:
          :return: None
          :type: None
      Exemplo:
          >>> create_table("/caminho/tabela/tabela1/", "tabela_nome", 5, "col1", "col2", "col3")
    """
    df.write.format('parquet').bucketBy(value, keys).mode("overwrite").saveAsTable(table)
