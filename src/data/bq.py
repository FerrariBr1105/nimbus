import pandas as pd
import numpy as np
from google.cloud import bigquery
from pandas_gbq import read_gbq

def exporta_dados(df, table_id):

    # Configurar o job de carregamento
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )

    # Carregar o DataFrame para a tabela do BQ
    job = client.load_table_from_dataframe(
        df, 
        table_id, 
        job_config=job_config
    )

    # Esperar o job ser concluído
    job.result()

    print(f"Carregamento para a tabela {table_id} concluído com sucesso.")


def importa_dados(query):
    # Leia a tabela para um DataFrame
    df = read_gbq(
        query, 
        project_id='alert-being-435923-n9', 
        dialect='standard'
    )
    return df