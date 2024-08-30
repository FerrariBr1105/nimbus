import os
import zipfile
import pandas as pd

def descompacta_zip(dir_zip, dir_ext):
    with zipfile.ZipFile(dir_zip, 'r') as zip_ref:
        zip_ref.extractall(dir_ext)


def leitura_arquivo(path):
    df = pd.read_csv(path)
    return df

# INMET - dados históricos
def processa_csv_inmet(df_info, df):
    # Informações do dataframe
    df_info['ATRIBUTO'] = df_info['ATRIBUTO'].str.replace(':', '')
    var_info = dict(zip(df_info['ATRIBUTO'], df_info['VALOR']))

    # Inserindo as informações no dataframe principal
    for var in reversed(var_info):
        df.insert(2, var, var_info[var])
    return df

def transformacao_inmet(df):
    # Colunas
    cols = [c for c in df if "Unnamed" not in c]
    df = df[cols]
    df.columns = [c.lower() for c in df]


    # Níveis

    # Tipo

    return df