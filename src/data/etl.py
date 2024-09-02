import os
import zipfile
import pandas as pd
import numpy as np

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
    dict_ajt_var = {'data (yyyy-mm-dd)':'data', 
        'hora (utc)':'hora', 
        'região':'regiao', 
        'uf':'uf', 
        'estação':'estacao',
        'codigo (wmo)':'codigo_wmo', 
        'latitude':'latitude', 
        'longitude':'longitude', 
        'altitude':'altitude',
        'precipitação total, horário (mm)':'precipitacao_total',
        'pressao atmosferica ao nivel da estacao, horaria (mb)':'pressao_atm_nvl_estac',
        'pressão atmosferica max.na hora ant. (aut) (mb)':'pressao_atm_max_hora_ant',
        'pressão atmosferica min. na hora ant. (aut) (mb)':'pressao_atm_min_hora_ant',
        'radiacao global (kj/m²)':'radiacao_global',
        'temperatura do ar - bulbo seco, horaria (°c)':'temp_ar_bulbo_seco',
        'temperatura do ponto de orvalho (°c)':'temp_ponto_orvalho',
        'temperatura máxima na hora ant. (aut) (°c)':'temp_max_hora_ant',
        'temperatura mínima na hora ant. (aut) (°c)':'temp_min_hora_ant',
        'temperatura orvalho max. na hora ant. (aut) (°c)':'temp_orvalho_max_hora_ant',
        'temperatura orvalho min. na hora ant. (aut) (°c)':'temp_orvalho_min_hora_ant',
        'umidade rel. max. na hora ant. (aut) (%)':'umidade_rel_max_hora_ant',
        'umidade rel. min. na hora ant. (aut) (%)':'umidade_rel_min_hora_ant',
        'umidade relativa do ar, horaria (%)':'umid_rel_ar',
        'vento, direção horaria (gr) (° (gr))':'vento_dir_hora', 
        'vento, rajada maxima (m/s)':'vento_raj_max',
        'vento, velocidade horaria (m/s)':'vento_vel_hora'
    }

    cols = [c for c in df if "Unnamed" not in c]
    df = df[cols]
    df.columns = [c.lower() for c in df]
    df.columns = df.columns.map(dict_ajt_var)

    # Limpeza
    lat_long = ['latitude', 'longitude']

    for l in lat_long:
        df[l] = (df[l]
                    .astype(str)
                    .str.replace(',', '.'))
        
    # Níveis

    # Tipo
    var_float = [
        'latitude', 
        'longitude',
        'pressao_atm_nvl_estac',
        'pressao_atm_max_hora_ant',
        'pressao_atm_min_hora_ant',
        'radiacao_global',
        'temp_ar_bulbo_seco',
        'temp_ponto_orvalho',
        'temp_max_hora_ant',
        'temp_min_hora_ant',
        'temp_orvalho_max_hora_ant',
        'temp_orvalho_min_hora_ant',
        'vento_raj_max',
        'vento_vel_hora'
    ]
    for f in var_float:
        df[f] = df[f].astype(np.float64)

    var_int = [
        #'altitude',
        'umidade_rel_min_hora_ant',
        'umid_rel_ar',
        'vento_dir_hora'
    ]
    for i in var_int:
        df[i] = df[i].astype(np.int64)
    
    df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')

    return df 

def carregamento_inmet(df):
    uf = df['uf'][0]
    dir_load = f'../../../data/processed/inmet_historico/{uf.lower()}.csv'

    if os.path.exists(dir_load):
        df_atualizar = pd.read_csv(dir_load, sep=';')
        df_load = pd.concat([df_atualizar, df])
        tam_i = len(df_load)
        df_load.drop_duplicates(subset=['data', 'hora'], keep='first', inplace=True)
        tam_f = len(df_load)
        amostra_dup = tam_i-tam_f
        print(f'O arquivo contém {amostra_dup} amostras duplicadas que não serão carregadas.')
    else:
        df_load = df.copy()

    df_load.to_csv(
        dir_load, 
        sep=';', 
        decimal='.',
        index=False 
    )
    
    print(f'Amostra carregada com sucesso em: {dir_load[:-3]}')