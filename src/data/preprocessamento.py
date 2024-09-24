from datetime import datetime

import pandas as pd
import numpy as np

# Data da ocorrência

def padroniza_hora(str):
    
    if 'UTC' in str:
        hora_str = datetime.strptime(str, '%H%M UTC')
        hora = hora_str.strftime('%H:%M:%S')

    elif ':' in str:
        hora_str = datetime.strptime(str, '%H:%M')
        hora = hora_str.strftime('%H:%M:%S')
    else:
        print('erro.')
        hora = '00:00:00'

    return hora


def prep_aneel(df):

    # Data/hora da ocorrência
    df['dthinicioocorrenciaaberta'] = pd.to_datetime(
        df['dthinicioocorrenciaaberta'], 
        format='%Y-%m-%d %H:%M:%S'
    ) 

    df['data_key'] = df['dthinicioocorrenciaaberta'].apply(lambda dt: dt.replace(minute=0, second=0)) 

    # Estado
    cod_uf_ibge = {
        12: 'AC',  # Acre
        27: 'AL',  # Alagoas
        13: 'AM',  # Amazonas
        29: 'BA',  # Bahia
        23: 'CE',  # Ceará
        53: 'DF',  # Distrito Federal
        32: 'ES',  # Espírito Santo
        52: 'GO',  # Goiás
        21: 'MA',  # Maranhão
        31: 'MG',  # Minas Gerais
        50: 'MS',  # Mato Grosso do Sul
        51: 'MT',  # Mato Grosso
        15: 'PA',  # Pará
        25: 'PB',  # Paraíba
        26: 'PE',  # Pernambuco
        22: 'PI',  # Piauí
        41: 'PR',  # Paraná
        33: 'RJ',  # Rio de Janeiro
        24: 'RN',  # Rio Grande do Norte
        43: 'RS',  # Rio Grande do Sul
        11: 'RO',  # Rondônia
        14: 'RR',  # Roraima
        42: 'SC',  # Santa Catarina
        35: 'SP',  # São Paulo
        28: 'SE',  # Sergipe
        17: 'TO'   # Tocantins
    }

    df['uf'] = df['ufibge'].map(cod_uf_ibge)

    return None


def prep_inmet(df):

    df['data'] = pd.to_datetime(
        df['data'], 
        format='%Y-%m-%d %H:%M:%S'
    )

    df['hora'] = (
        df['hora']
                .astype(str)
                .apply(padroniza_hora)
    )

    df['data_key'] = df['data'].astype(str) + ' ' + df['hora'].astype(str)
    df = df.loc[df['data'].notnull(), :]
    df['data_key'] = pd.to_datetime(df['data_key'], format='%Y-%m-%d %H:%M:%S')
    
    return df

def gera_base_modelagem(df_aneel, df_inmet):
    # Tabela de agregação para o Estado e por data/hora:
    df_inmet_gpb = df_inmet.groupby(['uf', 'data_key']).agg(
        pressao_atm = ('pressao_atm_nvl_estac', 'mean'),
        temp_min = ('temp_min_hora_ant', 'mean'),
        temp_max = ('temp_max_hora_ant', 'mean'),
        umidade = ('umid_rel_ar', 'mean'),
        vento_direcao = ('vento_dir_hora', 'mean'),
        vento_rajada = ('vento_raj_max', 'mean'),
        vento_velocidade = ('vento_vel_hora', 'mean')
    )
    df_inmet_gpb.reset_index(inplace=True)

    # Unindo os dataframes
    df_mdl = pd.merge(
        left=df_aneel,
        right=df_inmet_gpb,
        on=['data_key', 'uf'],
        how='inner'
    )
    
    return df_mdl