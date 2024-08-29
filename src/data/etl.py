import os
import zipfile
import pandas as pd

def descompacta_zip(dir_zip, dir_ext):
    with zipfile.ZipFile(dir_zip, 'r') as zip_ref:
        zip_ref.extractall(dir_ext)


def leitura_arquivo(path):
    df = pd.read_csv(path)
    return df