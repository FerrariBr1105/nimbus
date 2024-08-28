import os

def verifica_existencia_arquivo(nome, dir):
    files = os.listdir(dir)
    return any(f'{nome}' in f for f in files)