import os
import zipfile

def descompacta_zip(dir_zip, dir_ext):
    with zipfile.ZipFile(dir_zip, 'r') as zip_ref:
        zip_ref.extractall(dir_ext)