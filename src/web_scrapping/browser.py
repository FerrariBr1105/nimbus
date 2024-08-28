# Pacote destinado as funções de navegação em geral.
# Navegador
# Seleção

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Setup Selenium
def inicia_navegador(download_dir):
    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        "download.default_directory":download_dir,  # Define padrão de download
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    service = Service(executable_path='../utils/chromedriver.exe')
    navegador = webdriver.Chrome(service=service, options=chrome_options)
    navegador.maximize_window()
    return navegador

# Abre página
def acessa_pagina(navegador, url):
    navegador.get(url)

# Aguarda um tempo em segundos até que o elemento apareça na tela do navegador.
def aguarda_elemento(navegador, tempo, localizador):
    wait = WebDriverWait(navegador, tempo)
    wait.until(EC.visibility_of_element_located(localizador))

# 