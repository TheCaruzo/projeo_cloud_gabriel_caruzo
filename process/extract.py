from datetime import datetime, timedelta
import requests
import os
import zipfile
import pandas as pd

from .helpers import yymmdd
from .azure_storage import save_file_to_blob

PATH_TO_SAVE = "./dados_b3"

def build_url_download(date_to_download):
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"

def try_http_download(url):
    session = requests.Session()
    try:
        print(f"Tentando {url}")
        resp = session.get(url, timeout=30)
        if (resp.ok) and resp.content and len(resp.content) > 200:
            if (resp.content[:2] == b"PK"):
                return resp.content, os.path.basename(url)
    except requests.RequestException:
        print(f"Falha ao acessar a {url}")
        pass

    return None, None

def achar_zip_pregao_recente(max_days):
    for days_back in range(0, max_days):
        dt_obj = datetime.now() - timedelta(days=days_back)
        dt_str = yymmdd(dt_obj)
        url = build_url_download(dt_str)
        zip_bytes, zip_name = try_http_download(url)
        if zip_bytes:
            if days_back > 0:
                print(f"Arquivo encontrado para {dt_str} (há {days_back} dias)")
            return dt_str, zip_bytes, zip_name
    return None, None, None

def run():
    print("Iniciando a extração dos dados da B3...")
    MAX_DAYS = 7
    dt, zip_bytes, zip_name = achar_zip_pregao_recente(MAX_DAYS)

    if not zip_bytes:
        raise RuntimeError(f"Não foi possível baixar o arquivo de cotações nos últimos {MAX_DAYS} dias. Verifique conexão / site da B3.")

    print(f"Baixado arquivo de cotações: {zip_name}")

    # 2) Salvar o Zip
    os.makedirs(PATH_TO_SAVE, exist_ok=True)
    zip_path = f"{PATH_TO_SAVE}/pregao_{dt}.zip"
    with open(zip_path, "wb") as f:
        f.write(zip_bytes)

    print(f"Zip salvo em {zip_path}")

    # 3) Extrair os arquivos do zip
    first_extract_dir = os.path.join(PATH_TO_SAVE, f"pregao_{dt}")
    os.makedirs(first_extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(first_extract_dir)

    second_zip = os.path.join(first_extract_dir, f"SPRE{dt}.zip")
    second_extract_dir = os.path.join(PATH_TO_SAVE, f"ARQUIVOSPREGAO_SPRE{dt}")
    os.makedirs(second_extract_dir, exist_ok=True)
    with zipfile.ZipFile(second_zip, "r") as zf:
        zf.extractall(second_extract_dir)

    print("Arquivos extraídos do zip com sucesso")

    # Subir o(s) XML(s) para o Azure Blob Storage
    arquivos = [f for f in os.listdir(f"{PATH_TO_SAVE}/ARQUIVOSPREGAO_SPRE{dt}") if f.endswith(".xml")]
    last_xml_name = None

    for arquivo in arquivos:
        save_file_to_blob(arquivo, f"{PATH_TO_SAVE}/ARQUIVOSPREGAO_SPRE{dt}/{arquivo}")
        last_xml_name = arquivo
    print("Arquivo(s) XML enviado(s) para o Azure Blob Storage com sucesso")

    # Criar um DataFrame com os dados extraídos
    extracted_data = []
    for arquivo in arquivos:
        file_path = f"{PATH_TO_SAVE}/ARQUIVOSPREGAO_SPRE{dt}/{arquivo}"
        extracted_data.append({"Arquivo": arquivo, "Caminho": file_path})

    # Grava um ponteiro com o nome do último XML enviado
    if last_xml_name:
        POINTER_LOCAL = os.path.join(PATH_TO_SAVE, "last_data.txt")
        with open(POINTER_LOCAL, "w", encoding="utf-8") as f:
            f.write(last_xml_name.strip())
        save_file_to_blob("last_data.txt", POINTER_LOCAL)

    # Lógica para transformar dados do último arquivo XML
    if last_xml_name:
        last_xml_path = os.path.join(PATH_TO_SAVE, f"ARQUIVOSPREGAO_SPRE{dt}", last_xml_name)
        print(f"Transformando dados do último arquivo XML: {last_xml_path}")

if __name__ == "__main__":
    run()