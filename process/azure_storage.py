import os
import logging
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import PublicAccess
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

AZURE_BLOB_CONNECTION = os.environ.get("AZURE_BLOB_CONNECTION", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
CONTAINER = os.environ.get("AZURE_CONTAINER_NAME", "dados-pregao-bolsa")

def get_blob_service_client():
    """Returns a BlobServiceClient using the connection string."""
    return BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)

def ensure_container_exists(service_client: BlobServiceClient, container_name: str):
    """Creates a blob container if it does not already exist."""
    try:
        service_client.create_container(container_name, public_access=PublicAccess.Container)
        logging.info(f"Container '{container_name}' created with public access.")
    except ResourceExistsError:
        logging.info(f"Container '{container_name}' already exists.")
    except Exception as e:
        logging.error(f"Failed to create or check container '{container_name}': {e}", exc_info=True)
        raise

def save_file_to_blob(file_name, local_path_file):
    """Uploads a local file to Azure Blob Storage."""
    service_client = get_blob_service_client()
    ensure_container_exists(service_client, CONTAINER)
    container_client = service_client.get_container_client(CONTAINER)
    try:
        with open(local_path_file, "rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)
        logging.info(f"Successfully uploaded '{local_path_file}' to blob '{file_name}'.")
    except Exception as e:
        logging.error(f"Failed to upload blob '{file_name}': {e}", exc_info=True)
        raise

def get_file_from_blob(file_name):
    """Downloads a blob from Azure Blob Storage and returns its content as a string."""
    if not file_name:
        logging.error("[ERRO] O nome do blob não foi especificado.")
        return None

    service_client = get_blob_service_client()
    if not CONTAINER:
        logging.error("[ERRO] O nome do contêiner não foi especificado.")
        return None

    ensure_container_exists(service_client, CONTAINER)  # Garantir que o contêiner existe
    container_client = service_client.get_container_client(CONTAINER)
    blob_client = container_client.get_blob_client(file_name)
    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall().decode("utf-8")
        logging.info(f"Successfully downloaded blob '{file_name}' from container '{CONTAINER}'.")
        return blob_content
    except ResourceNotFoundError:
        logging.error(f"[ERRO] Blob '{file_name}' não encontrado no contêiner '{CONTAINER}'. Certifique-se de que o arquivo foi enviado corretamente.")
        return None
    except ValueError as ve:
        logging.error(f"[ERRO] Valor inválido ao acessar o blob: {ve}")
        return None
    except Exception as e:
        logging.error(f"[ERRO] Falha ao baixar o blob '{file_name}': {e}", exc_info=True)
        return None
