from azure.storage.blob import BlobServiceClient
from azure.storage.blob import PublicAccess
import logging
import os


AZURE_BLOB_CONNECTION = os.getenv("AZURESTORAGE_CONNECTION_STRING")
CONTAINER = os.getenv("AZURESTORAGE_CONTAINER_NAME", "dados-pregao-bolsa")

def save_file_to_blob(file_name, local_path_file):

    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception as e:
        pass #container ja existe

    with open(local_path_file, "rb") as data:
        container.upload_blob(name=file_name, data=data, overwrite=True)

def get_file_from_blob(file_name):
    service = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception as e:
        pass #container ja existe

    #Cria a referencia do arquivo no azure
    blob_client = container.get_blob_client(file_name)

    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall().decode("utf-8")
        return blob_content
    except Exception as e:
        logging.error("Error ao obter arquivo")







