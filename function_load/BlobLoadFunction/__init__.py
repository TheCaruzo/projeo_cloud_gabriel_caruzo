import logging
import os
import requests
import azure.functions as func


def main(myblob: func.InputStream):
    """Blob trigger function compatible with function.json entrypoint 'main'.

    Logs blob name and size, reads content and optionally forwards the
    bytes to `BACKEND_INGEST_URL` if configured in app settings.
    """
    logging.info(f"Python blob trigger function processed blob. Name: {myblob.name} Blob Size: {myblob.length} bytes")
    try:
        data = myblob.read()
        logging.info(f"Read {len(data)} bytes from blob {myblob.name}")

        backend = os.environ.get("BACKEND_INGEST_URL")
        if backend:
            try:
                resp = requests.post(backend, data=data, headers={"Content-Type": "application/octet-stream"}, timeout=60)
                logging.info(f"Posted blob to backend {backend}, status={resp.status_code}")
            except Exception:
                logging.exception("Failed to POST blob to backend")

    except Exception:
        logging.exception("Error processing blob trigger")


# Optional SDK-style binding variant (uncomment to use) - requires
# adding `azurefunctions-extensions-bindings-blob` to requirements.
#
# import azurefunctions.extensions.bindings.blob as blob
#
# def main_client(client: blob.BlobClient):
#     logging.info(
#         f"Python blob trigger function processed blob \n"
#         f"Properties: {client.get_blob_properties()}\n"
#         f"Blob content head: {client.download_blob().read(size=1)}"
#     )


