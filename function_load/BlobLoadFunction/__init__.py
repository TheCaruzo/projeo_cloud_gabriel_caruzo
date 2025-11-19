import logging
import os
import requests
import azure.functions as func


# Use the FunctionApp decorator-style API (same pattern your professor used)
app = func.FunctionApp()


@app.blob_trigger(arg_name="myblob", path="arquivosb3/{name}", connection="AZURE_STORAGE_CONNECTION_STRING")
def load_file_b3_trigger(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob - Name: {myblob.name} - Blob Size: {myblob.length} bytes"
    )
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


