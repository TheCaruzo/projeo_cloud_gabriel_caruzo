import logging
import azure.functions as func
from pathlib import Path

def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob\n"
        f"Name: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )

    try:
        # Resolve repository root (two levels up from this file: BlobLoadFunction -> function_load -> repo root)
        repo_root = Path(__file__).resolve().parents[2]
        out_dir = repo_root / 'dados_b3'
        out_dir.mkdir(parents=True, exist_ok=True)

        # Use only the filename portion of the blob name
        filename = Path(myblob.name).name
        out_path = out_dir / filename

        # Read and write blob content to project folder
        with open(out_path, 'wb') as f:
            f.write(myblob.read())

        logging.info(f"Saved blob to {out_path}")
    except Exception:
        logging.exception("Failed to save blob to disk")
