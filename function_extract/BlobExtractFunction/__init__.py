import logging
import azure.functions as func
import sys
from pathlib import Path

# Ensure repository root is on sys.path so we can import the project's `process` package
repo_root = Path(__file__).resolve().parents[2]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

try:
    from process import extract as extract_module
except Exception:
    logging.exception("Failed to import process.extract module")
    extract_module = None


def main(mytimer: func.TimerRequest) -> None:
    logging.info("Timer trigger fired for extract job")
    if extract_module is None:
        logging.error("process.extract module not available; aborting run")
        return

    try:
        extract_module.run()
        logging.info("Extraction run completed successfully")
    except Exception:
        logging.exception("Extraction run failed")
