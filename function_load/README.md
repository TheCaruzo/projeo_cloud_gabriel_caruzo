# Blob Load Function (local)

This folder contains a simple Azure Functions Python blob-trigger that saves incoming blobs into the project `dados_b3` folder.

Quick run (Windows PowerShell):

1. Start Azurite (if not already running). You can use the Azurite VS Code extension or run Azurite Docker image.

2. Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
```

3. From this folder, start the Functions host (requires Azure Functions Core Tools):

```powershell
func start
```

The function listens on the blob container path `arquivosb3`. For local testing with Azurite, the connection string in `local.settings.json` uses `UseDevelopmentStorage=true`.

When a blob is added to the container, the function writes the blob bytes into `dados_b3/<filename>` in the repository root.
