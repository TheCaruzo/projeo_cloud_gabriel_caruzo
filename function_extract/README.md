# Extract Function

This Azure Function runs the `process.extract.run()` job from the repository on a schedule (Timer trigger).

Defaults:
- Schedule: daily at 06:00 (NCRONTAB `0 0 6 * * *`).
- Uses Azurite/local storage via `local.settings.json` (the file includes `AZURE_BLOB_CONNECTION` pointing to the default Azurite connection string).

Run locally (PowerShell):

```powershell
cd 'C:\Users\Dell\Desktop\projeo_cloud_gabriel_caruzo\function_extract'
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
func start
```

Notes:
- Timer triggers require the Functions host storage account (the `AzureWebJobsStorage` value in `local.settings.json`). For local testing, use Azurite.
- Logs from the extraction will be printed and the extraction code will upload extracted XMLs to the blob container defined in `AZURE_CONTAINER_NAME`.
