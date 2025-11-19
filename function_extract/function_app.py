import logging
import azure.functions as func
import extract as extract

app = func.FunctionApp()
@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def extract_arquivos_trigger(myTimer: func.TimerRequest) -> None:
    logging.info('Executando extração de arquivos B3...')
    # Chama a função de extração
    extract.run()
    logging.info('Extração de arquivos B3 concluída.')