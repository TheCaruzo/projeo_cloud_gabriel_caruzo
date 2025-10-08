from datetime import datetime, timedelta

def yymmdd(date_obj=None):
    """Converte um objeto datetime para o formato YYMMDD. Usa a data atual se nenhum argumento for fornecido."""
    if date_obj is None:
        date_obj = datetime.now() - timedelta(days=1)
    return date_obj.strftime("%y%m%d")


