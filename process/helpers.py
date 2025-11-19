from datetime import datetime, timedelta
import os


def yymmdd(date_obj=None):
    """Converte um objeto datetime para o formato YYMMDD.

    Por compatibilidade com a necessidade comum do projeto, quando nenhum
    argumento é fornecido retorna a data de hoje (antes estava retornando
    ontem). Para obter ontem explicitamente passe um objeto datetime.
    """
    if date_obj is None:
        date_obj = datetime.now()
    return date_obj.strftime("%y%m%d")


def find_existing_b3_date(base_path=None):
    if base_path is None:
        base_path = os.path.join(os.getcwd(), "dados_b3")

    def exists_for(dt_str: str) -> bool:
        zip_path = os.path.join(base_path, f"pregao_{dt_str}.zip")
        folder1 = os.path.join(base_path, f"ARQUIVOSPREGAO_SPRE{dt_str}")
        folder2 = os.path.join(base_path, f"pregao_{dt_str}")
        return os.path.exists(zip_path) or os.path.isdir(folder1) or os.path.isdir(folder2)

    today = datetime.now()
    today_str = yymmdd(today)
    if exists_for(today_str):
        return today_str

    yesterday = today - timedelta(days=1)
    yesterday_str = yymmdd(yesterday)
    if exists_for(yesterday_str):
        return yesterday_str

    return None


