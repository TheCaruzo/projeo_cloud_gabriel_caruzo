from sqlalchemy.orm import Session
from .models import PregaoReport
from .db_connection import get_session


class Repository:
    def __init__(self):
        """Inicializa a sessão do banco de dados."""
        self.session = get_session()

    def insert_price(self, data):
        """
        Insere múltiplos registros na tabela 'price_reports'.
        :param data: Lista de instâncias de PriceReport.
        """
        try:
            self.session.bulk_save_objects(data)
            self.session.commit()
            print(f"[INFO] {len(data)} registro(s) inserido(s) com sucesso.")
        except Exception as e:
            self.session.rollback()
            print(f"[ERRO] Não foi possível inserir os dados: {e}")
        finally:
            self.session.close()

    def select_all_price(self):
        """
        Busca todos os registros da tabela 'price_reports'.
        :return: Lista de instâncias de PriceReport.
        """
        try:
            records = self.session.query(PregaoReport).all()
            return records
        except Exception as e:
            print(f"[ERRO] Não foi possível buscar os dados: {e}")
            return []
        finally:
            self.session.close()

