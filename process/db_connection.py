import os
import sys
import locale
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

# Força o encoding UTF-8 no Python
os.environ["PYTHONUTF8"] = "1"
sys.stdout.reconfigure(encoding='utf-8')
sys.stdin.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

print(f"[INFO] Encoding padrão: {locale.getpreferredencoding()}")

def get_engine():
    """Conecta ao banco de dados 'b3_data'."""
    try:
        print("[INFO] Tentando conectar ao banco de dados...")
        engine = create_engine(
            'postgresql+psycopg2://cloud:123456@localhost:5432/b3_data',
            connect_args={'client_encoding': 'utf8'}
        )
        print("Conexão com o banco de dados 'b3_data' estabelecida.")
        try:
            print("[INFO] Tentando criar as tabelas...")
            Base.metadata.create_all(engine)  # Cria as tabelas se não existirem
            print("[INFO] Tabelas verificadas/criadas no banco de dados 'b3_data'.")
        except Exception as e:
            print(f"[ERRO] Não foi possível criar as tabelas: {e}")
            raise
        return engine
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar ao banco de dados: {e}")
        raise

def get_session():
    """Cria uma sessão para interagir com o banco de dados."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

if __name__ == "__main__":
    engine = get_engine()