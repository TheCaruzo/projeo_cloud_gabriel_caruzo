import os
import pyodbc
from sqlalchemy import create_engine

def get_connection():
    """Conexão com Azure SQL via pyodbc."""
    
    server = os.getenv("DB_HOST", "caruzo.database.windows.net")
    database = os.getenv("DB_NAME", "AP2Cloud")
    username = os.getenv("DB_USER", "ap2")
    password = os.getenv("DB_PASSWORD", "12345678G@")

    connection_string = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

    try:
        conn = pyodbc.connect(connection_string)
        print("Conectado ao Azure SQL!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao Azure SQL: {e}")
        return None


def get_sqlalchemy_connection_string():
    """Retorna string SQLAlchemy."""
    
    server = os.getenv("DB_HOST", "caruzo.database.windows.net")
    database = os.getenv("DB_NAME", "AP2Cloud")
    username = os.getenv("DB_USER", "ap2")
    password = os.getenv("DB_PASSWORD", "12345678G@")

    return (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}:1433/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server&encrypt=yes"
    )


def get_sqlalchemy_engine():
    conn_str = get_sqlalchemy_connection_string()
    engine = create_engine(conn_str)
    return engine


if __name__ == "__main__":
    conn = get_connection()
    if conn:
        conn.close()
