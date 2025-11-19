import os
import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus


SERVER = "caruzo.database.windows.net"
DATABASE = "AP2Cloud"
USERNAME = "ap2"
PASSWORD = "12345678G@"
DRIVER = "ODBC Driver 18 for SQL Server"
PORT = 1433



def get_connection():
    """Conexão com Azure SQL via pyodbc."""
    # Prefer explicit ODBC connection string from environment for production
    env_odbc = os.getenv("ODBC_CONN")
    if env_odbc:
        try:
            conn = pyodbc.connect(env_odbc)
            print("Conectado ao Azure SQL via pyodbc (ODBC_CONN)!")
            return conn
        except Exception as e:
            print(f"Erro ao conectar usando ODBC_CONN: {e}")

    # Fallback to the hardcoded constants (useful for local dev)
    connection_string = (
        f"Driver={{{DRIVER}}};"
        f"Server={SERVER},{PORT};"
        f"Database={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

    try:
        conn = pyodbc.connect(connection_string)
        print("Conectado ao Azure SQL via pyodbc (fallback)!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao Azure SQL: {e}")
        return None


def get_sqlalchemy_connection_string():
    """Retorna string SQLAlchemy."""
    # If an environment ODBC string exists, use it; otherwise build from
    # constants. Return a URL-encoded string ready for the
    # `odbc_connect` SQLAlchemy param.
    env_odbc = os.getenv("ODBC_CONN")
    if env_odbc:
        return quote_plus(env_odbc)

    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER},{PORT};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )
    return quote_plus(odbc_str)



def get_sqlalchemy_engine():
    # Use the ODBC connection string via the `odbc_connect` query param.
    # `get_sqlalchemy_connection_string()` returns a URL-encoded odbc string.
    quoted = get_sqlalchemy_connection_string()
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}")
    return engine



if __name__ == "__main__":
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 name FROM sys.tables;")
        result = cursor.fetchone()
        print("Teste OK! Primeira tabela:", result)
        conn.close()