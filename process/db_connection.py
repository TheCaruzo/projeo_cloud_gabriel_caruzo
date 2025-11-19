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
        print("Conectado ao Azure SQL via pyodbc!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao Azure SQL: {e}")
        return None


def get_sqlalchemy_connection_string():
    """Retorna string SQLAlchemy."""
    # Build a proper ODBC connection string and URL-encode it so that
    # special characters in the password (e.g. @) don't break the URL.
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