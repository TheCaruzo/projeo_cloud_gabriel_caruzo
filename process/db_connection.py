import os
import mysql.connector

def get_connection():
    """Connect using mysql.connector. Reads connection info from environment
    variables with sensible defaults for local development.

    Environment variables used:
    - DB_HOST (default: 'localhost')
    - DB_USER (default: 'root')
    - DB_PASSWORD (default: '123456')
    - DB_NAME (default: 'dados_b3')
    - DB_PORT (default: 3306)
    """
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "123456")
    database = os.getenv("DB_NAME", "dados_b3")
    port = int(os.getenv("DB_PORT", "3306"))

    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
        )
        print(f"Conexão com o banco de dados '{database}' estabelecida ({user}@{host}:{port}).")
        return connection
    except mysql.connector.Error as e:
        print(f"[ERRO] Não foi possível conectar ao banco de dados: {e}")
        return None


def get_sqlalchemy_connection_string(driver: str = "mysql+mysqlconnector") -> str:
    """Return a SQLAlchemy connection string built from environment variables.

    Example for MySQL: "mysql+mysqlconnector://user:password@host:port/dbname"
    """
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "123456")
    database = os.getenv("DB_NAME", "dados_b3")
    port = os.getenv("DB_PORT", "3306")

    # URL-encode password if necessary? For now assume no special chars.
    return f"{driver}://{user}:{password}@{host}:{port}/{database}"


def get_sqlalchemy_engine(**create_engine_kwargs):
    """Create and return a SQLAlchemy engine using the connection string.

    If SQLAlchemy is not installed, raises ImportError with guidance.
    """
    try:
        from sqlalchemy import create_engine
    except Exception as e:
        raise ImportError("sqlalchemy is required for get_sqlalchemy_engine(). Install it with 'pip install sqlalchemy'.") from e

    conn_str = get_sqlalchemy_connection_string()
    engine = create_engine(conn_str, **create_engine_kwargs)
    return engine


if __name__ == "__main__":
    conn = get_connection()
    if conn:
        conn.close()