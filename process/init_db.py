import os
import mysql.connector
from mysql.connector import errorcode


def get_admin_connection():
    """Connect to MySQL server without specifying a database (for creating DB)."""
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "123456")
    port = int(os.getenv("DB_PORT", "3306"))

    return mysql.connector.connect(host=host, user=user, password=password, port=port)


def create_database_and_table():
    db_name = os.getenv("DB_NAME", "dados_b3")

    conn = None
    try:
        conn = get_admin_connection()
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET 'utf8mb4'")
        print(f"Database '{db_name}' ensured.")
        conn.database = db_name

        # Create table Cotacoes if not exists (schema provided by user)
        # Create table Cotacoes if not exists (use larger Ativo to accommodate longer tickers)
        create_table_sql = (
            "CREATE TABLE IF NOT EXISTS Cotacoes ("
            "Id INT NOT NULL AUTO_INCREMENT, "
            "Ativo VARCHAR(64), "
            "DataPregao DATE, "
            "Abertura DECIMAL(10,2), "
            "Fechamento DECIMAL(10,2), "
            "Volume DECIMAL(18,2), "
            "PRIMARY KEY (Id)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )

        cursor.execute(create_table_sql)
        print("Table 'Cotacoes' ensured.")

        # Ensure existing table column size is at least 64 for Ativo (idempotent)
        try:
            alter_sql = "ALTER TABLE Cotacoes MODIFY COLUMN Ativo VARCHAR(64)"
            cursor.execute(alter_sql)
            print("Altered 'Cotacoes.Ativo' to VARCHAR(64) if it existed with smaller size.")
        except Exception:
            # If table doesn't exist yet or alter not permitted, ignore and continue
            pass

    except mysql.connector.Error as err:
        print(f"[ERRO] Falha ao criar DB/tabela: {err}")
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        if conn:
            conn.close()


if __name__ == "__main__":
    create_database_and_table()
