import mysql.connector
from db_connection import get_connection

def insert_pregao(ativo, data_pregao, abertura, fechamento, preco_minimo, preco_maximo):
    """Insere um registro na tabela 'pregao'."""
    try:
        connection = get_connection()
        cursor = connection.cursor()
        sql = """
            INSERT INTO pregao_report (ativo, data_pregao, abertura, fechamento, preco_minimo, preco_maximo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (ativo, data_pregao, abertura, fechamento, preco_minimo, preco_maximo)
        cursor.execute(sql, values)
        connection.commit()
        print(f"Registro inserido com sucesso: {ativo}, {data_pregao}")
    except mysql.connector.Error as err:
        print(f"[ERRO] Não foi possível inserir o registro: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def select_all_pregao():
    """
    Busca todos os registros da tabela 'pregao_report' via SQL puro.
    """
    try:
        connection = get_connection()
        if connection:
            cursor = connection.cursor()
            sql = "SELECT * FROM pregao_report"
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return result
    except Exception as e:
        print(f"[ERRO] Não foi possível buscar os dados: {e}")
        return []