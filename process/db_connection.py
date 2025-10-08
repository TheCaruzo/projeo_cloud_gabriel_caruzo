import mysql.connector

def get_connection():
    """Conecta ao banco de dados 'dados_b3' no MySQL."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  # Substitua pelo seu usuário MySQL
            password="123456",  # Substitua pela sua senha MySQL
            database="dados_b3"  # Certifique-se de que o banco foi criado
        )
        print("Conexão com o banco de dados 'dados_b3' estabelecida.")
        return connection
    except mysql.connector.Error as e:
        print(f"[ERRO] Não foi possível conectar ao banco de dados: {e}")
        return None

if __name__ == "__main__":
    conn = get_connection()
    if conn:
        conn.close()