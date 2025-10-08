import psycopg2

try:
    print("[INFO] Tentando conectar ao banco de dados...")
    conn = psycopg2.connect(
        dbname="b3_data",
        user="cloud",
        password="123456",
        host="localhost",
        port="5432"
    )
    print("[INFO] Conexão com o banco de dados estabelecida com sucesso!")
    conn.close()
except Exception as e:
    print(f"[ERRO] Não foi possível conectar ao banco de dados: {e}")