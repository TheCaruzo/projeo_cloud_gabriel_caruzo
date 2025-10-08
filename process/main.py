from extract import extract_data_from_blob, parse_xml
from process_data import transform_data
from create_table import create_table
from repository import insert_price, select_all_price
from models import PregaoReport 

def main():
    """Executa o processo completo: ETL e gerenciamento do banco de dados."""
    print("[INFO] Iniciando processo completo...")

    try:
        # Etapa 1: Extração
        print("[INFO] Extraindo dados do Blob Storage...")
        xml_bytes = extract_data_from_blob()  # Extrai o XML do Blob Storage
        raw_data = parse_xml(xml_bytes)  # Processa o XML para obter os dados brutos
        print(f"[INFO] {len(raw_data)} registro(s) extraído(s).")
    except Exception as e:
        print(f"[ERRO] Falha na etapa de EXTRAÇÃO: {e}")
        return

    try:
        # Etapa 2: Transformação
        print("[INFO] Transformando os dados...")
        transformed_data = transform_data(raw_data)  # Transforma os dados para o formato do banco
        print(f"[INFO] {len(transformed_data)} registro(s) pronto(s) para inserção.")
    except Exception as e:
        print(f"[ERRO] Falha na etapa de TRANSFORMAÇÃO: {e}")
        return

    try:
        # Etapa 3: Criar tabela no banco de dados
        print("[INFO] Criando tabela no banco de dados...")
        create_table()  # Garante que a tabela exista
        print("[INFO] Tabela criada com sucesso.")
    except Exception as e:
        print(f"[ERRO] Falha ao CRIAR TABELA: {e}")
        return

    try:
        # Etapa 4: Inserir dados transformados no banco de dados
        print("[INFO] Inserindo dados transformados no banco de dados...")
        insert_price(transformed_data)
        print("[INFO] Dados inseridos com sucesso.")
    except Exception as e:
        print(f"[ERRO] Falha ao INSERIR DADOS: {e}")
        return

    try:
        # Etapa 5: Consultar dados do banco de dados
        print("\n[INFO] Verificando dados inseridos (consultando os 5 primeiros registros)...")
        records = select_all_price(limit=5)  # Busca os 5 primeiros registros para verificação
        print(f"[INFO] {len(records)} registro(s) encontrado(s).")
        print("-" * 50)
        for record in records:
            print(f"Ativo: {record.ativo}, Data: {record.data_pregao}, Abertura: {record.abertura}, Fechamento: {record.fechamento}")
        print("-" * 50)
    except Exception as e:
        print(f"[ERRO] Falha ao CONSULTAR DADOS: {e}")
        return

    print("[INFO] Processo completo concluído com sucesso.")

if __name__ == "__main__":
    main()