from process.azure_storage import get_file_from_blob
from lxml import etree
import pandas as pd
from io import BytesIO
from typing import List, Dict
from datetime import datetime
from process.helpers import yymmdd
from process.queries import insert_pregao, insert_pregao_bulk, select_all_pregao  # Importa as funções de manipulação do banco de dados
import os

def to_decimal(value):
    """Converte um valor para Decimal, retornando None em caso de erro."""
    from decimal import Decimal, InvalidOperation
    if value is None:
        return None
    try:
        return Decimal(str(value).replace(",", ".").strip())
    except (InvalidOperation, ValueError):
        return None

def to_date(value: str):
    """Converte uma string de data no formato ISO para um objeto de data."""
    from datetime import datetime
    if "T" in value:
        value = value.split("T", 1)[0]
    return datetime.strptime(value, "%Y-%m-%d").date()

def extract_data_from_xml(xml_bytes: bytes) -> List[Dict]:
    """Extrai informações de <PricRpt> do XML."""
    tree = etree.parse(BytesIO(xml_bytes), etree.XMLParser(recover=True, huge_tree=True))
    pricrpts = tree.xpath('//*[local-name()="PricRpt"]')

    if not pricrpts:
        print("Nenhum elemento <PricRpt> encontrado no XML.")
        return []

    print(f"Encontrados {len(pricrpts)} elementos <PricRpt> no XML.")

    extracted_data = []
    for pr in pricrpts:
        ativo = pr.xpath('.//*[local-name()="TckrSymb"][1]/text()')
        data_pregao = pr.xpath('.//*[local-name()="TradDt"]/*[local-name()="Dt"][1]/text()')

        if not ativo or not data_pregao:
            print("Registro ignorado por falta de 'ativo' ou 'data_pregao'.")
            continue

        # try to extract opening/closing/min/max prices
        abertura = to_decimal(pr.xpath('.//*[local-name()="FrstPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="FrstPric"][1]/text()') else None)
        fechamento = to_decimal(pr.xpath('.//*[local-name()="LastPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="LastPric"][1]/text()') else None)
        minimo = to_decimal(pr.xpath('.//*[local-name()="MinPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="MinPric"][1]/text()') else None)
        maximo = to_decimal(pr.xpath('.//*[local-name()="MaxPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="MaxPric"][1]/text()') else None)

        # try to extract volume (various possible tag name patterns)
        vol_texts = pr.xpath('.//*[contains(local-name(), "Qty") or contains(local-name(), "Vol")]/text()')
        total_volume = None
        if vol_texts:
            # pick the first numeric-like value
            for vt in vol_texts:
                try:
                    total_volume = to_decimal(vt)
                    if total_volume is not None:
                        break
                except Exception:
                    continue

        # Use database column names directly to simplify downstream inserts
        ativo_str = ativo[0].strip()
        # Only include assets that end with '34'
        if not ativo_str.endswith("34"):
            continue

        extracted_data.append({
            "Ativo": ativo_str,
            "DataPregao": to_date(data_pregao[0].strip()),
            "Abertura": abertura,
            "Fechamento": fechamento,
            "Volume": total_volume,
        })

    return extracted_data

def transform():
    """Processa o arquivo XML, salva os dados extraídos em um arquivo Excel e insere no banco de dados."""
    # Ler o nome do arquivo salvo pelo extract.py
    # compute repo root (used both for searching and for diagnostic output)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, os.pardir))

    def find_last_data_file() -> str:
        # Allow override via environment variable
        env_path = os.getenv("LAST_DATA_FILE")
        candidates = []
        if env_path:
            candidates.append(env_path)

        # If user runs from repo root: ./dados_b3/last_data.txt
        candidates.append(os.path.join(os.getcwd(), "dados_b3", "last_data.txt"))

        # If script is run from process/ folder: ../dados_b3/last_data.txt
        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.abspath(os.path.join(script_dir, os.pardir))
        candidates.append(os.path.join(repo_root, "dados_b3", "last_data.txt"))

        for p in candidates:
            if p and os.path.exists(p):
                return p
        return None

    last_data_path = find_last_data_file()
    if not last_data_path:
        print("Erro: O arquivo 'last_data.txt' não foi encontrado. Certifique-se de que o script 'extract.py' foi executado.")
        print("Paths verificados:")
        print(f" - LAST_DATA_FILE env: {os.getenv('LAST_DATA_FILE')}")
        print(f" - cwd/dados_b3: {os.path.join(os.getcwd(), 'dados_b3', 'last_data.txt')}")
        print(f" - repo/dados_b3: {os.path.join(repo_root, 'dados_b3', 'last_data.txt')}")
        return

    with open(last_data_path, "r", encoding="utf-8") as f:
        file_name = f.read().strip()

    print(f"Baixando o arquivo XML '{file_name}' do Azure Blob Storage...")

    # Allow local fallback for development when Azurite is not running.
    # Set environment var `USE_LOCAL_BLOBS=true` or `LOCAL_BLOB_DIR` to read files from disk.
    use_local = os.getenv("USE_LOCAL_BLOBS", "false").lower() in ("1", "true", "yes")
    local_dir = os.getenv("LOCAL_BLOB_DIR")
    xml_content = None
    if use_local or local_dir:
        if not local_dir:
            # default to repository/dados_b3
            local_dir = os.path.abspath(os.path.join(repo_root, "dados_b3"))
        local_path = os.path.join(local_dir, file_name)
        print(f"Tentando carregar arquivo local: {local_path}")
        if os.path.exists(local_path):
            with open(local_path, "rb") as f:
                xml_content = f.read().decode("utf-8")
            print(f"Arquivo carregado do disco: {local_path}")
        else:
            print(f"Arquivo local não encontrado: {local_path}; tentando Azure Blob...")

    if xml_content is None:
        xml_content = get_file_from_blob(file_name)

    if not xml_content:
        print(f"Não foi possível baixar o arquivo '{file_name}' do Azure Blob Storage.")
        return

    print("Extraindo informações específicas do arquivo XML...")
    extracted_data = extract_data_from_xml(xml_content.encode('utf-8'))

    if not extracted_data:
        print("Nenhum dado foi extraído do arquivo XML.")
        return

    # Criar um DataFrame e salvar em Excel
    df = pd.DataFrame(extracted_data)
    print("Primeiros registros identificados:")
    print(df.head(10))  # Mostra os primeiros 10 registros

    output_file = "resultado.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Dados salvos em {output_file}")

    # Inserir os dados no banco de dados (bulk)
    print("Inserindo dados no banco de dados (bulk)...")

    # Keep only expected columns (in order) using the DB column names we already set
    expected = ["Ativo", "DataPregao", "Abertura", "Fechamento", "Volume"]
    cols = [c for c in expected if c in df.columns]
    df_to_insert = df[cols]

    # Ensure the column names and table name match the queries implementation
    inserted = insert_pregao_bulk(df_to_insert, table_name="Cotacoes")
    print(f"Dados inseridos com sucesso no banco de dados: {inserted} registros.")

    # Consultar os dados inseridos
    print("\nConsultando os dados inseridos no banco de dados...")
    records = select_all_pregao()
    print(f"Total de registros no banco de dados: {len(records)}")
    print("Exibindo os 5 primeiros registros:")
    print("-" * 50)
    for record in records[:5]:  # Exibe os 5 primeiros registros
        print(f"Ativo: {record[1]}, Data: {record[2]}, Abertura: {record[3]}, Fechamento: {record[4]}")
    print("-" * 50)

if __name__ == "__main__":
    transform()