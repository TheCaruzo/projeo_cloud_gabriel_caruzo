from azure_storage import get_file_from_blob
from lxml import etree
import pandas as pd
from io import BytesIO
from typing import List, Dict
from datetime import datetime
from helpers import yymmdd
from queries import insert_pregao, select_all_pregao  # Importa as funções de manipulação do banco de dados

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

        extracted_data.append({
            "Ativo": ativo[0].strip(),
            "data_pregao": to_date(data_pregao[0].strip()),
            "data_abertura": to_decimal(pr.xpath('.//*[local-name()="FrstPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="FrstPric"][1]/text()') else None),
            "data_fechamento": to_decimal(pr.xpath('.//*[local-name()="LastPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="LastPric"][1]/text()') else None),
            "preco_minimo": to_decimal(pr.xpath('.//*[local-name()="MinPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="MinPric"][1]/text()') else None),
            "preco_maximo": to_decimal(pr.xpath('.//*[local-name()="MaxPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="MaxPric"][1]/text()') else None),
        })

    return extracted_data

def transform():
    """Processa o arquivo XML, salva os dados extraídos em um arquivo Excel e insere no banco de dados."""
    # Ler o nome do arquivo salvo pelo extract.py
    try:
        with open("./dados_b3/last_data.txt", "r", encoding="utf-8") as f:
            file_name = f.read().strip()
    except FileNotFoundError:
        print("Erro: O arquivo 'last_data.txt' não foi encontrado. Certifique-se de que o script 'extract.py' foi executado.")
        return

    print(f"Baixando o arquivo XML '{file_name}' do Azure Blob Storage...")
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

    # Inserir os dados no banco de dados
    print("Inserindo dados no banco de dados...")
    for data in extracted_data:
        insert_pregao(
            ativo=data["Ativo"],
            data_pregao=data["data_pregao"],
            abertura=data["data_abertura"],
            fechamento=data["data_fechamento"],
            preco_minimo=data["preco_minimo"],
            preco_maximo=data["preco_maximo"]
        )
    print("Dados inseridos com sucesso no banco de dados.")

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