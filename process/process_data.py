from azure_storage import get_file_from_blob
from lxml import etree
import pandas as pd
from io import BytesIO
from typing import List, Dict
from datetime import datetime
from decimal import Decimal, InvalidOperation

POINTER_BLOB = "last_XML.txt"  # Nome do arquivo que aponta para o último XML

def to_decimal(value) -> Decimal:
    """Converte um valor para Decimal, retornando None em caso de erro."""
    if value is None:
        return None
    try:
        return Decimal(str(value).replace(",", ".").strip())
    except (InvalidOperation, ValueError):
        return None

def to_date(value: str) -> datetime.date:
    """Converte uma string de data no formato ISO para um objeto de data."""
    if "T" in value:
        value = value.split("T", 1)[0]
    return datetime.strptime(value, "%Y-%m-%d").date()

def extract_data_from_xml(xml_bytes: bytes) -> List[Dict]:
    """Extrai informações de <PricRpt> do XML."""
    tree = etree.parse(BytesIO(xml_bytes), etree.XMLParser(recover=True, huge_tree=True))
    pricrpts = tree.xpath('//*[local-name()="PricRpt"]')

    if not pricrpts:
        print("[ERRO] Nenhum elemento <PricRpt> encontrado no XML.")
        return []

    print(f"[INFO] Encontrados {len(pricrpts)} elementos <PricRpt> no XML.")

    extracted_data = []
    for pr in pricrpts:
        ativo = pr.xpath('.//*[local-name()="TckrSymb"][1]/text()')
        data_pregao = pr.xpath('.//*[local-name()="TradDt"]/*[local-name()="Dt"][1]/text()')

        if not ativo or not data_pregao:
            print("[AVISO] Registro ignorado por falta de 'ativo' ou 'data_pregao'.")
            continue

        extracted_data.append({
            "Ativo": ativo[0].strip(),
            "data_pregao": to_date(data_pregao[0].strip()),
            "data_abertura": to_decimal(pr.xpath('.//*[local-name()="FrstPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="FrstPric"][1]/text()') else None),
            "data_fechamento": to_decimal(pr.xpath('.//*[local-name()="LastPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="LastPric"][1]/text()') else None),
            "preco_minimo": to_decimal(pr.xpath('.//*[local-name()="MinPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="MinPric"][1]/text()') else None),
            "preco_maximo": to_decimal(pr.xpath('.//*[local-name()="MaxPric"][1]/text()')[0] if pr.xpath('.//*[local-name()="MaxPric"][1]/text()') else None),
            "total_volume": to_decimal(pr.xpath('.//*[local-name()="NtlFinVol"][1]/text()')[0] if pr.xpath('.//*[local-name()="NtlFinVol"][1]/text()') else None),
        })

    return extracted_data

def transform(limit=None):
    """Processa o arquivo XML e salva os dados extraídos em um arquivo Excel."""
    print("[INFO] Obtendo o nome do último arquivo XML do Blob Storage...")
    pointer_content = get_file_from_blob(POINTER_BLOB)
    if not pointer_content:
        print("[ERRO] Não foi possível obter o nome do último arquivo XML.")
        return

    file_name = pointer_content.strip()
    print(f"[INFO] Baixando o arquivo XML '{file_name}' do Azure Blob Storage...")
    xml_content = get_file_from_blob(file_name)

    if not xml_content:
        print(f"[ERRO] Não foi possível baixar o arquivo '{file_name}' do Azure Blob Storage.")
        return

    print("[INFO] Extraindo informações específicas do arquivo XML...")
    extracted_data = extract_data_from_xml(xml_content.encode('utf-8'))

    if not extracted_data:
        print("[ERRO] Nenhum dado foi extraído do arquivo XML.")
        return

    # Criar um DataFrame e salvar em Excel
    df = pd.DataFrame(extracted_data)
    print("[INFO] Primeiros registros identificados:")
    print(df.head(10))  # Mostra os primeiros 10 registros

    output_file = "resultado.xlsx"
    df.to_excel(output_file, index=False)
    print(f"[INFO] Dados salvos em {output_file}")

if __name__ == "__main__":
    transform(limit=1000)  # Limitando para 10 resultados para testes