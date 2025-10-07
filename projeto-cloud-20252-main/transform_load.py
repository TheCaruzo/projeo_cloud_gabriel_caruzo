from azure_storage import get_file_from_blob
from lxml import etree
import io
import pandas as pd

DATA_FILE = "250923"
FILE_NAME = f"BVBG186_{DATA_FILE}.xml"

def transform(limit=None):
    # Baixar o arquivo XML do Azure Blob Storage
    print("[INFO] Baixando o arquivo XML do Azure Blob Storage...")
    xml_content = get_file_from_blob(FILE_NAME)

    if not xml_content:
        print(f"[ERRO] Não foi possível baixar o arquivo {FILE_NAME} do Azure Blob Storage.")
        return

    # Criar um parser com recover=True para ignorar erros de sintaxe
    parser = etree.XMLParser(recover=True, huge_tree=True)
    xml_tree = etree.parse(io.BytesIO(xml_content.encode('utf-8')), parser=parser)

    print("[INFO] Extraindo informações específicas do arquivo XML...")

    # Namespace do XML para facilitar a busca pelas tags
    ns = {"bvmf": "urn:bvmf.186.01.xsd"}

    # Lista para armazenar os resultados
    data = []
    count = 0

    # Iterar sobre as tags relevantes
    for instrmt_elem in xml_tree.iterfind(".//bvmf:Instrmt", namespaces=ns):
        ticker = instrmt_elem.findtext("bvmf:TckrSymb", namespaces=ns)

        # Iterar sobre os detalhes da negociação associados ao instrumento
        for trad_elem in instrmt_elem.findall("bvmf:TradDtls", namespaces=ns):
            # Adicionar os valores ao dicionário
            # Mapeamento das tags XML para os nomes das colunas desejados:
            # TckrSymb -> nome_acao
            # MinPric  -> preco_minimo
            # MaxPric  -> preco_maximo
            # FrstPric -> preco_abertura
            # LastPric -> preco_fechamento
            # RglrVol  -> volume_financeiro
            # TradDt   -> data_negociacao
            row = {
                "TckrSymb": ticker,
                "preco_minimo": trad_elem.findtext("bvmf:MinPric", namespaces=ns),
                "preco_maximo": trad_elem.findtext("bvmf:MaxPric", namespaces=ns),
                "preco_abertura": trad_elem.findtext("bvmf:FrstPric", namespaces=ns),
                "preco_fechamento": trad_elem.findtext("bvmf:LastPric", namespaces=ns),
                "volume_financeiro": trad_elem.findtext("bvmf:RglrVol", namespaces=ns),
                "data_negociacao": trad_elem.findtext("bvmf:TradDt/bvmf:Dt", namespaces=ns)
            }
            data.append(row)
            count += 1

            # Exibir progresso
            if count % 100 == 0:
                print(f"[INFO] Processados {count} registros...")

            # Respeitar o limite, se definido
            if limit and count >= limit:
                print("[INFO] Limite de registros atingido. Interrompendo o processamento.")
                break

        if limit and count >= limit:
            break

    # Criar um DataFrame e salvar em Excel
    df = pd.DataFrame(data)

    # Exibir os dados identificados no terminal
    print("[INFO] Primeiros registros identificados:")
    print(df.head(10))  # Mostra os primeiros 10 registros

    output_file = "resultado.xlsx"
    df.to_excel(output_file, index=False)
    print(f"[INFO] Dados salvos em {output_file}")

if __name__ == "__main__":
    transform(limit=10000)  # Limitando para 1000 resultados para testes
