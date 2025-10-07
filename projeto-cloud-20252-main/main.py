from extract import run as extract_run
from transform_load import validate_xml, transform

if __name__ == "__main__":
    print("[INFO] Iniciando o processo de ETL...")

    # Etapa 1: Extração
    print("[INFO] Etapa 1: Extração")
    extract_run()

    # Etapa 2: Validação do XML
    print("[INFO] Etapa 2: Validação do XML")
    xml_file_path = "./dados_b3/SPRE250923/BVBG.186.01_BV000471202509230001000061919292430.xml"
    validate_xml(xml_file_path)

    # Etapa 3: Transformação e Carga
    print("[INFO] Etapa 3: Transformação e Carga")
    transform()

    print("[INFO] Processo de ETL concluído com sucesso!")