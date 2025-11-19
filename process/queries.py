from .db_connection import get_connection, get_sqlalchemy_engine

import pandas as pd
from tqdm import tqdm


def insert_pregao(ativo, data_pregao, abertura=None, fechamento=None, volume=None):
    """Insere um registro na tabela `Cotacoes`.
    Parâmetros: ativo, data_pregao, abertura, fechamento, volume
    """
    try:
        connection = get_connection()
        cursor = connection.cursor()
        sql = """
            INSERT INTO Cotacoes (Ativo, DataPregao, Abertura, Fechamento, Volume)
            VALUES (?, ?, ?, ?, ?)
        """
        values = (ativo, data_pregao, abertura, fechamento, volume)
        cursor.execute(sql, values)
        connection.commit()
        print(f"Registro inserido com sucesso: {ativo}, {data_pregao}")
    except Exception as err:
        print(f"[ERRO] Não foi possível inserir o registro: {err}")
    finally:
        try:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if 'connection' in locals() and connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        except Exception:
            pass


def insert_pregao_bulk(records, table_name="Cotacoes", chunksize: int = 1000):
    """Bulk insert using pandas.to_sql in chunked mode with a progress bar.

    - `records` can be a list of dicts or a pandas.DataFrame.
    - The function will create an SQLAlchemy engine from environment vars via
      `db_connection.get_sqlalchemy_engine()`.
    """
   
    if isinstance(records, pd.DataFrame):
        df = records.copy()
    else:
        df = pd.DataFrame(records)

    if df.empty:
        print("Nenhum registro para inserir.")
        return 0


    expected_cols = [
        "Ativo",
        "DataPregao",
        "Abertura",
        "Fechamento",
        "Volume",
    ]
  
    cols = [c for c in expected_cols if c in df.columns]
    if not cols:
        raise ValueError("DataFrame não contém colunas esperadas para inserção.")

    df = df[cols]

    try:
        engine = get_sqlalchemy_engine()
    except Exception as e:
        print(f"[ERRO] Não foi possível criar engine SQLAlchemy: {e}")
        raise

    total = len(df)
    inserted = 0
    try:
        with engine.begin() as conn:
        
            with tqdm(total=total, unit="rows") as pbar:
                for start in range(0, total, chunksize):
                    end = start + chunksize
                    chunk = df.iloc[start:end]
                   
                    chunk.to_sql(name=table_name, con=conn, if_exists="append", index=False)
                    inserted += len(chunk)
                    pbar.update(len(chunk))
    finally:
        try:
            engine.dispose()
        except Exception:
            pass

    print(f"Inserção bulk concluída: {inserted} registros inseridos na tabela '{table_name}'.")
    return inserted


def select_all_pregao():
    """Busca todos os registros da tabela 'Cotacoes' via SQL puro.
    Returns rows with explicit column order: Id, Ativo, DataPregao, Abertura, Fechamento, Volume
    """
    try:
        connection = get_connection()
        if connection:
            cursor = connection.cursor()
            sql = (
                "SELECT Id, Ativo, DataPregao, Abertura, Fechamento, Volume "
                "FROM Cotacoes"
            )
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            connection.close()
            return result
    except Exception as e:
        print(f"[ERRO] Não foi possível buscar os dados: {e}")
        return []