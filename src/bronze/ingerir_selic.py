import logging
import os
from datetime import datetime

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BRONZE_PATH = "/opt/airflow/data/bronze"
URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
FORMATO = "json"
DATA_INICIAL = "01/01/2020"
DATA_FINAL = "31/12/2024"


def ingest_selic_data_pure():
    url = f"{URL_BASE}?formato={FORMATO}&dataInicial={DATA_INICIAL}&dataFinal={DATA_FINAL}"

    try:
        logger.info(f"Iniciando requisição GET para a URL: {url}")
        response = requests.get(url, timeout=30)

        response.raise_for_status()

        dados_json = response.json()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"Erro HTTP encontrado: {http_err}")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Erro de Conexão encontrado: {conn_err}")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Erro de Timeout encontrado: {timeout_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Erro genérico na requisição: {req_err}")
        raise
    except ValueError as json_err:
        logger.error(f"Erro ao decodificar o JSON: {json_err}")
        raise

    try:
        df = pd.DataFrame(dados_json)

        if df.empty:
            logger.warning("A API retornou uma lista de dados vazia.")
            return

        timestamp_ingestao = datetime.now().isoformat()
        qtd_registros = len(df)

        logger.info(f"Quantidade de registros recuperados: {qtd_registros}")

        anos_presentes = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.year.unique()
        df["timestamp_ingestao"] = timestamp_ingestao

    except Exception as e:
        logger.error(f"Erro ao processar a estrutura dos dados no Pandas: {e}")
        raise

    try:
        for ano in anos_presentes:
            ano_dir = os.path.join(BRONZE_PATH, f"ano={ano}")
            os.makedirs(ano_dir, exist_ok=True)

            mascara_ano = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.year == ano
            df_ano = df[mascara_ano]

            caminho_arquivo = os.path.join(ano_dir, "selic.parquet")

            df_ano.to_parquet(path=caminho_arquivo, engine="pyarrow", index=False)
            logger.info(f"Dados do ano {ano} salvos com sucesso em: {caminho_arquivo}")

        logger.info("Carga da Camada Bronze Concluída!")

    except Exception as e:
        logger.error(f"Erro ao salvar os arquivos Parquet estruturados: {e}")
        raise


if __name__ == "__main__":
    ingest_selic_data_pure()
