import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.periodo_selic import obter_periodo

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BRONZE_PATH = "/opt/airflow/data/bronze"
URL_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
FORMATO = "json"


def fetch_selic_data(
    url_base: str, formato: str, data_inicial: str, data_final: str
) -> List[Dict[str, Any]]:
    """Extrai a série em janelas inferiores a dez anos, sem lacunas."""
    inicio = datetime.strptime(data_inicial, "%d/%m/%Y").date()
    fim = datetime.strptime(data_final, "%d/%m/%Y").date()
    if inicio > fim:
        raise ValueError("Data inicial posterior à data final")
    limite = inicio.replace(year=inicio.year + 10, day=1) - timedelta(days=1)
    if fim > limite:
        dados = []
        while inicio <= fim:
            limite = inicio.replace(year=inicio.year + 10, day=1) - timedelta(days=1)
            final_janela = min(fim, limite)
            dados.extend(fetch_selic_data(
                url_base, formato, inicio.strftime("%d/%m/%Y"),
                final_janela.strftime("%d/%m/%Y"),
            ))
            inicio = final_janela + timedelta(days=1)
        return dados
    url = f"{url_base}?formato={formato}&dataInicial={data_inicial}&dataFinal={data_final}"

    try:
        logger.info(f"Iniciando requisição GET para a URL: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
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


def transform_selic_data(dados_json: List[Dict[str, Any]]) -> pd.DataFrame:
    """Estrutura os dados em um DataFrame e adiciona metadados."""
    if not dados_json:
        logger.warning("A API retornou uma lista de dados vazia.")
        return pd.DataFrame()

    try:
        df = pd.DataFrame(dados_json)
        logger.info(f"Quantidade de registros recuperados: {len(df)}")

        df["ano"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.year
        df["timestamp_ingestao"] = datetime.now().isoformat()

        return df
    except Exception as e:
        logger.error(f"Erro ao processar a estrutura dos dados no Pandas: {e}")
        raise


def save_data_by_year(df: pd.DataFrame, base_path: str) -> None:
    """Particiona e salva os dados em formato Parquet por ano."""
    if df.empty:
        logger.warning("DataFrame vazio. Nenhum arquivo será salvo.")
        return

    try:
        for ano, df_ano in df.groupby("ano"):
            ano_dir = os.path.join(base_path, f"ano={ano}")
            os.makedirs(ano_dir, exist_ok=True)

            caminho_arquivo = os.path.join(ano_dir, "selic.parquet")

            df_salvar = df_ano.drop(columns=["ano"])

            df_salvar.to_parquet(path=caminho_arquivo, engine="pyarrow", index=False)
            logger.info(f"Dados do ano {ano} salvos com sucesso em: {caminho_arquivo}")

        logger.info("Carga da Camada Bronze Concluída!")
    except Exception as e:
        logger.error(f"Erro ao salvar os arquivos Parquet estruturados: {e}")
        raise


def pipeline_ingest_selic() -> None:
    """Orquestrador do pipeline de ingestão da Selic."""
    inicio, fim = obter_periodo()
    dados_brutos = fetch_selic_data(
        URL_BASE, FORMATO, inicio.strftime("%d/%m/%Y"), fim.strftime("%d/%m/%Y")
    )
    df_processado = transform_selic_data(dados_brutos)
    save_data_by_year(df_processado, BRONZE_PATH)


if __name__ == "__main__":
    pipeline_ingest_selic()
