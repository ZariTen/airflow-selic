import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

GOLD_PATH = "/opt/airflow/data/gold"


def validar_camada_gold():
    logger.info("Iniciando a validação dos dados na camada Gold.")

    tabelas = ["acumulado_mensal", "acumulado_anual"]
    total_linhas_validadas = 0

    for tabela in tabelas:
        caminho_diretorio = os.path.join(GOLD_PATH, tabela)
        caminho_arquivo = os.path.join(caminho_diretorio, f"{tabela}.parquet")

        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(
                f"Erro de Validação: O arquivo {caminho_arquivo} não foi encontrado."
            )

        if os.path.getsize(caminho_arquivo) == 0:
            raise ValueError(
                f"Erro de Validação: O arquivo {caminho_arquivo} está vazio (0 bytes)."
            )

        df = pd.read_parquet(caminho_arquivo, engine="pyarrow")
        linhas_atuais = len(df)
        total_linhas_validadas += linhas_atuais

        logger.info(
            f"Tabela '{tabela}' carregada com sucesso. Registros: {linhas_atuais}"
        )

        colunas_criticas = ["ano"]
        if "mes" in df.columns:
            colunas_criticas.append("mes")

        for col in colunas_criticas:
            nulos = df[col].isnull().sum()
            if nulos > 0:
                raise ValueError(
                    f"Erro de Validação: Coluna crítica '{col}' na tabela '{tabela}' contém {nulos} valores nulos."
                )

        anos_obrigatorios = set(range(2020, 2025))
        anos_no_df = set(df["ano"].unique())

        anos_faltantes = anos_obrigatorios - anos_no_df
        if anos_faltantes:
            raise ValueError(
                f"Erro de Validação: A tabela '{tabela}' não possui cobertura temporal completa. Anos faltantes: {anos_faltantes}"
            )

    logger.info(f"Total de linhas combinadas encontradas: {total_linhas_validadas}")
    if total_linhas_validadas < 50:
        raise ValueError(
            f"Erro de Validação: Volumetria abaixo do esperado. Esperado >= 50, encontrado: {total_linhas_validadas}"
        )

    logger.info(f" Tabelas validadas: {', '.join(tabelas)}")
    logger.info(f" Total de registros auditados: {total_linhas_validadas}")


with DAG(
    dag_id="sub_validacao_gold",
    description="DAG de validação de qualidade de dados para a camada Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["subdag", "validacao", "gold"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:
    validar_dados = PythonOperator(
        task_id="validar_dados_gold",
        python_callable=validar_camada_gold,
    )
