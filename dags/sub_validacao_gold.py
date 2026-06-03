import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger("airflow.task")

GOLD_PATH = "/opt/airflow/data/gold"
TABELAS_ESPERADAS = ["acumulado_mensal", "acumulado_anual"]
ANOS_OBRIGATORIOS = set(range(2020, 2025))
VOLUMETRIA_MINIMA = 50


def _on_failure_callback(context: dict) -> None:
    """Loga informações estruturadas quando a validação Gold falha."""
    logger.error(
        "Falha na validação Gold | dag_id=%s | task_id=%s | execution_date=%s | "
        "tentativa=%s | log_url=%s",
        context.get("dag").dag_id,
        context.get("task_instance").task_id,
        context.get("execution_date"),
        context.get("task_instance").try_number,
        context.get("task_instance").log_url,
    )


def _validar_tabela(tabela: str) -> int:
    """
    Executa todas as checagens de qualidade para uma tabela Gold.
    Retorna a quantidade de linhas validadas.
    """
    caminho_diretorio = os.path.join(GOLD_PATH, tabela)
    caminho_arquivo = os.path.join(caminho_diretorio, f"{tabela}.parquet")

    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")

    if os.path.getsize(caminho_arquivo) == 0:
        raise ValueError(f"Arquivo vazio (0 bytes): {caminho_arquivo}")

    df = pd.read_parquet(caminho_arquivo, engine="pyarrow")
    linhas = len(df)
    logger.info("Tabela '%s' carregada: %d registros", tabela, linhas)

    colunas_criticas = ["ano"] + (["mes"] if "mes" in df.columns else [])
    for col in colunas_criticas:
        nulos = df[col].isnull().sum()
        if nulos > 0:
            raise ValueError(
                f"Coluna crítica '{col}' na tabela '{tabela}' "
                f"contém {nulos} valor(es) nulo(s)"
            )

    anos_presentes = set(df["ano"].unique())
    anos_faltantes = ANOS_OBRIGATORIOS - anos_presentes
    if anos_faltantes:
        raise ValueError(
            f"Tabela '{tabela}' sem cobertura completa. "
            f"Anos faltantes: {sorted(anos_faltantes)}"
        )

    return linhas


def validar_camada_gold() -> None:
    """
    Valida existência, integridade e cobertura temporal de todas as tabelas Gold.
    Falha de forma estruturada com log completo em cada etapa.
    """
    logger.info("Iniciando validação da camada Gold")
    total_linhas = 0
    erros = []

    for tabela in TABELAS_ESPERADAS:
        try:
            linhas = _validar_tabela(tabela)
            total_linhas += linhas
            logger.info(
                "Tabela '%s' validada com sucesso: %d registros", tabela, linhas
            )
        except (FileNotFoundError, ValueError) as exc:
            logger.error("Falha na validação da tabela '%s': %s", tabela, exc)
            erros.append(str(exc))
        except Exception as exc:
            logger.error(
                "Erro inesperado ao validar tabela '%s': %s",
                tabela,
                exc,
                exc_info=True,
            )
            erros.append(f"[{tabela}] Erro inesperado: {exc}")

    if erros:
        raise ValueError(
            f"Validação Gold encerrada com {len(erros)} erro(s):\n"
            + "\n".join(f"  - {e}" for e in erros)
        )

    if total_linhas < VOLUMETRIA_MINIMA:
        raise ValueError(
            f"Volumetria total abaixo do mínimo esperado. "
            f"Esperado >= {VOLUMETRIA_MINIMA}, encontrado: {total_linhas}"
        )

    logger.info(
        "Validação Gold concluída. Tabelas: %s | Total de registros: %d",
        ", ".join(TABELAS_ESPERADAS),
        total_linhas,
    )


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="sub_validacao_gold",
    description="DAG de validação de qualidade de dados para a camada Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["subdag", "validacao", "gold"],
    default_args=DEFAULT_ARGS,
) as dag:
    validar_dados = PythonOperator(
        task_id="validar_dados_gold",
        python_callable=validar_camada_gold,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
