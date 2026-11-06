import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, Easter
from pandas.tseries.offsets import Day
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.periodo_selic import obter_periodo

logger = logging.getLogger("airflow.task")

GOLD_PATH = "/opt/airflow/data/gold"
TABELAS_ESPERADAS = ["acumulado_mensal", "acumulado_anual"]


class CalendarioBancario(AbstractHolidayCalendar):
    """Feriados nacionais que podem adiar a primeira observação de um período."""
    rules = [
        Holiday("Ano Novo", month=1, day=1),
        Holiday("Tiradentes", month=4, day=21),
        Holiday("Dia do Trabalho", month=5, day=1),
        Holiday("Independência", month=9, day=7),
        Holiday("Nossa Senhora Aparecida", month=10, day=12),
        Holiday("Finados", month=11, day=2),
        Holiday("Proclamação da República", month=11, day=15),
        Holiday("Consciência Negra", month=11, day=20, start_date="2024-11-20"),
        Holiday("Natal", month=12, day=25),
        Holiday("Carnaval segunda", month=1, day=1, offset=[Easter(), Day(-48)]),
        Holiday("Carnaval terça", month=1, day=1, offset=[Easter(), Day(-47)]),
        Holiday("Sexta-feira Santa", month=1, day=1, offset=[Easter(), Day(-2)]),
        Holiday("Corpus Christi", month=1, day=1, offset=[Easter(), Day(60)]),
    ]


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
    inicio, fim = obter_periodo()
    # O dia final pode ainda não ter sido publicado pelo BCB.
    dias = pd.bdate_range(inicio, fim - timedelta(days=1))
    feriados = CalendarioBancario().holidays(inicio, fim)
    dias = dias[~dias.isin(feriados)]
    meses_esperados = set(zip(dias.year, dias.month))
    anos_faltantes = {ano for ano, _ in meses_esperados} - anos_presentes
    if anos_faltantes:
        raise ValueError(
            f"Tabela '{tabela}' sem cobertura completa. "
            f"Anos faltantes: {sorted(anos_faltantes)}"
        )

    if tabela == "acumulado_mensal":
        meses_presentes = set(zip(df["ano"], df["mes"]))
        meses_faltantes = meses_esperados - meses_presentes
        if meses_faltantes:
            raise ValueError(
                f"Tabela '{tabela}' com meses faltantes: {sorted(meses_faltantes)}"
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
    max_active_runs=1,
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
