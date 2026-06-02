import logging
import os

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH = "/opt/airflow/data/gold"


def ler_silver() -> pd.DataFrame:
    """Lê as partições da camada Silver e retorna um DataFrame unificado e ordenado."""
    if not os.path.exists(SILVER_PATH):
        raise FileNotFoundError(f"Diretório Silver não encontrado: {SILVER_PATH}")

    particoes = [
        d
        for d in os.listdir(SILVER_PATH)
        if os.path.isdir(os.path.join(SILVER_PATH, d)) and d.startswith("ano=")
    ]

    if not particoes:
        raise ValueError(f"Nenhuma partição encontrada em: {SILVER_PATH}")

    fragmentos = []
    for particao in sorted(particoes):
        caminho_parquet = os.path.join(SILVER_PATH, particao, "selic.parquet")
        if not os.path.exists(caminho_parquet):
            logger.warning(
                f"Arquivo não encontrado, pulando partição: {caminho_parquet}"
            )
            continue
        df_part = pd.read_parquet(caminho_parquet, engine="pyarrow")
        fragmentos.append(df_part)
        logger.info(f"Partição lida: {caminho_parquet} ({len(df_part)} registros)")

    if not fragmentos:
        raise ValueError("Nenhum dado foi carregado da camada Silver.")

    df = pd.concat(fragmentos, ignore_index=True)
    df = df.sort_values("data").reset_index(drop=True)
    logger.info(f"Total de registros lidos da Silver: {len(df)}")
    return df


def _taxa_acumulada(taxas: pd.Series) -> float:
    """Função interna para calcular o juro composto acumulado de uma série de taxas."""
    return float((np.prod(1 + taxas / 100) - 1) * 100)


def calcular_acumulado_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega a taxa Selic por ano e mês, calculando o juro acumulado do período."""
    df = df.copy()
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month

    df_mensal = (
        df.groupby(["ano", "mes"])
        .agg(
            taxa_acumulada_mensal=("valor", _taxa_acumulada),
            qtd_dias_uteis=("valor", "count"),
        )
        .reset_index()
        .sort_values(["ano", "mes"])
        .reset_index(drop=True)
    )

    df_mensal["taxa_acumulada_mensal"] = df_mensal["taxa_acumulada_mensal"].round(4)

    logger.info(f"Acumulado mensal calculado: {len(df_mensal)} registros (ano/mês)")
    return df_mensal


def calcular_acumulado_anual(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega a taxa Selic por ano, calculando o juro acumulado e a média diária."""
    df = df.copy()
    df["ano"] = df["data"].dt.year

    df_anual = (
        df.groupby("ano")
        .agg(
            taxa_acumulada_anual=("valor", _taxa_acumulada),
            taxa_media_diaria=("valor", "mean"),
            qtd_dias_uteis=("valor", "count"),
        )
        .reset_index()
        .sort_values("ano")
        .reset_index(drop=True)
    )

    df_anual["taxa_acumulada_anual"] = df_anual["taxa_acumulada_anual"].round(4)
    df_anual["taxa_media_diaria"] = df_anual["taxa_media_diaria"].round(6)

    logger.info(f"Acumulado anual calculado: {len(df_anual)} registros (ano)")
    return df_anual


def salvar_gold(df: pd.DataFrame, nome_tabela: str) -> None:
    """Grava as tabelas agregadas finais no diretório da camada Gold."""
    if df.empty:
        logger.warning(f"Nenhum dado para salvar na tabela Gold: {nome_tabela}")
        return

    diretorio = os.path.join(GOLD_PATH, nome_tabela)
    os.makedirs(diretorio, exist_ok=True)

    caminho_arquivo = os.path.join(diretorio, f"{nome_tabela}.parquet")
    df.to_parquet(path=caminho_arquivo, engine="pyarrow", index=False)
    logger.info(
        f"Tabela '{nome_tabela}' salva em: {caminho_arquivo} ({len(df)} registros)"
    )


def process_gold_selic() -> None:
    """Orquestrador principal para geração dos indicadores analíticos da Gold."""
    logger.info("Iniciando processamento da camada Gold")

    df_silver = ler_silver()
    df_mensal = calcular_acumulado_mensal(df_silver)
    df_anual = calcular_acumulado_anual(df_silver)

    salvar_gold(df_mensal, "acumulado_mensal")
    salvar_gold(df_anual, "acumulado_anual")

    logger.info("Carga da camada Gold concluída!")


if __name__ == "__main__":
    process_gold_selic()
