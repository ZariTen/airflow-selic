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
    fragmentos = []

    if not os.path.exists(SILVER_PATH):
        raise FileNotFoundError(f"Diretório Silver não encontrado: {SILVER_PATH}")

    particoes = [
        d
        for d in os.listdir(SILVER_PATH)
        if os.path.isdir(os.path.join(SILVER_PATH, d)) and d.startswith("ano=")
    ]

    if not particoes:
        raise ValueError(f"Nenhuma partição encontrada em: {SILVER_PATH}")

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
    logger.info(f"Total de registros lidos da Silver: {len(df)}")
    return df


def calcular_media_mensal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month

    def acumulada_mensal(taxas):
        return (np.prod(1 + taxas / 100) - 1) * 100

    df_mensal = (
        df.groupby(["ano", "mes"])
        .agg(
            taxa_acumulada_mensal=("valor", acumulada_mensal),
            qtd_registros=("valor", "count"),
        )
        .reset_index()
        .sort_values(["ano", "mes"])
    )

    df_mensal["taxa_acumulada_mensal"] = df_mensal["taxa_acumulada_mensal"].round(4)

    logger.info(f"Taxa acumulada mensal calculada: {len(df_mensal)} registros (ano/mês)")
    return df_mensal
    return df_mensal


def calcular_variacao_anual(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ano"] = df["data"].dt.year

    def acumulada(taxas):
        return np.prod(1 + taxas / 100) - 1

    df_anual = (
        df.groupby("ano")
        .agg(
            taxa_acumulada_anual=("valor", acumulada),
            taxa_media_anual=("valor", "mean"),
            qtd_dias_uteis=("valor", "count"),
        )
        .reset_index()
        .sort_values("ano")
    )

    df_anual["taxa_acumulada_anual"] = df_anual["taxa_acumulada_anual"].round(6)
    df_anual["taxa_media_anual"] = df_anual["taxa_media_anual"].round(4)

    logger.info(f"Variação acumulada anual calculada: {len(df_anual)} registros (ano)")
    return df_anual


def salvar_gold(df: pd.DataFrame, nome_tabela: str) -> None:
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


def process_gold_selic():
    logger.info("Iniciando Processamento da Camada Gold")

    try:
        df_silver = ler_silver()
    except Exception as e:
        logger.error(f"Falha na leitura da Silver: {e}")
        raise

    try:
        df_media_mensal = calcular_media_mensal(df_silver)
    except Exception as e:
        logger.error(f"Falha no cálculo da média mensal: {e}")
        raise

    try:
        df_variacao_anual = calcular_variacao_anual(df_silver)
    except Exception as e:
        logger.error(f"Falha no cálculo da variação acumulada anual: {e}")
        raise

    try:
        salvar_gold(df_media_mensal, "media_mensal")
        salvar_gold(df_variacao_anual, "variacao_acumulada_anual")
    except Exception as e:
        logger.error(f"Falha ao salvar dados na Gold: {e}")
        raise

    logger.info("Carga da Camada Gold Concluída!")


if __name__ == "__main__":
    process_gold_selic()
