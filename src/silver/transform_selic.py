import logging
import os
from typing import Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"
QUARENTENA_PATH = "/opt/airflow/data/quarentena"

ANO_INICIAL = 2020
ANO_FINAL = 2024
VALOR_MIN = 0.0
VALOR_MAX = 100.0


def ler_bronze() -> pd.DataFrame:
    """Lê recursivamente as partições no diretório Bronze e consolida num DataFrame."""
    if not os.path.exists(BRONZE_PATH):
        raise FileNotFoundError(f"Diretório Bronze não encontrado: {BRONZE_PATH}")

    particoes = [
        d
        for d in os.listdir(BRONZE_PATH)
        if os.path.isdir(os.path.join(BRONZE_PATH, d)) and d.startswith("ano=")
    ]

    if not particoes:
        raise ValueError(f"Nenhuma partição encontrada em: {BRONZE_PATH}")

    fragmentos = []
    for particao in sorted(particoes):
        caminho_parquet = os.path.join(BRONZE_PATH, particao, "selic.parquet")
        if not os.path.exists(caminho_parquet):
            logger.warning(
                f"Arquivo não encontrado, pulando partição: {caminho_parquet}"
            )
            continue
        df_part = pd.read_parquet(caminho_parquet, engine="pyarrow")
        fragmentos.append(df_part)
        logger.info(f"Partição lida: {caminho_parquet} ({len(df_part)} registros)")

    if not fragmentos:
        raise ValueError("Nenhum dado foi carregado da camada Bronze.")

    df = pd.concat(fragmentos, ignore_index=True)
    logger.info(f"Total de registros lidos da Bronze: {len(df)}")
    return df


def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica tipagem de dados e padronização de formatos."""
    df = df.copy()

    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    qtd_data_invalida = df["data"].isna().sum()
    if qtd_data_invalida:
        logger.warning(f"Datas que não puderam ser convertidas: {qtd_data_invalida}")

    df["valor"] = df["valor"].astype(str).str.strip().str.replace(",", ".", regex=False)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    qtd_valor_invalido = df["valor"].isna().sum()
    if qtd_valor_invalido:
        logger.warning(f"Valores que não puderam ser convertidos: {qtd_valor_invalido}")

    return df


def validar(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Valida regras de negócio da Silver e separa registros bons dos ruins."""
    df = df.sort_values("data", na_position="last").reset_index(drop=True)
    motivos = pd.Series("", index=df.index, dtype=str)

    motivos[df["data"].isna()] += "[data_nula]"

    mask_fora_range = df["data"].notna() & (
        (df["data"].dt.year < ANO_INICIAL) | (df["data"].dt.year > ANO_FINAL)
    )
    motivos[mask_fora_range] += f"[data_fora_range_{ANO_INICIAL}_{ANO_FINAL}]"

    motivos[df["valor"].isna()] += "[valor_nulo]"
    motivos[df["valor"].notna() & (df["valor"] < VALOR_MIN)] += "[valor_negativo]"
    motivos[df["valor"].notna() & (df["valor"] > VALOR_MAX)] += (
        f"[valor_acima_{VALOR_MAX}]"
    )

    mask_duplicata = df.duplicated(subset=["data"], keep="first")
    motivos[mask_duplicata] += "[data_duplicada]"

    mask_invalido = motivos != ""
    df_invalido = df[mask_invalido].copy()
    df_invalido["motivo_rejeicao"] = motivos[mask_invalido]
    df_valido = df[~mask_invalido].copy()

    logger.info(f"Registros válidos:   {len(df_valido)}")
    logger.info(f"Registros inválidos: {len(df_invalido)}")

    if not df_invalido.empty:
        contagem = (
            df_invalido["motivo_rejeicao"]
            .str.extractall(r"(\[[^\]]+\])")[0]
            .value_counts()
        )
        logger.warning("Distribuição dos motivos de rejeição:")
        for motivo, qtd in contagem.items():
            logger.warning(f"  {motivo}: {qtd} registro(s)")

    return df_valido, df_invalido


def salvar_silver(df_valido: pd.DataFrame) -> None:
    """Salva os dados válidos, particionando por ano de forma eficiente."""
    if df_valido.empty:
        logger.warning("Nenhum dado válido para salvar na Silver.")
        return

    df_valido = df_valido.copy()
    df_valido["ano_particao"] = df_valido["data"].dt.year

    for ano, df_ano in df_valido.groupby("ano_particao"):
        ano_dir = os.path.join(SILVER_PATH, f"ano={ano}")
        os.makedirs(ano_dir, exist_ok=True)

        caminho_arquivo = os.path.join(ano_dir, "selic.parquet")

        if os.path.exists(caminho_arquivo):
            logger.warning(f"Sobrescrevendo arquivo existente: {caminho_arquivo}")

        df_salvar = df_ano.drop(columns=["ano_particao"])
        df_salvar.to_parquet(path=caminho_arquivo, engine="pyarrow", index=False)
        logger.info(
            f"Silver {ano} salva: {caminho_arquivo} ({len(df_salvar)} registros)"
        )


def salvar_quarentena(df_invalido: pd.DataFrame) -> None:
    """Acumula e grava os logs de inconsistências na Quarentena."""
    if df_invalido.empty:
        return

    os.makedirs(QUARENTENA_PATH, exist_ok=True)
    caminho_arquivo = os.path.join(QUARENTENA_PATH, "selic_rejeitados.parquet")

    if os.path.exists(caminho_arquivo):
        df_existente = pd.read_parquet(caminho_arquivo, engine="pyarrow")
        df_invalido = pd.concat([df_existente, df_invalido], ignore_index=True)

    df_invalido.to_parquet(path=caminho_arquivo, engine="pyarrow", index=False)
    logger.warning(
        f"Registros rejeitados salvos em quarentena: {caminho_arquivo} "
        f"({len(df_invalido)} total acumulado)"
    )


def process_silver_selic() -> None:
    """Função principal coordenadora da camada Silver."""
    logger.info("Iniciando processamento da camada Silver")

    df_bronze = ler_bronze()
    df_transformado = transformar(df_bronze)
    df_valido, df_invalido = validar(df_transformado)

    salvar_silver(df_valido)
    salvar_quarentena(df_invalido)

    logger.info("Carga da camada Silver concluída!")


if __name__ == "__main__":
    process_silver_selic()
