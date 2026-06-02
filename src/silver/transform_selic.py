import logging
import os

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"

ANO_INICIAL = 2020
ANO_FINAL = 2024
VALOR_MIN = 0.0
VALOR_MAX = 100.0


def ler_bronze() -> pd.DataFrame:
    fragmentos = []

    if not os.path.exists(BRONZE_PATH):
        raise FileNotFoundError(f"Diretório Bronze não encontrado: {BRONZE_PATH}")

    particoes = [
        d
        for d in os.listdir(BRONZE_PATH)
        if os.path.isdir(os.path.join(BRONZE_PATH, d)) and d.startswith("ano=")
    ]

    if not particoes:
        raise ValueError(f"Nenhuma partição encontrada em: {BRONZE_PATH}")

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
    df = df.copy()

    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    qtd_data_invalida = df["data"].isna().sum()
    if qtd_data_invalida:
        logger.warning(f"Datas que não puderam ser convertidas: {qtd_data_invalida}")

    df["valor"] = df["valor"].astype(str).str.strip().str.replace(",", ".", regex=False)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    qtd_valor_invalido = df["valor"].isna().sum()
    if qtd_valor_invalido:
        logger.warning(
            f"Valores que não puderam ser convertidos para float: {qtd_valor_invalido}"
        )

    return df


def validar(df: pd.DataFrame) -> pd.DataFrame:
    motivos_invalido = pd.Series([""] * len(df), index=df.index)

    mask_data_nula = df["data"].isna()
    motivos_invalido[mask_data_nula] += "[data_nula]"

    mask_data_fora_range = df["data"].notna() & (
        (df["data"].dt.year < ANO_INICIAL) | (df["data"].dt.year > ANO_FINAL)
    )
    motivos_invalido[mask_data_fora_range] += (
        f"[data_fora_range_{ANO_INICIAL}_{ANO_FINAL}]"
    )

    mask_valor_nulo = df["valor"].isna()
    motivos_invalido[mask_valor_nulo] += "[valor_nulo]"

    mask_valor_negativo = df["valor"].notna() & (df["valor"] < VALOR_MIN)
    motivos_invalido[mask_valor_negativo] += "[valor_negativo]"

    mask_valor_fora_range = df["valor"].notna() & (df["valor"] > VALOR_MAX)
    motivos_invalido[mask_valor_fora_range] += f"[valor_acima_{VALOR_MAX}]"

    mask_duplicata = df.duplicated(subset=["data"], keep="first")
    motivos_invalido[mask_duplicata] += "[data_duplicada]"

    mask_invalido = motivos_invalido != ""
    df_invalido = df[mask_invalido].copy()
    df_invalido["motivo_rejeicao"] = motivos_invalido[mask_invalido]
    df_valido = df[~mask_invalido].copy()

    logger.info(f"Registros válidos:   {len(df_valido)}")
    logger.info(f"Registros inválidos: {len(df_invalido)}")

    if not df_invalido.empty:
        contagem_motivos = (
            df_invalido["motivo_rejeicao"]
            .str.extractall(r"(\[[^\]]+\])")[0]
            .value_counts()
        )
        logger.warning("Distribuição dos motivos de rejeição:")
        for motivo, qtd in contagem_motivos.items():
            logger.warning(f"  {motivo}: {qtd} registro(s)")

    return df_valido


def salvar_silver(df_valido: pd.DataFrame) -> None:
    if df_valido.empty:
        logger.warning("Nenhum dado válido para salvar na Silver.")
        return

    anos_presentes = df_valido["data"].dt.year.unique()

    for ano in sorted(anos_presentes):
        ano_dir = os.path.join(SILVER_PATH, f"ano={ano}")
        os.makedirs(ano_dir, exist_ok=True)

        df_ano = df_valido[df_valido["data"].dt.year == ano].copy()
        caminho_arquivo = os.path.join(ano_dir, "selic.parquet")
        df_ano.to_parquet(path=caminho_arquivo, engine="pyarrow", index=False)
        logger.info(
            f"Dados válidos do ano {ano} salvos em: {caminho_arquivo} ({len(df_ano)} registros)"
        )


def process_silver_selic():
    logger.info("Iniciando Processamento da Camada Silver")

    try:
        df_bronze = ler_bronze()
    except Exception as e:
        logger.error(f"Falha na leitura da Bronze: {e}")
        raise

    try:
        df_transformado = transformar(df_bronze)
    except Exception as e:
        logger.error(f"Falha na transformação dos dados: {e}")
        raise

    try:
        df_valido = validar(df_transformado)
    except Exception as e:
        logger.error(f"Falha na validação dos dados: {e}")
        raise

    try:
        salvar_silver(df_valido)
    except Exception as e:
        logger.error(f"Falha ao salvar dados na Silver: {e}")
        raise

    logger.info("Carga da Camada Silver Concluída!")


if __name__ == "__main__":
    process_silver_selic()
