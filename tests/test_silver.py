import pandas as pd
import pytest

from src.silver import transform_selic as silver


def test_transformar_parses_dates_and_normalizes_decimal_values():
    result = silver.transformar(
        pd.DataFrame(
            {
                "data": ["02/01/2024", "03/01/2024", "invalid"],
                "valor": [" 11,65 ", "12.10", "not-a-number"],
            }
        )
    )

    assert pd.api.types.is_datetime64_any_dtype(result["data"])
    assert result["data"].iloc[:2].tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
    ]
    assert result["valor"].iloc[:2].tolist() == [11.65, 12.10]
    assert pd.isna(result["data"].iloc[2])
    assert pd.isna(result["valor"].iloc[2])


def test_validar_separates_valid_records_and_records_all_rejection_reasons(monkeypatch):
    monkeypatch.setenv("SELIC_DATA_INICIAL", "01/01/2020")
    monkeypatch.setenv("SELIC_DATA_FINAL", "31/12/2024")
    df = pd.DataFrame(
        {
            "data": pd.to_datetime(
                ["2024-01-02", "2024-01-02", "2019-12-31", "2024-01-04", "2024-01-05", None]
            ),
            "valor": [10.0, 11.0, 10.0, -1.0, 101.0, None],
        }
    )

    valid, invalid = silver.validar(df)

    assert len(valid) == 1
    assert valid.iloc[0]["data"] == pd.Timestamp("2024-01-02")
    assert len(invalid) == 5
    duplicate_reason = invalid.loc[
        invalid["data"].eq(pd.Timestamp("2024-01-02")), "motivo_rejeicao"
    ]
    assert duplicate_reason.str.contains(r"\[data_duplicada\]").any()
    assert "[data_fora_range_2020_2024]" in invalid.loc[
        invalid["data"].eq(pd.Timestamp("2019-12-31")), "motivo_rejeicao"
    ].iloc[0]
    assert "[valor_negativo]" in invalid.loc[
        invalid["data"].eq(pd.Timestamp("2024-01-04")), "motivo_rejeicao"
    ].iloc[0]
    assert "[valor_acima_100.0]" in invalid.loc[
        invalid["data"].eq(pd.Timestamp("2024-01-05")), "motivo_rejeicao"
    ].iloc[0]
    null_reason = invalid[invalid["data"].isna()]["motivo_rejeicao"].iloc[0]
    assert "[data_nula]" in null_reason
    assert "[valor_nulo]" in null_reason


def test_ler_bronze_loads_existing_partitions_and_skips_missing_files(tmp_path, monkeypatch):
    (tmp_path / "ano=2023").mkdir()
    (tmp_path / "ano=2024").mkdir()
    pd.DataFrame({"data": ["02/01/2023"], "valor": [11.65]}).to_parquet(
        tmp_path / "ano=2023" / "selic.parquet", index=False
    )
    monkeypatch.setattr(silver, "BRONZE_PATH", str(tmp_path))

    result = silver.ler_bronze()

    assert result.to_dict("records") == [{"data": "02/01/2023", "valor": 11.65}]


def test_ler_bronze_raises_when_directory_or_partitions_are_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(silver, "BRONZE_PATH", str(tmp_path / "missing"))
    with pytest.raises(FileNotFoundError):
        silver.ler_bronze()

    monkeypatch.setattr(silver, "BRONZE_PATH", str(tmp_path))
    with pytest.raises(ValueError, match="Nenhuma partição"):
        silver.ler_bronze()


def test_salvar_silver_partitions_valid_rows_by_year(tmp_path, monkeypatch):
    monkeypatch.setattr(silver, "SILVER_PATH", str(tmp_path))
    df = pd.DataFrame(
        {
            "data": pd.to_datetime(["2023-01-02", "2024-01-03"]),
            "valor": [11.65, 11.75],
        }
    )

    silver.salvar_silver(df)

    assert len(pd.read_parquet(tmp_path / "ano=2023" / "selic.parquet")) == 1
    assert len(pd.read_parquet(tmp_path / "ano=2024" / "selic.parquet")) == 1
    assert "ano_particao" not in pd.read_parquet(tmp_path / "ano=2023" / "selic.parquet").columns


def test_salvar_quarentena_accumulates_rejected_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(silver, "QUARENTENA_PATH", str(tmp_path))
    first = pd.DataFrame({"data": [pd.NaT], "valor": [None], "motivo_rejeicao": ["[data_nula]"]})
    second = pd.DataFrame({"data": [pd.Timestamp("2024-01-02")], "valor": [-1.0], "motivo_rejeicao": ["[valor_negativo]"]})

    silver.salvar_quarentena(first)
    silver.salvar_quarentena(second)

    output = pd.read_parquet(tmp_path / "selic_rejeitados.parquet")
    assert len(output) == 2
    assert output["motivo_rejeicao"].tolist() == ["[data_nula]", "[valor_negativo]"]
