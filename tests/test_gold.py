import numpy as np
import pandas as pd
import pytest

from src.gold import agregar_selic as gold


def test_taxa_acumulada_calculates_compound_interest():
    result = gold._taxa_acumulada(pd.Series([10.0, 20.0]))

    assert result == pytest.approx(32.0)


def test_calcular_acumulado_mensal_groups_by_month_and_counts_days():
    df = pd.DataFrame(
        {
            "data": pd.to_datetime(["2024-02-01", "2024-01-02", "2024-01-03"]),
            "valor": [2.0, 10.0, 20.0],
        }
    )

    result = gold.calcular_acumulado_mensal(df)

    assert result[["ano", "mes"]].to_dict("records") == [
        {"ano": 2024, "mes": 1},
        {"ano": 2024, "mes": 2},
    ]
    assert result["qtd_dias_uteis"].tolist() == [2, 1]
    assert result["taxa_acumulada_mensal"].tolist() == [32.0, 2.0]


def test_calcular_acumulado_anual_calculates_compound_rate_and_average():
    df = pd.DataFrame(
        {
            "data": pd.to_datetime(["2023-12-29", "2024-01-02", "2024-01-03"]),
            "valor": [5.0, 10.0, 20.0],
        }
    )

    result = gold.calcular_acumulado_anual(df)

    assert result["ano"].tolist() == [2023, 2024]
    assert result["taxa_acumulada_anual"].tolist() == [5.0, 32.0]
    assert result["taxa_media_diaria"].tolist() == [5.0, 15.0]
    assert result["qtd_dias_uteis"].tolist() == [1, 2]


def test_ler_silver_returns_sorted_rows_from_partitions(tmp_path, monkeypatch):
    (tmp_path / "ano=2023").mkdir()
    (tmp_path / "ano=2024").mkdir()
    pd.DataFrame({"data": pd.to_datetime(["2023-12-29"]), "valor": [5.0]}).to_parquet(
        tmp_path / "ano=2023" / "selic.parquet", index=False
    )
    pd.DataFrame({"data": pd.to_datetime(["2024-01-02"]), "valor": [10.0]}).to_parquet(
        tmp_path / "ano=2024" / "selic.parquet", index=False
    )
    monkeypatch.setattr(gold, "SILVER_PATH", str(tmp_path))

    result = gold.ler_silver()

    assert result["data"].tolist() == [pd.Timestamp("2023-12-29"), pd.Timestamp("2024-01-02")]


def test_ler_silver_raises_when_no_data_is_available(tmp_path, monkeypatch):
    monkeypatch.setattr(gold, "SILVER_PATH", str(tmp_path))
    with pytest.raises(ValueError, match="Nenhuma partição"):
        gold.ler_silver()

    (tmp_path / "ano=2024").mkdir()
    with pytest.raises(ValueError, match="Nenhum dado"):
        gold.ler_silver()


def test_salvar_gold_writes_named_table(tmp_path, monkeypatch):
    monkeypatch.setattr(gold, "GOLD_PATH", str(tmp_path))
    df = pd.DataFrame({"ano": [2024], "valor": [np.float64(1.23)]})

    gold.salvar_gold(df, "acumulado_anual")

    output_path = tmp_path / "acumulado_anual" / "acumulado_anual.parquet"
    assert output_path.exists()
    pd.testing.assert_frame_equal(pd.read_parquet(output_path), df)
