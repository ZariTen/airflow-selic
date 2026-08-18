from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from src.bronze import ingerir_selic as bronze


def test_fetch_selic_data_builds_request_and_returns_json(monkeypatch):
    response = Mock()
    response.json.return_value = [{"data": "02/01/2024", "valor": "11,65"}]
    get = Mock(return_value=response)
    monkeypatch.setattr(bronze.requests, "get", get)

    result = bronze.fetch_selic_data("https://example.test/series", "json", "01/01/2024", "31/01/2024")

    assert result == [{"data": "02/01/2024", "valor": "11,65"}]
    get.assert_called_once_with(
        "https://example.test/series?formato=json&dataInicial=01/01/2024&dataFinal=31/01/2024",
        timeout=30,
    )
    response.raise_for_status.assert_called_once_with()


def test_fetch_selic_data_reraises_request_errors(monkeypatch):
    error = requests.exceptions.Timeout("request timed out")
    monkeypatch.setattr(bronze.requests, "get", Mock(side_effect=error))

    with pytest.raises(requests.exceptions.Timeout):
        bronze.fetch_selic_data("https://example.test", "json", "01/01/2024", "31/01/2024")


def test_transform_selic_data_adds_year_and_ingestion_timestamp():
    result = bronze.transform_selic_data(
        [
            {"data": "02/01/2024", "valor": "11,65"},
            {"data": "15/12/2023", "valor": "11.75"},
        ]
    )

    assert result["ano"].tolist() == [2024, 2023]
    assert result["data"].tolist() == ["02/01/2024", "15/12/2023"]
    assert result["timestamp_ingestao"].notna().all()


def test_transform_selic_data_returns_empty_dataframe_for_empty_response():
    result = bronze.transform_selic_data([])

    assert result.empty


def test_save_data_by_year_writes_one_parquet_file_per_partition(tmp_path):
    df = pd.DataFrame(
        {
            "data": ["02/01/2023", "03/01/2024"],
            "valor": [11.65, 11.75],
            "ano": [2023, 2024],
            "timestamp_ingestao": ["now", "now"],
        }
    )

    bronze.save_data_by_year(df, str(tmp_path))

    for year, expected_value in [(2023, 11.65), (2024, 11.75)]:
        output = pd.read_parquet(tmp_path / f"ano={year}" / "selic.parquet")
        assert output["valor"].tolist() == [expected_value]
        assert "ano" not in output.columns


def test_save_data_by_year_does_not_create_output_for_empty_dataframe(tmp_path):
    bronze.save_data_by_year(pd.DataFrame(), str(tmp_path))

    assert list(tmp_path.iterdir()) == []
