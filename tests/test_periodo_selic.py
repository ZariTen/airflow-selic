"""Regressões do período dinâmico e da atualização diária da SELIC."""
from datetime import date, datetime, timezone

import pytest

from src.bronze import ingerir_selic as bronze
from src.silver import transform_selic as silver
import pandas as pd


def test_ingestao_resolve_periodo_na_execucao(monkeypatch):
    monkeypatch.setenv("SELIC_DATA_INICIAL", "01/01/2025")
    monkeypatch.setenv("SELIC_DATA_FINAL", "31/12/2025")
    consultas = []
    monkeypatch.setattr(bronze, "fetch_selic_data", lambda *args: consultas.append(args) or [])
    bronze.pipeline_ingest_selic()
    assert consultas[0][-2:] == ("01/01/2025", "31/12/2025")


def test_consulta_longa_divide_janelas_sem_lacunas(monkeypatch):
    urls = []
    class Resposta:
        def raise_for_status(self):
            pass
        def json(self):
            return [{"data": "02/01/2000", "valor": "0.01"}]
    def consultar(url, timeout):
        urls.append(url)
        return Resposta()
    monkeypatch.setattr(bronze.requests, "get", consultar)
    resultado = bronze.fetch_selic_data("https://example.test", "json", "01/01/2000", "01/01/2020")
    assert len(urls) == 3
    assert len(resultado) == 3
    assert "dataFinal=31/12/2009" in urls[0]
    assert "dataInicial=01/01/2010&dataFinal=31/12/2019" in urls[1]
    assert "dataInicial=01/01/2020&dataFinal=01/01/2020" in urls[2]


def test_silver_aceita_atual_e_preserva_historico(monkeypatch):
    monkeypatch.setenv("SELIC_DATA_FINAL", "06/11/2026")
    df = silver.transformar(pd.DataFrame({"data": ["02/01/2020", "05/11/2026", "01/01/2027"], "valor": [0.01]*3}))
    validos, invalidos = silver.validar(df)
    assert validos["data"].dt.year.tolist() == [2020, 2026]
    assert invalidos["data"].dt.year.tolist() == [2027]


def test_periodo_padrao_usa_data_de_sao_paulo(monkeypatch):
    monkeypatch.delenv("SELIC_DATA_INICIAL", raising=False)
    monkeypatch.delenv("SELIC_DATA_FINAL", raising=False)
    inicio, fim = bronze.obter_periodo(datetime(2027, 1, 1, 1, tzinfo=timezone.utc))
    assert inicio == date(2020, 1, 1)
    assert fim == date(2026, 12, 31)


def test_periodo_configurado_permite_historico(monkeypatch):
    monkeypatch.setenv("SELIC_DATA_INICIAL", "01/01/2020")
    monkeypatch.setenv("SELIC_DATA_FINAL", "31/12/2024")
    assert bronze.obter_periodo() == (date(2020, 1, 1), date(2024, 12, 31))


@pytest.mark.parametrize("inicio,fim", [
    ("2020-01-01", "31/12/2024"),
    ("31/02/2020", "31/12/2024"),
    ("01/01/2025", "31/12/2024"),
    ("01/01/2020", "01/01/2099"),
    ("", "31/12/2024"),
])
def test_periodo_rejeita_configuracao_invalida(monkeypatch, inicio, fim):
    monkeypatch.setenv("SELIC_DATA_INICIAL", inicio)
    monkeypatch.setenv("SELIC_DATA_FINAL", fim)
    with pytest.raises(ValueError, match="SELIC_DATA"):
        bronze.obter_periodo(datetime(2026, 11, 6, tzinfo=timezone.utc))
