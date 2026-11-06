"""Integração opcional com a API real: SELIC_TESTE_API_REAL=1 pytest."""
import os

import pandas as pd
import pytest

from src.bronze import ingerir_selic as bronze
from src.silver import transform_selic as silver
from src.gold import agregar_selic as gold
from test_validacao_gold import carregar_validacao


@pytest.mark.skipif(os.environ.get("SELIC_TESTE_API_REAL") != "1", reason="Consulta real habilitada explicitamente")
@pytest.mark.parametrize("fim", ["31/12/2024", None], ids=["historico", "atualizacao"])
def test_pipeline_real_bcb_grava_e_valida_todas_camadas(tmp_path, monkeypatch, fim):
    monkeypatch.setenv("SELIC_DATA_INICIAL", "01/01/2020")
    if fim:
        monkeypatch.setenv("SELIC_DATA_FINAL", fim)
    else:
        monkeypatch.delenv("SELIC_DATA_FINAL", raising=False)
    for modulo, atributo, pasta in [
        (bronze, "BRONZE_PATH", "bronze"),
        (silver, "BRONZE_PATH", "bronze"),
        (silver, "SILVER_PATH", "silver"),
        (silver, "QUARENTENA_PATH", "quarentena"),
        (gold, "SILVER_PATH", "silver"),
        (gold, "GOLD_PATH", "gold"),
    ]:
        monkeypatch.setattr(modulo, atributo, str(tmp_path / pasta))
    bronze.pipeline_ingest_selic()
    silver.process_silver_selic()
    gold.process_gold_selic()
    contexto = carregar_validacao()
    contexto["GOLD_PATH"] = str(tmp_path / "gold")
    contexto["validar_camada_gold"]()
    dados = gold.ler_silver()
    mensal = pd.read_parquet(tmp_path / "gold/acumulado_mensal/acumulado_mensal.parquet")
    anual = pd.read_parquet(tmp_path / "gold/acumulado_anual/acumulado_anual.parquet")
    assert not (tmp_path / "quarentena").exists()
    assert dados["data"].max().date() <= bronze.obter_periodo()[1]
    if fim:
        assert len(dados) == 1255
        assert len(mensal) == 60
        assert len(anual) == 5
    else:
        assert dados["data"].max().year == bronze.obter_periodo()[1].year
    print(f"BCB real: {len(dados)} registros, {len(mensal)} meses, {len(anual)} anos; última data: {dados['data'].max()}")
