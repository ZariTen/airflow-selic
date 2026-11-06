"""Validação opcional das DAGs no Airflow instalado, sem mocks."""
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.dag_processing.dagbag import DagBag


@pytest.fixture(scope="module")
def dags_selic():
    pasta = Path(__file__).resolve().parents[1] / "dags"
    conjunto = DagBag(dag_folder=str(pasta), include_examples=False, safe_mode=False)
    assert conjunto.import_errors == {}
    return conjunto.dags


def test_airflow_carrega_master_com_fuso_e_subdags_manuais(dags_selic):
    assert set(dags_selic) == {"master_orquestrador_bcb", "validacao_api_bcb", "rodar_ingestao_selic", "sub_validacao_gold"}
    master = dags_selic["master_orquestrador_bcb"]
    assert master.timezone.name == "America/Sao_Paulo"
    assert master.max_active_runs == 1
    for nome, dag in dags_selic.items():
        assert dag.max_active_runs == 1
        if nome != "master_orquestrador_bcb":
            assert dag.schedule is None


def test_airflow_renderiza_sensor_com_janela_curta(dags_selic):
    sensor = dags_selic["validacao_api_bcb"].get_task("check_api_availability")
    sensor.render_template_fields({})
    inicio = datetime.strptime(sensor.request_params["dataInicial"], "%d/%m/%Y").date()
    fim = datetime.strptime(sensor.request_params["dataFinal"], "%d/%m/%Y").date()
    assert (fim - inicio).days == 14


def test_airflow_executa_validacao_gold_historica(dags_selic, tmp_path, monkeypatch):
    from test_validacao_gold import gravar_tabelas
    monkeypatch.setenv("SELIC_DATA_INICIAL", "01/01/2020")
    monkeypatch.setenv("SELIC_DATA_FINAL", "31/12/2024")
    gravar_tabelas(tmp_path, "2024-12")
    tarefa = dags_selic["sub_validacao_gold"].get_task("validar_dados_gold")
    monkeypatch.setitem(tarefa.python_callable.__globals__, "GOLD_PATH", str(tmp_path))
    tarefa.python_callable()
