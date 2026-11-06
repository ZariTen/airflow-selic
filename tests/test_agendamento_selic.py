"""Contrato das DAGs por AST, sem substituir módulos do Airflow."""
import ast
from pathlib import Path

PASTA = Path(__file__).resolve().parents[1] / "dags"


def argumentos(nome, construtor):
    arvore = ast.parse((PASTA / nome).read_text())
    chamada = next(no for no in ast.walk(arvore) if isinstance(no, ast.Call)
                   and isinstance(no.func, ast.Name) and no.func.id == construtor)
    return {item.arg: item.value for item in chamada.keywords}


def test_master_diaria_serializada_e_com_fuso():
    args = argumentos("master_dag_selic.py", "DAG")
    assert ast.literal_eval(args["schedule"]) == "0 10 * * *"
    assert ast.literal_eval(args["max_active_runs"]) == 1
    assert "America/Sao_Paulo" in ast.unparse(args["start_date"])


def test_subdags_manuais_e_serializadas():
    for nome in ["sub_ingestao_selic.py", "sub_validacao_api.py", "sub_validacao_gold.py"]:
        args = argumentos(nome, "DAG")
        assert ast.literal_eval(args["schedule"]) is None
        assert ast.literal_eval(args["max_active_runs"]) == 1


def test_sensor_consulta_apenas_janela_recente_renderizada():
    args = argumentos("sub_validacao_api.py", "HttpSensor")
    parametros = ast.literal_eval(args["request_params"])
    assert parametros["dataInicial"] == "{{ data_sonda(-14) }}"
    assert parametros["dataFinal"] == "{{ data_sonda() }}"
    args_dag = argumentos("sub_validacao_api.py", "DAG")
    assert "data_sonda" in ast.unparse(args_dag["user_defined_macros"])


def test_datas_sonda_respeitam_virada_no_fuso():
    from datetime import datetime, timezone
    caminho = PASTA / "sub_validacao_api.py"
    arvore = ast.parse(caminho.read_text())
    arvore.body = [no for no in arvore.body if isinstance(no, (ast.Import, ast.ImportFrom, ast.FunctionDef))
                   and not (isinstance(no, ast.ImportFrom) and (no.module or "").startswith("airflow"))]
    contexto = {}
    exec(compile(arvore, str(caminho), "exec"), contexto)
    agora = datetime(2027, 1, 1, 1, tzinfo=timezone.utc)
    assert contexto["data_sonda"](agora=agora) == "31/12/2026"
    assert contexto["data_sonda"](-14, agora=agora) == "17/12/2026"
