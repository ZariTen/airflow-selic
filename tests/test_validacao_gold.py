"""Validação real dos dados Gold sem importar o runtime Airflow."""
import ast
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest


def carregar_validacao():
    caminho = Path(__file__).resolve().parents[1] / "dags/sub_validacao_gold.py"
    arvore = ast.parse(caminho.read_text())
    arvore.body = [no for no in arvore.body if not isinstance(no, ast.With)
                   and not (isinstance(no, ast.ImportFrom) and (no.module or "").startswith("airflow"))]
    contexto: dict[str, Any] = {"__file__": str(caminho)}
    exec(compile(arvore, str(caminho), "exec"), contexto)
    return contexto


def gravar_tabelas(caminho, fim):
    meses = pd.period_range("2020-01", fim, freq="M")
    mensal = pd.DataFrame({"ano": meses.year, "mes": meses.month})
    anual = pd.DataFrame({"ano": sorted(set(meses.year))})
    for nome, df in [("acumulado_mensal", mensal), ("acumulado_anual", anual)]:
        (caminho / nome).mkdir(exist_ok=True)
        df.to_parquet(caminho / nome / f"{nome}.parquet", index=False)


def test_gold_rejeita_historico_sem_ano_atual(tmp_path, monkeypatch):
    monkeypatch.setenv("SELIC_DATA_FINAL", "06/11/2026")
    gravar_tabelas(tmp_path, "2025-12")
    contexto = carregar_validacao()
    contexto["GOLD_PATH"] = str(tmp_path)
    with pytest.raises(ValueError, match="2026"):
        contexto["validar_camada_gold"]()


@pytest.mark.parametrize("fim,ultimo_mes", [("01/01/2026", "2025-12"), ("02/01/2026", "2025-12"), ("06/11/2026", "2026-11"), ("31/12/2024", "2024-12")])
def test_gold_aceita_cobertura_sem_periodos_futuros(tmp_path, monkeypatch, fim, ultimo_mes):
    monkeypatch.setenv("SELIC_DATA_FINAL", fim)
    gravar_tabelas(tmp_path, ultimo_mes)
    contexto = carregar_validacao()
    contexto["GOLD_PATH"] = str(tmp_path)
    contexto["validar_camada_gold"]()


def test_gold_virada_de_ano_sem_dia_util(tmp_path, monkeypatch):
    gravar_tabelas(tmp_path, "2026-12")
    contexto = carregar_validacao()
    contexto["GOLD_PATH"] = str(tmp_path)
    # Em 2027, 1º de janeiro é feriado e os dias 2 e 3 são fim de semana.
    contexto["obter_periodo"] = lambda: (date(2020, 1, 1), date(2027, 1, 4))
    contexto["validar_camada_gold"]()
    contexto["obter_periodo"] = lambda: (date(2020, 1, 1), date(2027, 1, 5))
    with pytest.raises(ValueError, match="2027"):
        contexto["validar_camada_gold"]()


@pytest.mark.parametrize("ano,mes,primeiro_dia_util", [(2026, 5, 4), (2025, 3, 5)])
def test_gold_nao_exige_mes_antes_de_dia_util_bancario(tmp_path, monkeypatch, ano, mes, primeiro_dia_util):
    gravar_tabelas(tmp_path, f"{ano}-{mes - 1:02d}")
    contexto = carregar_validacao()
    contexto["GOLD_PATH"] = str(tmp_path)
    contexto["obter_periodo"] = lambda: (date(2020, 1, 1), date(ano, mes, primeiro_dia_util))
    contexto["validar_camada_gold"]()
    contexto["obter_periodo"] = lambda: (date(2020, 1, 1), date(ano, mes, primeiro_dia_util + 1))
    with pytest.raises(ValueError, match=f"{ano}.*{mes}"):
        contexto["validar_camada_gold"]()


def test_gold_rejeita_lacuna_mensal_mesmo_com_todos_anos(tmp_path, monkeypatch):
    monkeypatch.setenv("SELIC_DATA_FINAL", "06/11/2026")
    gravar_tabelas(tmp_path, "2026-11")
    arquivo = tmp_path / "acumulado_mensal/acumulado_mensal.parquet"
    df = pd.read_parquet(arquivo)
    df.iloc[1:].to_parquet(arquivo, index=False)
    contexto = carregar_validacao()
    contexto["GOLD_PATH"] = str(tmp_path)
    with pytest.raises(ValueError, match="2020.*1"):
        contexto["validar_camada_gold"]()
