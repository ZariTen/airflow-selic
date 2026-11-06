"""Período de carga SELIC, resolvido na execução no fuso de São Paulo."""
import os
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

FUSO = ZoneInfo("America/Sao_Paulo")


def obter_periodo(agora: datetime | None = None) -> tuple[date, date]:
    """Lê SELIC_DATA_INICIAL/FINAL (DD/MM/AAAA); padrão: 2020 até hoje."""
    agora = agora or datetime.now(FUSO)
    hoje = agora.astimezone(FUSO).date()
    datas = []
    for nome, padrao in [("SELIC_DATA_INICIAL", "01/01/2020"),
                         ("SELIC_DATA_FINAL", hoje.strftime("%d/%m/%Y"))]:
        valor = os.environ.get(nome, padrao)
        try:
            if not re.fullmatch(r"\d{2}/\d{2}/\d{4}", valor):
                raise ValueError("Formato inválido")
            datas.append(datetime.strptime(valor, "%d/%m/%Y").date())
        except ValueError as exc:
            raise ValueError(f"{nome} deve ser uma data válida em DD/MM/AAAA") from exc
    inicio, fim = datas
    if inicio > fim or fim > hoje:
        raise ValueError("SELIC_DATA_INICIAL <= SELIC_DATA_FINAL <= hoje é obrigatório")
    return inicio, fim
