import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger("airflow.task")


def _on_failure_callback(context: dict) -> None:
    """Loga informações estruturadas quando a validação da API falha."""
    logger.error(
        "Falha na validação da API BCB | dag_id=%s | task_id=%s | execution_date=%s | "
        "tentativa=%s | log_url=%s",
        context.get("dag").dag_id,
        context.get("task_instance").task_id,
        context.get("execution_date"),
        context.get("task_instance").try_number,
        context.get("task_instance").log_url,
    )


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="validacao_api_bcb",
    description="DAG para validar a disponibilidade da API do Banco Central",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bcb", "sensor", "infra"],
    default_args=DEFAULT_ARGS,
) as dag:
    check_api = HttpSensor(
        task_id="check_api_availability",
        http_conn_id="bcb_api",
        endpoint="dados/serie/bcdata.sgs.11/dados",
        request_params={
            "formato": "json",
            "dataInicial": "01/01/2020",
            "dataFinal": "31/12/2024",
        },
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=120,
        mode="reschedule",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    fim_validacao = EmptyOperator(
        task_id="fim_validacao",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    check_api >> fim_validacao
