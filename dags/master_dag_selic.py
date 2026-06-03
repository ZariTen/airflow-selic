import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger("airflow.task")


def _on_failure_callback(context: dict) -> None:
    """Loga informações estruturadas quando qualquer task da DAG falha."""
    logger.error(
        "Falha na DAG master | dag_id=%s | task_id=%s | execution_date=%s | "
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
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="master_orquestrador_bcb",
    description="DAG orquestradora para validação e ingestão da SELIC",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["master", "orquestrador", "bcb"],
    default_args=DEFAULT_ARGS,
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    trigger_validacao_api = TriggerDagRunOperator(
        task_id="trigger_validacao_api",
        trigger_dag_id="validacao_api_bcb",
        wait_for_completion=True,
        poke_interval=15,
        reset_dag_run=True,
    )

    trigger_ingestao_selic = TriggerDagRunOperator(
        task_id="trigger_ingestao_selic",
        trigger_dag_id="rodar_ingestao_selic",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_validacao_gold = TriggerDagRunOperator(
        task_id="trigger_validacao_gold",
        trigger_dag_id="sub_validacao_gold",
        wait_for_completion=True,
        poke_interval=15,
        reset_dag_run=True,
    )

    fim = EmptyOperator(task_id="fim")

    (
        inicio
        >> trigger_validacao_api
        >> trigger_ingestao_selic
        >> trigger_validacao_gold
        >> fim
    )
