import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger("airflow.task")


def _on_failure_callback(context: dict) -> None:
    """Loga informações estruturadas quando uma task da DAG de ingestão falha."""
    logger.error(
        "Falha na DAG de ingestão | dag_id=%s | task_id=%s | execution_date=%s | "
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
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="rodar_ingestao_selic",
    description="Pipeline medalhão para processar dados da SELIC (Bronze → Silver → Gold)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bcb", "medallion", "etl"],
    default_args=DEFAULT_ARGS,
) as dag:
    ingerir_selic_bronze = BashOperator(
        task_id="ingerir_selic_bronze",
        bash_command="python /opt/airflow/src/bronze/ingerir_selic.py",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    transform_selic_silver = BashOperator(
        task_id="transform_selic_silver",
        bash_command="python /opt/airflow/src/silver/transform_selic.py",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    agregar_selic_gold = BashOperator(
        task_id="agregar_selic_gold",
        bash_command="python /opt/airflow/src/gold/agregar_selic.py",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    ingerir_selic_bronze >> transform_selic_silver >> agregar_selic_gold
