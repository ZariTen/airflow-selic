from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="master_orquestrador_bcb",
    description="DAG Orquestradora para validação e ingestão da SELIC",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["master", "orquestrador", "bcb"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    trigger_validacao = TriggerDagRunOperator(
        task_id="trigger_validacao_api",
        trigger_dag_id="validacao_api_bcb",
        wait_for_completion=True,
        poke_interval=15,
    )

    trigger_ingestao = TriggerDagRunOperator(
        task_id="trigger_ingestao_selic",
        trigger_dag_id="rodar_ingestao_selic",
        wait_for_completion=True,
    )

    trigger_validacao_gold = TriggerDagRunOperator(
        task_id="trigger_validacao_gold",
        trigger_dag_id="sub_validacao_gold",
        wait_for_completion=True,
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> trigger_validacao >> trigger_ingestao >> trigger_validacao_gold >> fim
