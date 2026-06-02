from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="validacao_api_bcb",
    description="DAG para validar a disponibilidade da API do Banco Central",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bcb", "sensor", "infra"],
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
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
        timeout=60,
        mode="reschedule",
    )

    fim_validacao = EmptyOperator(task_id="fim_validacao")

    check_api >> fim_validacao
