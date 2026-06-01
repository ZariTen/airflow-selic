from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='validacao_api_bcb',
    description='DAG para validar a disponibilidade da API do Banco Central',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bcb', 'sensor', 'infra'],
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:

    check_api_availability = HttpSensor(
        task_id='check_api_availability',
        http_conn_id='bcb_api',
        endpoint='dados/serie/bcdata.sgs.11/dados',
        request_params={
            'formato': 'json',
            'dataInicial': '01/01/2020',
            'dataFinal': '31/12/2024'
        },
        method='GET',
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=300,
        mode='poke',
    )

    trigger_ingestion_pipeline = TriggerDagRunOperator(
        task_id='trigger_ingestion_pipeline',
        trigger_dag_id='rodar_ingestao_selic',
        wait_for_completion=False,
    )

    check_api_availability >> trigger_ingestion_pipeline
