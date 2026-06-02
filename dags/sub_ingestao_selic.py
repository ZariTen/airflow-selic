from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='rodar_ingestao_selic',
    description='Pipeline Medalhão para processar dados da SELIC',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bcb', 'medallion', 'etl'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
) as dag:

    ingerir_selic_bronze = BashOperator(
        task_id='ingerir_selic_bronze',
        bash_command='python /opt/airflow/src/bronze/ingerir_selic.py',
    )

    transform_selic_silver = BashOperator(
        task_id='transform_selic_silver',
        bash_command='python /opt/airflow/src/silver/transform_selic.py',
    )

    agregar_selic_gold = BashOperator(
        task_id='agregar_selic_gold',
        bash_command='python /opt/airflow/src/gold/agregar_selic.py',
    )

    ingerir_selic_bronze >> transform_selic_silver >> agregar_selic_gold
