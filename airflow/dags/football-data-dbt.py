from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from pendulum import datetime

from project_hanoi.football_data.dbt.dag_parser import DbtDagParser


with DAG(
    dag_id='football-data.co.uk-dbt',
    description='Run models for data from Joseph Buchdal''s football-data.co.uk website',
    schedule_interval='@daily',
    start_date=datetime(2022, 6, 1, tz='Europe/London'),
    catchup=False,
    concurrency=1,
    default_args={
        'retries': 2,
        'retry_exponential_backoff': True,
    },
    tags=['football','football-data.co.uk','dbt'],
) as dag:

    dag_parser = DbtDagParser(
        dbt_project_dir=Variable.get("DBT_FOOTBALL_DATA_PROJECT_DIR"),
        dbt_profiles_dir=Variable.get("DBT_PROFILES_DIR"),
        dbt_target='dev',
        dag=dag,
    )

    trigger = ExternalTaskSensor(task_id='trigger', external_dag_id='football-data.co.uk')

    dbt_compile = dag_parser.get_dbt_compile()
    dbt_run = dag_parser.get_dbt_run(expanded=True)
    dbt_test = dag_parser.get_dbt_test()

    trigger >> dbt_compile >> dbt_run >> dbt_test
