from airflow import DAG
from airflow.models import Variable
from pendulum import yesterday

from project_hanoi.football_data.dbt.dag_parser import DbtDagParser


with DAG(
    dag_id='football-data.co.uk-models_only',
    description='Run dbt models using data from Joseph Buchdal''s football-data.co.uk website',
    schedule_interval=None,
    start_date=yesterday('Europe/London'),
    catchup=False,
    concurrency=2,
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

    dbt_compile = dag_parser.get_dbt_compile_task()
    dbt_run = dag_parser.get_dbt_run_group()
    dbt_test = dag_parser.get_dbt_test_group()

    dbt_compile >> dbt_run >> dbt_test
