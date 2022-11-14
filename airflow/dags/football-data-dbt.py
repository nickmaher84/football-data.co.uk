from airflow import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from pendulum import datetime

from football_data.dag_parser import DbtDagParser

dataset = Dataset('//football/football-data')


@dag(
    dag_id='football-data.co.uk-dbt',
    description='Run models for data from Joseph Buchdal''s football-data.co.uk website',
    schedule=[dataset],
    start_date=datetime(2022, 6, 1, tz='Europe/London'),
    concurrency=1,
    default_args={
        'retries': 2,
        'retry_exponential_backoff': True,
    },
    tags=['football','football-data.co.uk','dbt'],
)
def football_data_dbt():

    dag = DbtDagParser(
        dbt_project_dir=Variable.get("DBT_FOOTBALL_DATA_PROJECT_DIR"),
        dbt_profiles_dir=Variable.get("DBT_PROFILES_DIR"),
        dbt_target='dev',
    )

    dbt_compile = dag.get_dbt_compile()
    dbt_run = dag.get_dbt_run(expanded=True)
    dbt_test = dag.get_dbt_test()

    dbt_compile >> dbt_run >> dbt_test


d = football_data_dbt()
