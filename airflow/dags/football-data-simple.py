from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook as Hook
from pendulum import datetime

from project_hanoi.football_data.load import loader


with DAG(
    dag_id='Football-Data.co.uk-simple',
    description='Download latest data from Joseph Buchdal''s football-data.co.uk website',
    schedule_interval='@monthly',
    start_date=datetime(2022, 5, 1, tz='Europe/London'),
    catchup=False,
    tags=['football','football-data.co.uk'],
) as dag:

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()

    load_countries = PythonOperator(
        task_id='load_countries',
        python_callable=loader.load_countries,
        op_kwargs={
            'engine': engine,
        },
    )

    load_leagues = PythonOperator(
        task_id='load_leagues',
        python_callable=loader.load_leagues,
        op_kwargs={
            'engine': engine,
        },
    )

    load_seasons = PythonOperator(
        task_id='load_seasons',
        python_callable=loader.load_seasons,
        op_kwargs={
            'engine': engine,
        },
    )

    update_seasons = PythonOperator(
        task_id='update_seasons',
        python_callable=loader.update_seasons,
        op_kwargs={
            'engine': engine,
        },
    )

    load_files = PythonOperator(
        task_id='load_files',
        python_callable=loader.load_modified_files,
        op_kwargs={
            'engine': engine,
        },
    )

    dbt_profiles_dir = Variable.get("DBT_PROFILES_DIR")
    dbt_project_dir = Variable.get("DBT_FOOTBALL_DATA_PROJECT_DIR")

    run_models = BashOperator(
        task_id='dbt_run',
        bash_command=(
            f'dbt run --target dev '
            f'--profiles-dir {dbt_profiles_dir} '
            f'--project-dir {dbt_project_dir}'
        ),
        env={
            'DBT_USER': '{{ conn.football_db.login }}',
            'DBT_ENV_SECRET_PASSWORD': '{{ conn.football_db.password }}',
            'DBT_HOST': '{{ conn.football_db.host }}',
            'DBT_SCHEMA': '{{ conn.football_db.schema }}',
            'DBT_PORT': '{{ conn.football_db.port }}',
        },
    )

    load_countries >> load_leagues >> load_seasons >> update_seasons >> load_files >> run_models
