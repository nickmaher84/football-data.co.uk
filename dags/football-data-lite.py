from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook as Hook
from pendulum import yesterday

from ProjectHanoi.FootballData.loader import main


with DAG(
    dag_id='Football-Data.co.uk-Lite',
    description='Download latest data from Joseph Buchdal''s football-data.co.uk website',
    schedule_interval='@daily',
    start_date=yesterday('Europe/London'),
    catchup=False,
    tags=['football','football-data.co.uk'],
) as dag:

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()

    load_countries = PythonOperator(
        task_id='load_countries',
        python_callable=main.load_countries,
        op_kwargs={
            'engine': engine,
        },
    )

    load_leagues = PythonOperator(
        task_id='load_leagues',
        python_callable=main.load_leagues,
        op_kwargs={
            'engine': engine,
        },
    )

    load_seasons = PythonOperator(
        task_id='load_seasons',
        python_callable=main.load_seasons,
        op_kwargs={
            'engine': engine,
        },
    )

    update_seasons = PythonOperator(
        task_id='update_seasons',
        python_callable=main.update_seasons,
        op_kwargs={
            'engine': engine,
        },
    )

    load_files = PythonOperator(
        task_id='load_files',
        python_callable=main.load_modified_files,
        op_kwargs={
            'engine': engine,
        },
    )

    load_countries >> load_leagues >> load_seasons >> update_seasons >> load_files
