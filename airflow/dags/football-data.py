from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook as Hook
from airflow.models import Variable

from sqlalchemy import MetaData, Table
from pendulum import datetime

from project_hanoi.football_data.load import loader
from project_hanoi.football_data.dbt.dag_parser import DbtDagParser


with DAG(
    dag_id='football-data.co.uk',
    description='Download latest data from Joseph Buchdal''s football-data.co.uk website and run models',
    schedule_interval='@daily',
    start_date=datetime(2022, 6, 1, tz='Europe/London'),
    catchup=False,
    concurrency=2,
    default_args={
        'retries': 3,
        'retry_exponential_backoff': True,
    },
    tags=['football','football-data.co.uk'],
) as dag:

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='football-data', bind=engine)

    load_countries = PythonOperator(
        task_id='load_countries',
        python_callable=loader.load_countries,
        op_kwargs={
            'engine': engine,
        },
    )

    with TaskGroup(group_id='load_by_country') as load_by_country:
        country_table = Table('country', metadata, autoload_with=engine)
        countries = engine.execute(
            country_table.select()
        )
        for country in countries:
            with TaskGroup(group_id=country['country_name']) as by_country:
                load_leagues = PythonOperator(
                    task_id='load_leagues',
                    python_callable=loader.load_leagues_for_country,
                    op_kwargs={
                        'engine': engine,
                        'country': country,
                    },
                )

                load_seasons = PythonOperator(
                    task_id='load_seasons',
                    python_callable=loader.load_seasons_for_country,
                    op_kwargs={
                        'engine': engine,
                        'country': country,
                    },
                )

                load_leagues >> load_seasons

                league_table = Table('league', metadata, autoload_with=engine)
                leagues = engine.execute(
                    league_table.select().where(league_table.c.country_code == country['country_code'])
                )
                for league in leagues:
                    with TaskGroup(group_id=league['league_name'].replace(' ', '_')+'-'+league['country_code']) as load_by_league:
                        update_seasons = PythonOperator(
                            task_id='update_seasons',
                            python_callable=loader.update_seasons_for_league,
                            op_kwargs={
                                'engine': engine,
                                'league': league,
                            },
                        )

                        load_updated_files = PythonOperator(
                            task_id='load_updated_files',
                            python_callable=loader.load_updated_files_for_league,
                            op_kwargs={
                                'engine': engine,
                                'league': league,
                            },
                        )
                        update_seasons >> load_updated_files

                        load_seasons >> load_by_league

    load_countries >> load_by_country

    dag_parser = DbtDagParser(
        dbt_project_dir=Variable.get("DBT_FOOTBALL_DATA_PROJECT_DIR"),
        dbt_profiles_dir=Variable.get("DBT_PROFILES_DIR"),
        dbt_target='dev',
        dag=dag,
    )

    dbt_compile_task = dag_parser.get_dbt_compile_task()
    dbt_run_group = dag_parser.get_dbt_run_group()
    dbt_test_group = dag_parser.get_dbt_test_group()

    [dbt_compile_task, load_by_country] >> dbt_run_group >> dbt_test_group
