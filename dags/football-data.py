from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook as Hook
from sqlalchemy import MetaData, Table
from pendulum import yesterday

from ProjectHanoi.FootballData.loader import main


with DAG(
    dag_id='Football-Data.co.uk',
    description='Download latest data from Joseph Buchdal''s football-data.co.uk website',
    schedule_interval='@daily',
    start_date=yesterday('Europe/London'),
    catchup=False,
    concurrency=2,
    tags=['football','football-data.co.uk'],
) as dag:

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='football-data', bind=engine)

    load_countries = PythonOperator(
        task_id='load_countries',
        python_callable=main.load_countries,
        op_kwargs={
            'engine': engine,
        },
    )

    country_table = Table('country', metadata, autoload_with=engine)
    countries = engine.execute(
        country_table.select()
    )
    for country in countries:
        with TaskGroup(group_id=country['country_name']) as load_by_country:
            load_leagues = PythonOperator(
                task_id='load_leagues',
                python_callable=main.load_leagues_for_country,
                op_kwargs={
                    'engine': engine,
                    'country': country,
                },
            )

            load_seasons = PythonOperator(
                task_id='load_seasons',
                python_callable=main.load_seasons_for_country,
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
                        python_callable=main.update_seasons_for_league,
                        op_kwargs={
                            'engine': engine,
                            'league': league,
                        },
                    )

                    load_updated_files = PythonOperator(
                        task_id='load_updated_files',
                        python_callable=main.load_updated_files_for_league,
                        op_kwargs={
                            'engine': engine,
                            'league': league,
                        },
                    )
                    update_seasons >> load_updated_files

                    load_seasons >> load_by_league

            load_countries >> load_by_country
