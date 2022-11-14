from airflow.decorators import dag
from pendulum import datetime

from dbt_operator import DbtBuildOperator
from football_data.scraper import load_countries, load_leagues_and_seasons, check_updated_files, load_updated_files


@dag(
    dag_id='football-data.co.uk',
    description='Download latest data from Joseph Buchdal''s football-data.co.uk website',
    schedule='@daily',
    start_date=datetime(2022, 6, 1, tz='Europe/London'),
    catchup=False,
    tags=['football', 'football-data.co.uk'],
)
def football_data():
    project_dir = 'DBT_FOOTBALL_DATA_PROJECT_DIR'

    countries = load_countries()
    divisions = load_leagues_and_seasons.partial().expand(country=countries)
    updated_files = check_updated_files.partial().expand(divisions=divisions)
    loaded_files = load_updated_files.partial().expand(files=updated_files)

    loaded_files >> DbtBuildOperator(task_id='dbt_build', project_dir=project_dir, trigger_rule='none_failed')


dag = football_data()
