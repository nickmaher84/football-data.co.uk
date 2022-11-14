from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook as Hook
from sqlalchemy import MetaData, Table
from pendulum import now

from fifa import lookup_country_code


@task(retries=1)
def load_countries():
    import requests
    from bs4 import BeautifulSoup
    from re import compile

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='football-data', bind=engine)

    def save_country(record):
        table = Table('country', metadata, autoload_with=engine)

        existing = engine.execute(
            table.select().where(
                table.c.url == record['url']
            )
        )

        if existing.fetchone():
            engine.execute(
                table.update().where(
                    table.c.url == record['url']
                ).values(
                    country_name=record['name'],
                )
            )

        else:
            engine.execute(
                table.insert().values(
                    country_code=record['code'],
                    country_name=record['name'],
                    url=record['url'],
                )
            )

    response = requests.get('http://football-data.co.uk/data.php')
    print(response.status_code, response.reason, response.url)

    soup = BeautifulSoup(response.text, 'html5lib')

    flags = soup.find_all(
        'img',
        src=compile(r'/flags/[a-z]+\.gif'),
        align='right',
        a=True,
    )

    countries = list()
    print('Loading countries...')

    for flag in flags:
        country = {
            'name': flag.parent.parent.text.strip(),
            'url': flag.parent['href'],
        }
        country['code'] = lookup_country_code(country['name'])

        print(country['name'])

        save_country(country)

        countries.append(country)

    return countries


@task(retries=1)
def load_leagues_and_seasons(country):
    import requests
    from bs4 import BeautifulSoup
    from re import compile
    from urllib.parse import urljoin

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='football-data', bind=engine)

    def save_league(record):
        table = Table('league', metadata, autoload_with=engine)

        existing = engine.execute(
            table.select().where(table.c.division == record['division'])
        )

        if existing.fetchone():
            engine.execute(
                table.update().where(
                    table.c.division == record['division']
                ).values(
                    league_name=record['league_name'],
                    country_code=record['country_code'],
                )
            )

        else:
            engine.execute(
                table.insert().values(
                    division=record['division'],
                    league_name=record['league_name'],
                    country_code=record['country_code'],
                )
            )

    def save_season(record):
        table = Table('season', metadata, autoload_with=engine)

        existing = engine.execute(
            table.select().where(table.c.url == record['url'])
        )

        if existing.fetchone():
            engine.execute(
                table.update().where(
                    table.c.url == record['url']
                ).values(
                    division=record['division'],
                    season_name=record['season_name'],
                )
            )

        else:
            engine.execute(
                table.insert().values(
                    url=record['url'],
                    division=record['division'],
                    season_name=record['season_name'],
                )
            )

    country_name = country['name']

    response = requests.get(country['url'])
    print(response.status_code, response.reason, response.url)

    soup = BeautifulSoup(response.text, 'html5lib')

    files = soup.find_all(
        'a',
        href=compile(r'\.csv'),
    )

    leagues = dict()
    print(f'Loading leagues and seasons for {country_name}...')

    for file in files:
        if file.text == 'CSV':
            continue

        filename = file['href'].split('/').pop()
        division = filename.replace('.csv', '')

        if file.find_previous_sibling('i'):
            leagues.setdefault(division, file.text)

            season = {
                'url': urljoin(country['url'], file['href']),
                'division': division,
                'league_name': file.text,
                'season_name': file.find_previous_sibling('i').text,
            }

        else:
            leagues.setdefault(division, country_name)

            season = {
                'url': urljoin(country['url'], file['href']),
                'division': division,
                'league_name': file.text,
                'season_name': 'All Seasons',
            }

        print('-', season['league_name'], season['season_name'])
        save_season(season)

    for division, league_name in leagues.items():
        league = {
            'division': division,
            'league_name': league_name,
            'country_code': country['code'],
        }

        save_league(league)

    return list(leagues.keys())


@task(retries=1)
def check_updated_files(divisions):
    import requests
    from dateutil.parser import parse

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='football-data', bind=engine)

    def find_files(record):
        print(record)

        table = Table('season', metadata, autoload_with=engine)
        select = table.select().where(table.c.division == record)

        return engine.execute(select)

    def update_file(record):
        table = Table('season', metadata, autoload_with=engine)

        engine.execute(
            table.update().where(
                table.c.url == record['url']
            ).values(
                last_modified=last_modified,
            )
        )

    files = list()

    for division in divisions:
        for file in find_files(division):
            response = requests.head(file['url'])
            print(response.status_code, response.reason, response.url)

            last_modified = parse(response.headers.get('last-modified'))

            if last_modified > file['last_modified']:
                print('UPDATED', file['last_modified'], '->', last_modified)

                update_file(file)

                files.append(file['url'])

            else:
                print('NO CHANGE:', file['last_modified'])

    return files


@task(retries=1)
def load_updated_files(files):
    import requests
    from csv import DictReader

    hook = Hook('football_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(schema='football-data', bind=engine)

    def delete_data(record):
        table = Table('raw', metadata, autoload_with=engine)

        engine.execute(
            table.delete().where(
                table.c.url == record
            )
        )

    def update_file(record, last_loaded):
        table = Table('season', metadata, autoload_with=engine)

        engine.execute(
            table.update().where(
                table.c.url == record
            ).values(
                last_loaded=last_loaded
            )
        )

    def write_data(url, record, loaded_at):
        table = Table('raw', metadata, autoload_with=engine)

        engine.execute(
            table.insert().values(
                url=url,
                json=record,
                loaded_at=loaded_at,
            )
        )

    if len(files) == 0:
        raise AirflowSkipException('No updated files to load. Skipping...')

    for url in files:
        response = requests.get(url)
        print(response.status_code, response.reason, response.url)

        timestamp = now()

        reader = DictReader(
            response.iter_lines(decode_unicode=True)
        )

        delete_data(url)
        for row in reader:
            write_data(url, row, timestamp)

        update_file(url, timestamp)
