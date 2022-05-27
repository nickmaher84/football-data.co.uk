from dateutil.parser import parse
from sqlalchemy import MetaData, Table
from project_hanoi.football_data.load import scraper


def load_countries(engine):
    metadata = MetaData(schema='football-data', bind=engine)
    countries = Table('country', metadata, autoload_with=engine)

    print('Loading countries...')
    for country in scraper.countries():
        print(country['name'])

        existing = engine.execute(
            countries.select().where(countries.c.url == country['url'])
        )

        if existing.fetchone():
            engine.execute(
                countries.update().where(countries.c.url == country['url']).values(
                    country_name=country['name'],
                )
            )
        else:
            engine.execute(
                countries.insert().values(
                    country_code=scraper.country_code(country['name']),
                    country_name=country['name'],
                    url=country['url'],
                )
            )


def load_leagues_for_country(engine, country):
    metadata = MetaData(schema='football-data', bind=engine)
    leagues = Table('league', metadata, autoload_with=engine)

    print('Loading leagues in {name}...'.format(name=country['country_name']))

    for league in scraper.leagues(country['url']):
        if not league['name']:
            league['name'] = country['country_name']
        print('-', league['name'])

        league['country'] = country['country_code']

        existing = engine.execute(
            leagues.select().where(leagues.c.division == league['code'])
        )

        if existing.fetchone():
            engine.execute(
                leagues.update().where(
                    leagues.c.division == league['code']
                ).values(
                    league_name=league['name'],
                    country_code=league['country'],
                )
            )
        else:
            engine.execute(
                leagues.insert().values(
                    division=league['code'],
                    league_name=league['name'],
                    country_code=league['country'],
                )
            )


def load_leagues(engine):
    metadata = MetaData(schema='football-data', bind=engine)
    countries = Table('country', metadata, autoload_with=engine)

    print('Loading leagues...')

    select = countries.select()
    for country in engine.execute(select):
        load_leagues_for_country(engine, country)


def load_seasons_for_country(engine, country):
    metadata = MetaData(schema='football-data', bind=engine)
    seasons = Table('season', metadata, autoload_with=engine)

    print('Loading seasons in {name}...'.format(name=country['country_name']))

    for season in scraper.seasons(country['url']):
        filename = season['url'].split('/').pop()
        season['code'] = filename.replace('.csv', '')

        print('-', season['season'], season['league'])

        existing = engine.execute(
            seasons.select().where(seasons.c.url == season['url'])
        )

        if existing.fetchone():
            engine.execute(
                seasons.update().where(
                    seasons.c.url == season['url']
                ).values(
                    division=season['code'],
                    season_name=season['season'],
                )
            )
        else:
            engine.execute(
                seasons.insert().values(
                    url=season['url'],
                    division=season['code'],
                    season_name=season['season'],
                )
            )


def load_seasons(engine):
    metadata = MetaData(schema='football-data', bind=engine)
    countries = Table('country', metadata, autoload_with=engine)

    print('Loading seasons...')

    select = countries.select()
    for country in engine.execute(select):
        load_seasons_for_country(engine, country)


def update_seasons_for_league(engine, league):
    metadata = MetaData(schema='football-data', bind=engine)
    seasons = Table('season', metadata, autoload_with=engine)

    print('Updating last modified dates for {league}...'.format(league=league))
    results = engine.execute(
        seasons.select().where(
            seasons.c.division == league['division']
        ).order_by(seasons.c.season_name.desc(), seasons.c.division)
    )

    for season in results.fetchall():
        last_modified = scraper.fetch_last_modified(season['url'])
        print(season['url'], last_modified)

        engine.execute(
            seasons.update().where(
                seasons.c.url == season['url']
            ).values(
                last_modified=parse(last_modified)
            )
        )


def update_seasons(engine):
    metadata = MetaData(schema='football-data', bind=engine)
    leagues = Table('league', metadata, autoload_with=engine)

    print('Updating last modified dates...')

    select = leagues.select()
    for league in engine.execute(select):
        update_seasons_for_league(engine, league)


def load_raw_file(engine, url):
    metadata = MetaData(schema='football-data', bind=engine)
    raw_data = Table('raw', metadata, autoload_with=engine)
    seasons = Table('season', metadata, autoload_with=engine)

    engine.execute(
        raw_data.delete().where(raw_data.c.url == url)
    )

    for row, timestamp in scraper.file(url):
        engine.execute(
            raw_data.insert().values(
                url=url,
                json=row,
                loaded_at=timestamp,
            )
        )

    engine.execute(
        seasons.update().where(
            seasons.c.url == url
        ).values(
            last_loaded=timestamp
        )
    )


def load_updated_files_for_league(engine, league):
    metadata = MetaData(schema='football-data', bind=engine)
    seasons = Table('season', metadata, autoload_with=engine)

    results = engine.execute(
        seasons.select().where(
            (seasons.c.division == league['division']) &
            (seasons.c.last_modified > seasons.c.last_loaded) | (seasons.c.last_loaded == None)
        ).order_by(seasons.c.last_modified)
    )

    for season in results.fetchall():
        load_raw_file(engine, season['url'])


def load_modified_files(engine, force_reload=False):
    metadata = MetaData(schema='football-data', bind=engine)
    seasons = Table('season', metadata, autoload_with=engine)

    if force_reload:
        results = engine.execute(
            seasons.select().order_by(seasons.c.last_modified.desc())
        )

    else:
        results = engine.execute(
            seasons.select().where(
                (seasons.c.last_modified > seasons.c.last_loaded) | (seasons.c.last_loaded == None)
            ).order_by(seasons.c.last_modified)
        )

    for season in results.fetchall():
        load_raw_file(engine, season['url'])
