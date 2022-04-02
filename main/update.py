from main.database import engine
from main.schema import countries, leagues, seasons

from main import scrape
from dateutil.parser import parse


def load_countries():
    print('Loading countries...')
    for country in scrape.available_countries():
        print(country['name'])
        country['code'] = country['name'][:3].upper()

        existing = engine.execute(
            countries.select().where(countries.c.url == country['url'])
        )

        if existing.fetchone():
            engine.execute(
                countries.update().where(countries.c.url == country['url']).values(country=country['name'])
            )
        else:
            engine.execute(
                countries.insert().values(
                    country_code=country['code'],
                    country=country['name'],
                    url=country['url'],
                )
            )


def load_leagues():
    print('Loading leagues...')
    for country in scrape.available_countries():
        print(country['name'])

        for league in scrape.available_leagues(country['url']):
            if not league['name']:
                league['name'] = country['name']
            print('-', league['name'])

            result = engine.execute(
                countries.select().where(countries.c.country == country['name'])
            ).fetchone()
            league['country'] = result['country_code']

            existing = engine.execute(
                leagues.select().where(leagues.c.division == league['code'])
            )

            if existing.fetchone():
                engine.execute(
                    leagues.update().where(
                        leagues.c.division == league['code']
                    ).values(
                        league=league['name'],
                        country_code=league['country'],
                    )
                )
            else:
                engine.execute(
                    leagues.insert().values(
                        division=league['code'],
                        league=league['name'],
                        country_code=league['country'],
                    )
                )


def load_seasons():
    print('Loading seasons...')
    for country in scrape.available_countries():
        print(country['name'])

        for season in scrape.available_files(country['url']):
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
                        season=season['season'],
                    )
                )
            else:
                engine.execute(
                    seasons.insert().values(
                        url=season['url'],
                        division=season['code'],
                        season=season['season'],
                    )
                )


def update_seasons():
    print('Updating last modified dates...')
    results = engine.execute(seasons.select().order_by(seasons.c.season.desc(), seasons.c.division))

    for season in results.fetchall():
        last_modified = scrape.fetch_last_modified(season['url'])
        print(season['url'], last_modified)

        engine.execute(
            seasons.update().where(
                seasons.c.url == season['url']
            ).values(
                last_modified=parse(last_modified)
            )
        )