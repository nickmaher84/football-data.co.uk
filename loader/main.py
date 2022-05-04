from database import engine
from schema import countries, leagues, seasons, raw_data

import scraper
from dateutil.parser import parse


def load_countries():
    print('Loading countries...')
    for country in scraper.countries():
        print(country['name'])

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
                    country_code=scraper.country_code(country['name']),
                    country=country['name'],
                    url=country['url'],
                )
            )


def load_leagues():
    print('Loading leagues...')
    for country in scraper.countries():
        print(country['name'])

        for league in scraper.leagues(country['url']):
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
    for country in scraper.countries():
        print(country['name'])

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
        last_modified = scraper.fetch_last_modified(season['url'])
        print(season['url'], last_modified)

        engine.execute(
            seasons.update().where(
                seasons.c.url == season['url']
            ).values(
                last_modified=parse(last_modified)
            )
        )


def load_raw_file(url):
    engine.execute(
        raw_data.delete().where(
            raw_data.c.url == url
        )
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


def load_modified_files(force_reload=False):
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
        load_raw_file(season['url'])


if __name__ == '__main__':
    # load_countries(); print()
    # load_leagues(); print()
    # load_seasons()
    # update_seasons()
    load_modified_files()
