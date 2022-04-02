from main.database import engine
from main.schema import matches, statistics, match_statistics, bookmakers, match_odds, match_over_unders, match_asian_handicaps, seasons

import yaml
import pandas as pd
import numpy as np


def upload_statistics():
    print('Loading data for statistics')

    with open('data/statistics.yml') as f:
        mappings = yaml.safe_load(f)

    upsert_records(statistics, mappings)


def upload_bookmakers():
    print('Loading data for bookmakers')

    with open('data/bookmakers.yml') as f:
        mappings = yaml.safe_load(f)

    upsert_records(bookmakers, mappings)


def upload_file(url):
    print('Loading data from {url}'.format(url=url))

    df = pd.read_csv(
        url,
        encoding='cp1252',
        parse_dates=['Date'],
        dayfirst=True,
        on_bad_lines=lambda x: x,
        engine='python',
    ).dropna(
        axis=1,
        how='all'
    ).dropna(
        subset=['Date'],
    )

    with open('data/column_mappings.yml') as f:
        column_mappings = yaml.safe_load(f)
        
    subset = [column for column in df.columns if column in column_mappings]
    match_data = df[subset].rename(columns=column_mappings)
    match_data = match_data.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    match_data['url'] = url

    ''' Load Match Info '''
    delete_records_for_url(matches, url)
    insert_records(matches, match_data)

    ''' Load Match Statistics '''
    delete_records_for_url(match_statistics, url)

    with open('data/statistics.yml') as f:
        mappings = yaml.safe_load(f)

    for statistic in mappings:
        if 'H' + statistic in df.columns:
            print(mappings.get(statistic))

            match_statistic_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_statistic_data['statistic'] = statistic
            match_statistic_data['home_stat'] = df['H' + statistic]
            match_statistic_data['away_stat'] = df['A' + statistic]

            insert_records(match_statistics, match_statistic_data)

    ''' Load Match Statistics '''
    delete_records_for_url(match_odds, url)
    delete_records_for_url(match_over_unders, url)
    delete_records_for_url(match_asian_handicaps, url)

    with open('data/bookmakers.yml') as f:
        mappings = yaml.safe_load(f)

    for bookmaker in mappings:
        if bookmaker + 'H' in df.columns:
            print('{bookmaker} - Match Odds'.format(bookmaker=mappings.get(bookmaker['name'])))

            match_odds_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_odds_data['bookmaker'] = bookmaker
            match_odds_data['home'] = df[bookmaker + 'H']
            match_odds_data['draw'] = df[bookmaker + 'D']
            match_odds_data['away'] = df[bookmaker + 'A']

            if 'Bb' in bookmaker:
                match_odds_data['count'] = df['Bb1X2']

            match_odds_data = match_odds_data.dropna(axis=0, subset=['home', 'draw', 'away'])
            insert_records(match_odds, match_odds_data)

        if bookmaker + '>2.5' in df.columns:
            print('{bookmaker} - Over/Under'.format(bookmaker=mappings.get(bookmaker['name'])))

            match_over_under_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_over_under_data['bookmaker'] = bookmaker
            match_over_under_data['over'] = df[bookmaker + '>2.5']
            match_over_under_data['under'] = df[bookmaker + '<2.5']

            if 'Bb' in bookmaker:
                match_over_under_data['count'] = df['BbOU']

            match_over_under_data = match_over_under_data.dropna(axis=0, subset=['over', 'under'])
            insert_records(match_over_unders, match_over_under_data)

        if bookmaker + 'AHH' in df.columns:
            print('{bookmaker} - Asian Handicap'.format(bookmaker=mappings.get(bookmaker['name'])))

            match_asian_handicap_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_asian_handicap_data['bookmaker'] = bookmaker
            match_asian_handicap_data['home'] = df[bookmaker + 'AHH']
            match_asian_handicap_data['away'] = df[bookmaker + 'AHA']

            if 'Bb' in bookmaker:
                match_asian_handicap_data['count'] = df['BbAH'].apply(lambda x: str(x).replace(u'\xa0', '').replace('nan', '0'))
                match_asian_handicap_data['handicap'] = df['BbAHh'].apply(lambda x: str(x).split(',')[0])

            elif bookmaker + 'AH' in df.columns:
                match_asian_handicap_data['handicap'] = df[bookmaker + 'AH']

            elif mappings.get(bookmaker['closing']):
                match_asian_handicap_data['handicap'] = df['AHCh']

            else:
                match_asian_handicap_data['handicap'] = df['AHh']

            match_asian_handicap_data = match_asian_handicap_data.dropna(axis=0, subset=['home', 'away'])
            insert_records(match_asian_handicaps, match_asian_handicap_data)

    ''' Update Last Loaded Date '''
    update_last_loaded(url)


def upload_recent_files(force_reload=False):
    if force_reload:
        results = engine.execute(
            seasons.select().order_by(seasons.c.last_modified.desc())
        )

    else:
        results = engine.execute(
            seasons.select().where(
                (seasons.c.last_modified > seasons.c.last_loaded) | (seasons.c.last_loaded is None)
            ).order_by(seasons.c.last_modified.desc())
        )

    for season in results.fetchall():
        upload_file(season['url'])


def delete_records_for_url(table, url):
    engine.execute(
        table.delete().where(table.c.url == url)
    )


def insert_records(table, df):
    engine.execute(
        table.insert().values(df.replace({np.nan: None}).to_dict('records'))
    )


def upsert_records(table, records):
    for code, values in records.items():
        if type(values) is str:
            values = {'name': values}

        existing = engine.execute(
            table.select().where(table.c.code == code)
        )

        if existing.fetchone():
            engine.execute(
                table.update().where(table.c.code == code).values(values)
            )
        else:
            values.update(code=code)
            engine.execute(
                table.insert().values(values)
            )


def update_last_loaded(url):
    engine.execute(
        seasons.update().where(seasons.c.url == url).values(last_loaded=pd.Timestamp.now())
    )
