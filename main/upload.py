from main.database import engine
from main.schema import matches, seasons

import pandas as pd
import numpy as np

match_headers = {
    'Date': 'date',
    'Time': 'time',
    'Div': 'division',
    'Country': 'country',
    'League': 'league',
    'Season': 'season',
    'HT': 'home_team',
    'Home': 'home_team',
    'HomeTeam': 'home_team',
    'AT': 'away_team',
    'Away': 'away_team',
    'AwayTeam': 'away_team',
    'HG': 'home_goals',
    'FTHG': 'home_goals',
    'AG': 'away_goals',
    'FTAG': 'away_goals',
    'Res': 'result',
    'FTR': 'result',
    'HTHG': 'ht_home_goals',
    'HTR': 'ht_result',
    'HTAG': 'ht_away_goals',
    'Referee': 'referee',
    'Attendance': 'attendance',
}


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

    subset = [column for column in df.columns if column in match_headers]
    match_data = df[subset].rename(columns=match_headers)
    match_data = match_data.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    match_data['url'] = url

    engine.execute(
        matches.delete().where(matches.c.url == url)
    )
    engine.execute(
        matches.insert().values(match_data.replace({np.nan: None}).to_dict('records'))
    )
    engine.execute(
        seasons.update().where(
            seasons.c.url == url
        ).values(
            last_loaded=pd.Timestamp.now()
        )
    )


def upload_recent_files(force_reload=False):
    if force_reload:
        results = engine.execute(
            seasons.select().order_by(seasons.c.last_modified.desc())
        )

    else:
        results = engine.execute(
            seasons.select().where(
                (seasons.c.last_modified > seasons.c.last_loaded) | (seasons.c.last_loaded == None)
            ).order_by(seasons.c.last_modified.desc())
        )

    for season in results.fetchall():
        upload_file(season['url'])
