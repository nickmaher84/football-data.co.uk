from main.database import engine
from main.schema import matches, statistics, match_statistics, seasons

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
statistics_dict = {
    'S': 'Shots',
    'ST': 'Shots on Target',
    'HW': 'Hit Woodwork',
    'C': 'Corners',
    'F': 'Fouls',
    'FKC': 'Free-kicks Conceded',
    'O': 'Offsides',
    'Y': 'Yellow Cards',
    'R': 'Red Cards',
    'BP': 'Booking Points',
}


def upload_statistics():
    print('Loading data for statistics')

    for code, name in statistics_dict.items():
        existing = engine.execute(
            statistics.select().where(statistics.c.code == code)
        )

        if existing.fetchone():
            engine.execute(
                statistics.update().where(statistics.c.code == code).values(name=name)
            )
        else:
            engine.execute(
                statistics.insert().values(code=code, name=name)
            )


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

    ''' Load Match Info '''
    engine.execute(
        matches.delete().where(matches.c.url == url)
    )
    engine.execute(
        matches.insert().values(match_data.replace({np.nan: None}).to_dict('records'))
    )

    ''' Load Match Statistics '''
    engine.execute(
        match_statistics.delete().where(match_statistics.c.url == url)
    )
    for statistic in statistics:
        if 'H' + statistic in df.columns:
            print(statistics.get(statistic))

            match_statistic_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_statistic_data['statistic'] = statistic
            match_statistic_data['home_stat'] = df['H' + statistic]
            match_statistic_data['away_stat'] = df['A' + statistic]
            engine.execute(
                match_statistics.insert().values(match_statistic_data.replace({np.nan: None}).to_dict('records'))
            )

    ''' Update Last Loaded Date '''
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
                (seasons.c.last_modified > seasons.c.last_loaded) | (seasons.c.last_loaded is None)
            ).order_by(seasons.c.last_modified.desc())
        )

    for season in results.fetchall():
        upload_file(season['url'])
