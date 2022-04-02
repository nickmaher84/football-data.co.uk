from main.database import engine
from main.schema import matches, statistics, match_statistics, bookmakers, match_odds, match_over_unders, match_asian_handicaps, seasons

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
bookmakers_dict = {
    'Avg': 'Market Average',
    'AvgC': 'Market Average (Closing)',
    'Max': 'Market Maximum',
    'MaxC': 'Market Maximum (Closing)',
    'Bb': 'Betbrain',
    'BbAv': 'Betbrain Average',
    'BbMx': 'Betbrain Maximum',
    'B365': 'Bet365',
    'B365C': 'Bet365 (Closing)',
    'BW': 'Bet&Win',
    'BWC': 'Bet&Win (Closing)',
    'IW': 'Interwetten',
    'IWC': 'Interwetten (Closing)',
    'PS': 'Pinnacle',
    'PSC': 'Pinnacle (Closing)',
    'VC': 'Victor Chandler',
    'VCC': 'Victor Chandler (Closing)',
    'WH': 'William Hill',
    'WHC': 'William Hill (Closing)',
    'BS': 'Blue Square',
    'GB': 'Gamebookers',
    'LB': 'Ladbrokes',
    'P': 'Pinnacle',
    'SB': 'SportingBet',
    'SJ': 'Stan James',
    'SO': 'SportingOdds',
    'SY': 'StanleyBet',
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


def upload_bookmakers():
    print('Loading data for bookmakers')

    for code, name in bookmakers_dict.items():
        existing = engine.execute(
            bookmakers.select().where(bookmakers.c.code == code)
        )

        if existing.fetchone():
            engine.execute(
                bookmakers.update().where(bookmakers.c.code == code).values(name=name.replace(' (Closing)', ''), closing='(Closing)' in name)
            )
        else:
            engine.execute(
                bookmakers.insert().values(code=code, name=name.replace(' (Closing)', ''), closing='(Closing)' in name)
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
    delete_records_for_url(matches, url)
    insert_records(matches, match_data)

    ''' Load Match Statistics '''
    delete_records_for_url(match_statistics, url)

    for statistic in statistics_dict:
        if 'H' + statistic in df.columns:
            print(statistics_dict.get(statistic))

            match_statistic_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_statistic_data['statistic'] = statistic
            match_statistic_data['home_stat'] = df['H' + statistic]
            match_statistic_data['away_stat'] = df['A' + statistic]

            insert_records(match_statistics, match_statistic_data)

    ''' Load Match Statistics '''
    delete_records_for_url(match_odds, url)
    delete_records_for_url(match_over_unders, url)
    delete_records_for_url(match_asian_handicaps, url)

    for bookmaker in bookmakers_dict:
        if bookmaker + 'H' in df.columns:
            print('{bookmaker} - Match Odds'.format(bookmaker=bookmakers_dict.get(bookmaker)))

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
            print('{bookmaker} - Over/Under'.format(bookmaker=bookmakers_dict.get(bookmaker)))

            match_over_under_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_over_under_data['bookmaker'] = bookmaker
            match_over_under_data['over'] = df[bookmaker + '>2.5']
            match_over_under_data['under'] = df[bookmaker + '<2.5']

            if 'Bb' in bookmaker:
                match_over_under_data['count'] = df['BbOU']

            match_over_under_data = match_over_under_data.dropna(axis=0, subset=['over', 'under'])
            insert_records(match_over_unders, match_over_under_data)

        if bookmaker + 'AHH' in df.columns:
            print('{bookmaker} - Asian Handicap'.format(bookmaker=bookmakers_dict.get(bookmaker)))

            match_asian_handicap_data = match_data[['date', 'home_team', 'away_team', 'url']].copy()
            match_asian_handicap_data['bookmaker'] = bookmaker
            match_asian_handicap_data['home'] = df[bookmaker + 'AHH']
            match_asian_handicap_data['away'] = df[bookmaker + 'AHA']

            if 'Bb' in bookmaker:
                match_asian_handicap_data['count'] = df['BbAH'].apply(lambda x: str(x).replace(u'\xa0', '').replace('nan', '0'))
                match_asian_handicap_data['handicap'] = df['BbAHh'].apply(lambda x: str(x).split(',')[0])

            elif bookmaker + 'AH' in df.columns:
                match_asian_handicap_data['handicap'] = df[bookmaker + 'AH']

            elif 'Closing' in bookmakers_dict[bookmaker]:
                match_asian_handicap_data['handicap'] = df['AHCh']

            else:
                match_asian_handicap_data['handicap'] = df['AHh']

            match_asian_handicap_data = match_asian_handicap_data.dropna(axis=0, subset=['home', 'away'])
            insert_records(match_asian_handicaps, match_asian_handicap_data)

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


def delete_records_for_url(table, url):
    engine.execute(
        table.delete().where(table.c.url == url)
    )


def insert_records(table, df):
    engine.execute(
        table.insert().values(df.replace({np.nan: None}).to_dict('records'))
    )
