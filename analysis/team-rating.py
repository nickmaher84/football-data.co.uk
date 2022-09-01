from sqlalchemy import MetaData, Table, engine_from_config
from configparser import ConfigParser
import pandas as pd
from numpy import exp, asarray
import statsmodels.api as sm

config = ConfigParser()
config.read('../load/alembic.ini')
engine = engine_from_config(config['alembic'])
metadata = MetaData(schema='football-data', bind=engine)


def run_model(competition_ids, variables=2, mu=0):
    if type(competition_ids) != list:
        competition_ids = [competition_ids]

    table = Table('team_match', metadata, autoload_with=engine)
    query = (
        table
        .select()
        .where(table.c.competition_id.in_(competition_ids))
    )
    df = pd.read_sql(query, engine)

    # print(df.head(2).T)
    weights, teams, goals = transform_df(df, variables, mu)

    poisson = sm.families.Poisson()
    model = sm.GLM(goals, teams, poisson, freq_weights=weights)
    result = model.fit()

    print(result.summary())


def transform_df(df, variables, mu):
    df['one'] = 1
    df['home'] = (df['home_away'] == 'H').astype(int)
    w = exp(mu * (df['date'] - pd.to_datetime('now', utc=True).date()).dt.days)
    y = df['goals_for']

    if variables == 0:
        x = pd.DataFrame()
        x['Team Strength'] = df['one']
        x['Home Advantage'] = df['home']

    elif variables == 1:
        attack = df.pivot(columns='team_id', values='one').fillna(0)
        defend = df.pivot(columns='opponent_id', values='one').fillna(0)

        x = attack - defend
        x['Home Advantage'] = df['home']

    elif variables == 2:
        df['team_id'] += ' (Attack)'
        df['opponent_id'] += ' (Defend)'

        attack = df.pivot(columns='team_id', values='one').fillna(0)
        defend = df.pivot(columns='opponent_id', values='one').fillna(0)

        x = attack.merge(-defend, left_index=True, right_index=True)
        x['Home Advantage'] = df['home']

    elif variables == 3:
        df['home_adv'] = df['team_id'] + ' (Home Advantage)'
        df['team_id'] += ' (Attack)'
        df['opponent_id'] += ' (Defend)'

        attack = df.pivot(columns='team_id', values='one').fillna(0)
        defend = df.pivot(columns='opponent_id', values='one').fillna(0)
        home = df.pivot(columns='home_adv', values='home').fillna(0)

        x = attack.merge(-defend, left_index=True, right_index=True)
        x = x.merge(home, left_index=True, right_index=True)

    elif variables == 4:
        df['home_attack'] = df['team_id'] + ' (Home Attack)'
        df['home_defend'] = df['opponent_id'] + ' (Home Defend)'
        df['away_attack'] = df['team_id'] + ' (Away Attack)'
        df['away_defend'] = df['opponent_id'] + ' (Away Defend)'
        df['away'] = 1 - df['home']

        home_attack = df.pivot(columns='home_attack', values='home').fillna(0)
        home_defend = df.pivot(columns='home_defend', values='home').fillna(0)
        away_attack = df.pivot(columns='away_attack', values='away').fillna(0)
        away_defend = df.pivot(columns='away_defend', values='away').fillna(0)

        home = home_attack.merge(-home_defend, left_index=True, right_index=True)
        away = away_attack.merge(-away_defend, left_index=True, right_index=True)
        x = home.merge(away, left_index=True, right_index=True)

    else:
        raise KeyError

    return w, x, y


if __name__ == '__main__':
    run_model('6d10050aaf7634492eb4cb176be8ca21', 0)
    run_model('6d10050aaf7634492eb4cb176be8ca21', 1)
    run_model('6d10050aaf7634492eb4cb176be8ca21', 2)
    run_model('6d10050aaf7634492eb4cb176be8ca21', 3)
    run_model('6d10050aaf7634492eb4cb176be8ca21', 4)