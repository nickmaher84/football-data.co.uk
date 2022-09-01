from sqlalchemy import MetaData, Table, engine_from_config
from configparser import ConfigParser
import pandas as pd

config = ConfigParser()
config.read('../load/alembic.ini')
engine = engine_from_config(config['alembic'])
metadata = MetaData(schema='football-data', bind=engine)


def get_data(competition_id, bookmaker_code):
    table = Table('betting-equation', metadata, autoload_with=engine)
    query = (
        table
        .select()
        .where(table.c.competition_id == competition_id)
        .where(table.c.bookmaker_code == bookmaker_code)
    )
    df = pd.read_sql(query, engine, 'id')

    print(df.head(3).T)


if __name__ == '__main__':
    get_data('6d10050aaf7634492eb4cb176be8ca21', 'Avg')