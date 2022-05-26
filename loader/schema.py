from database import metadata
from sqlalchemy import Table, Column, Index, ForeignKey
from sqlalchemy.types import String, DateTime, JSON


countries = Table(
    'country', metadata,
    Column('country_code', String(3), primary_key=True),
    Column('country_name', String(20), nullable=False, unique=True),
    Column('url', String(100), nullable=False, unique=True),
)

leagues = Table(
    'league', metadata,
    Column('division', String(3), primary_key=True),
    Column('league_name', String(100), nullable=False),
    Column('country_code', String(3), ForeignKey('country.country_code'), nullable=False),
    Index('uq_league', 'country_code', 'league_name', unique=True)
)

seasons = Table(
    'season', metadata,
    Column('url', String(100), primary_key=True),
    Column('division', String(3), ForeignKey('league.division'), nullable=False),
    Column('season_name', String(20), nullable=False),
    Column('last_modified', DateTime),
    Column('last_loaded', DateTime),
    Index('uq_season', 'division', 'season_name', unique=True)
)

raw_data = Table(
    'raw', metadata,
    Column('url', String(100), ForeignKey('season.url'), nullable=False, index=True),
    Column('json', JSON, nullable=False),
    Column('loaded_at', DateTime, nullable=False),
)


def deploy_schema(overwrite=False):
    if overwrite:
        metadata.drop_all()

    metadata.create_all()


if __name__ == '__main__':
    deploy_schema(True)
