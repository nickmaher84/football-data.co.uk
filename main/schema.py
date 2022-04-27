from main.database import metadata
from sqlalchemy import Table, Column, Index
from sqlalchemy.types import String, DateTime, JSON


countries = Table(
    'country', metadata,
    Column('country_code', String(3), primary_key=True),
    Column('country', String(20), nullable=False, unique=True),
    Column('url', String(100), nullable=False, unique=True),
)

leagues = Table(
    'league', metadata,
    Column('division', String(3), primary_key=True),
    Column('league', String(100), nullable=False),
    Column('country_code', String(3), nullable=False),
    Index('uq_league', 'country_code', 'league', unique=True)
)

seasons = Table(
    'season', metadata,
    Column('url', String(100), primary_key=True),
    Column('division', String(3), nullable=False),
    Column('season', String(20), nullable=False),
    Column('last_modified', DateTime),
    Column('last_loaded', DateTime),
    Index('uq_season', 'division', 'season', unique=True)
)

raw_data = Table(
    'raw', metadata,
    Column('url', String(100), nullable=False, index=True),
    Column('json', JSON, nullable=False),
    Column('loaded_at', DateTime, nullable=False),
)
