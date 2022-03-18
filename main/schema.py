from database import metadata
from sqlalchemy import Table, Column, Index
from sqlalchemy.types import String, Integer, Date, Time


matches = Table(
    'match', metadata,
    Column('league_code', String(3)),
    Column('country_name', String(20)),
    Column('league_name', String(100)),
    Column('season_name', String(100)),
    Column('date', Date, primary_key=True),
    Column('time', Time),
    Column('home_team', String(50), primary_key=True),
    Column('home_goals', Integer),
    Column('result', String(1)),
    Column('away_goals', Integer),
    Column('away_team', String(50), nullable=False),
    Column('ht_home_goals', Integer),
    Column('ht_result', String(1)),
    Column('ht_away_goals', Integer),
    Column('referee', String(50)),
    Column('attendance', Integer),
    Column('url', String(100), nullable=False, index=True),
    Index('away_matches', 'date', 'away_team', unique=True),
)
