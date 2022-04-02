from main.database import metadata
from sqlalchemy import Table, Column, Index
from sqlalchemy.types import String, Integer, Boolean, Numeric, Float, Date, Time, DateTime

matches = Table(
    'match', metadata,
    Column('division', String(3)),
    Column('country', String(20)),
    Column('league', String(100)),
    Column('season', String(100)),
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
    Index('uq_match', 'date', 'away_team', unique=True),
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

leagues = Table(
    'league', metadata,
    Column('division', String(3), primary_key=True),
    Column('league', String(100), nullable=False),
    Column('country_code', String(3), nullable=False),
    Index('uq_league', 'country_code', 'league', unique=True)
)

countries = Table(
    'country', metadata,
    Column('country_code', String(3), primary_key=True),
    Column('country', String(20), nullable=False, unique=True),
    Column('url', String(100), nullable=False, unique=True),
)

statistics = Table(
    'statistic', metadata,
    Column('code', String(3), primary_key=True),
    Column('name', String(20), unique=True)
)

match_statistics = Table(
    'match_statistics', metadata,
    Column('date', Date, primary_key=True, nullable=False),
    Column('statistic', String(3), primary_key=True, nullable=False, index=True),
    Column('home_team', String(100), primary_key=True, nullable=False),
    Column('home_stat', Integer),
    Column('away_stat', Integer),
    Column('away_team', String(100), nullable=False),
    Column('url', String(100), nullable=False, index=True),
    Index('uq_match_statistics', 'date', 'away_team', 'statistic', unique=True)
)

bookmakers = Table(
    'bookmaker', metadata,
    Column('code', String(5), primary_key=True),
    Column('name', String(20), nullable=False),
    Column('closing', Boolean(), nullable=False)
)

match_odds = Table(
    'match_odds', metadata,
    Column('date', Date, primary_key=True, nullable=False),
    Column('bookmaker', String(5), primary_key=True, nullable=False, index=True),
    Column('home_team', String(100), primary_key=True, nullable=False),
    Column('home', Float, nullable=False),
    Column('draw', Float, nullable=False),
    Column('away', Float, nullable=False),
    Column('away_team', String(100), nullable=False),
    Column('count', Integer),
    Column('url', String(100), nullable=False, index=True),
    Index('uq_match_odds', 'date', 'away_team', 'bookmaker', unique=True)
)

match_over_unders = Table(
    'match_over_under', metadata,
    Column('date', Date, primary_key=True, nullable=False),
    Column('bookmaker', String(5), primary_key=True, nullable=False, index=True),
    Column('home_team', String(100), primary_key=True, nullable=False),
    Column('away_team', String(100), nullable=False),
    Column('over', Float, nullable=False),
    Column('under', Float, nullable=False),
    Column('count', Integer),
    Column('url', String(100), nullable=False, index=True),
    Index('uq_match_over_under', 'date', 'away_team', 'bookmaker', unique=True)
)

match_asian_handicaps = Table(
    'match_asian_handicap', metadata,
    Column('date', Date, primary_key=True, nullable=False),
    Column('bookmaker', String(5), primary_key=True, nullable=False, index=True),
    Column('home_team', String(100), primary_key=True, nullable=False),
    Column('home', Float, nullable=False),
    Column('away', Float, nullable=False),
    Column('away_team', String(100), nullable=False),
    Column('handicap', Numeric(5, 2)),
    Column('count', Integer),
    Column('url', String(100), nullable=False, index=True),
    Index('uq_match_asian_handicap', 'date', 'away_team', 'bookmaker', unique=True)
)