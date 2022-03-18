from sqlalchemy import create_engine, MetaData

engine = create_engine('driver://user:pass@localhost/dbname')
metadata = MetaData(schema='football-data', bind=engine)
