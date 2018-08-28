from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

engine = create_engine('mysql+pymysql://wpzero:1234@127.0.0.1/best_stocks')
metadata = MetaData()

industry_codes = Table('industry_codes', metadata,
                       Column('index', Integer, primary_key=True),
                       Column('name', String),
                       Column('code', String),
                       Column('c_name', String)
)

conn = engine.connect()
