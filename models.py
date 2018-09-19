from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Index, Float, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declared_attr
import numpy as np
from datetime import datetime
import pandas as pd

engine = create_engine('mysql+pymysql://wpzero:1234@127.0.0.1/best_stocks', connect_args={'connect_timeout': 800})
metadata = MetaData()
conn = engine.connect()
Base = declarative_base()
Session = sessionmaker(bind=engine)

industry_codes = Table('industry_codes', metadata,
                       Column('index', Integer, primary_key=True),
                       Column('name', String),
                       Column('code', String),
                       Column('c_name', String)
)

def convert_to_py_type(v):
    '''
    转化为python默认的类型
    '''
    if isinstance(v, np.generic):
        return np.asscalar(v)
    else:
        return v


def table_exists(table_name):
    '''
    判断table_name是否存在
    '''
    return engine.dialect.has_table(conn, table_name)

def df_to_db(table_name, df, if_exists='replace'):
    '''
    save df to db
    '''
    if (df is not None) and len(df) > 0:
        df.to_sql(table_name, engine, if_exists=if_exists, dtype={'date': sa.VARCHAR(255)})

def convert_date(date):
    '''
    获得有效的时间
    '''
    date = datetime.now() if date is None else date
    if isinstance(date, pd.Timestamp):
        date = date.to_pydatetime()
    return date

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    fullname = Column(String(255))
    password = Column(String(255))
    __table_args__ = (Index('composite_index', 'name', 'fullname'), )

    def __repr__(self):
       return "<User(name='%s', fullname='%s', password='%s')>" % (self.name, self.fullname, self.password)

class RecordMixin(object):
    id = Column(Integer, primary_key=True)
    code = Column(String(255), nullable=False, index=True)
    price = Column(Float, index=True, nullable=False)
    date = Column(DateTime, nullable=False, index=True)
    count = Column(Integer, nullable=False)
    # O:卖
    # 1:买
    type = Column(Integer, nullable=False)

class StockMixin(object):
    id = Column(Integer, primary_key=True)
    code = Column(String(255), nullable=False, index=True)
    price = Column(Float, index=True, nullable=False)
    max_price = Column(Float, index=True)
    stop_price = Column(Float, index=True)
    date = Column(DateTime, nullable=False)
    stop_date = Column(DateTime)
    # 1:在账
    # 0:结账
    status = Column(Integer, nullable=False)
    count = Column(Integer, nullable=False)
    data = Column(JSON)

class ProfileMixin(object):
    id = Column(Integer, primary_key=True)
    money = Column(Float)

# 记录每一笔买入或者卖出
class MRecord(RecordMixin, Base):
    __tablename__ = 'm_records'

# 记录一次交易
class MStock(StockMixin, Base):
    __tablename__ = 'm_stocks'
    __table_args__ = (Index('date_status', 'date', 'status'), )

# 股票账户
class MProfile(ProfileMixin, Base):
    __tablename__ = 'm_profiles'

class FRecord(RecordMixin, Base):
    __tablename__ = 'f_records'

class FStock(StockMixin, Base):
    __tablename__ = 'f_stocks'
    __table_args__ = (Index('date_status', 'date', 'status'), )

class FProfile(ProfileMixin, Base):
    __tablename__ = 'f_profiles'

class Rps(Base):
    __tablename__ = 'rps'
    id = Column(Integer, primary_key=True)
    code = Column(String(255), nullable=False, index=True)
    date = Column(DateTime, nullable=False)
    days = Column(Integer, nullable=False)
    value = Column(Float, nullable=False)
    __table_args__ = (Index('code', 'days', 'date'),)

class ExtrsList(Base):
    __tablename__ = 'extrs_list'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    days = Column(Integer, nullable=False)
    data = Column(JSON)
    __table_args__ = (Index('days', 'date'),)

# 用来创建数据库结构
Base.metadata.create_all(engine)
