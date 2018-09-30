import sqlalchemy as sa
from sqlalchemy import create_engine
import tushare as ts
from datetime import datetime, timedelta
import data_pool as dp
import time
import backtest
from models import FetchDataVersion, Session

class FetchData():
    def run(self):
        '''
        抓取数据到数据库
        '''
        print('开始FetchData....................')
        v = self.version()
        self.start_date = (v.date + timedelta(days=1)).strftime('%Y%m%d')
        self.end_date = datetime.now().strftime('%Y%m%d')

        self.fetch_stock_basic()
        self.fetch_daily_data()
        self.compute_rps()
        self.compute_sepa()
        try:
            session = Session()
            v.start_date = datetime.now()
            session.add(v)
            session.commit()
            print('结束FetchData....................')
        except:
            session.rollback()
        finally:
            session.close()

    def fetch_stock_basic(self):
        '''
        抓取更新股票池
        '''
        dp.fetch_stock_basic_to_db()

    def fetch_daily_data(self):
        '''
        抓取最新的daily到数据库
        '''
        codes_df = dp.stock_basic_df()
        for index, row in codes_df.iterrows():
            dp.fetch_daily_data_to_db(row['ts_code'], end_date=self.end_date)

    def compute_rps(self):
        '''
        计算rps
        '''
        start_date = datetime.strptime(self.start_date, '%Y%m%d').strftime('%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y%m%d').strftime('%Y-%m-%d')
        c_rps = backtest.ComputeRps(start_date=start_date, end_date=end_date)
        c_rps.run()

    def compute_sepa(self):
        '''
        计算sepa
        '''
        start_date = datetime.strptime(self.start_date, '%Y%m%d').strftime('%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y%m%d').strftime('%Y-%m-%d')
        sepa = backtest.ComputeSepa(start_date=start_date, end_date=end_date)
        sepa.run()

    def version(self):
        try:
            session = Session()
            return session.query(FetchDataVersion).first()
        finally:
            session.close()
