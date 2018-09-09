from pf import Pf
import pandas as pd
import stock_pool as sp
import tushare as ts
from datetime import datetime
from models import conn
from models import Session, FRecord, FStock, FProfile, industry_codes
import talib as ta
from sqlalchemy.sql import select
import basic_method
import data_pool

# 回测的实现
# 2017-04-28 开始有150天线
class DayBacktest:
    def __init__(self, start_date='2017-04-28', end_date=datetime.now().strftime("%Y-%m-%d"), money=100000):
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.profile = Pf(t='f')
        self.profile.set_money(money)
        self.truncate_tables()

    def truncate_tables(self):
        for model in [FRecord, FStock, FProfile]:
            conn.execute(model.__table__.delete())

    def handle_bar(self, date):
        '''
        每个时间窗口的处理函数
        '''
        print(date)

    def datetime_indexes(self):
        '''
        获得时间序列
        '''
        df = ts.get_hist_data('000661')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True)
        df = df.loc[df.index >= self.start_date].loc[df.index <= self.end_date]
        return df.index

    def run(self):
        for dt in self.datetime_indexes():
            self.handle_bar(dt)

    def get_hist_data_df(self, code, date):
        '''
        获得某个股票的历史数据
        '''
        return data_pool.get_hist_data_df(code, date)

    def get_all_codes_df(self):
        '''
        获得所有的股票
        '''
        return data_pool.get_all_codes_df()

    def get_profit_data_df(self, code, date):
        '''
        获得股票的profit的dataframe
        '''
        return data_pool.get_profit_data_df(code, date)


class SepaDayBacktest(DayBacktest):
    def handle_bar(self, date):
        '''
        每个时间窗口的处理函数
        '''
    def sepa_step_check(self, code, date, days):
        '''
        1. 200天均线是递增的，并且坚持了days天数据
        2. 当前收盘价大于五十天局限的值
        3. 五十天天均线值大于一百五十天的均线值
        4. 一百五十天的均线值大于两百天的均线值
        <5> 当前股价至少出于一年内最高股价的25%以内
        <6> 当前股价至少比最近一年最低股价至少高30%
        '''
        df = self.get_hist_data(code, date)
        return basic_method.sepa_step_check(df, days)
