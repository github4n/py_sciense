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

    def get_growth_data_df(self, code, date):
        '''
        获得股票的增长信息的dataframe
        '''
        return data_pool.get_growth_data_df(code, date)


class SepaDayBacktest(DayBacktest):
    def handle_bar(self, date):
        '''
        每个时间窗口的处理函数
        '''
        # 卖昨天指定需要卖的票
        self.sell_stocks(date)
        # 买昨天指定需要买的票
        self.buy_stocks(date)

        self.stocks_pool = self.get_stocks_pool_thru_trend(date)

        # 第二天需要卖掉的股票
        self.sell_stocks_pool = self.get_sell_stocks_pool_thru_short_trend(date)
        # 第二天需要买的股票
        self.buy_stocks_pool = self.get_buy_stocks_pool_thru_short_trend(date)

    def sell_stocks(date):
        '''
        卖掉指定股票

        '''

    def buy_stocks(date):
        '''
        买股票
        '''

    def get_stocks_pool_thru_trend(self, date):
        '''
        根据长期sepa和基本面趋势模型
        获得股票池
        '''
        all_codes_df = self.get_all_codes_df()
        data = []
        labels = ['code', 'name', 'cname']
        indexes = []
        for index, row in all_codes_df.iterrows():
            hist_df = self.get_hist_data_df(row['code'], date)
            growth_df = self.get_growth_data_df(row['code'], date)
            if basic_method.sepa_step_check(hist_df, 3 * 20) and basic_method.growth_check(growth_df):
                data.append((row['code'], row['name']), row['cname'])
                indexes.append(row['code'])
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        return df

    def score_stocks_thru_short_trend(self, date):
        '''
        根据短期的趋势来给股票打分
        '''
        df = self.stocks_pool.copy()
        # 打分的操作
        return df

    def get_sell_stocks_pool_thru_short_trend(self, date):
        '''
        获得明天需要卖掉的股票
        '''

    def get_buy_stocks_pool_thru_short_trend(self, date):
        '''
        获得明天需要买的股票
        '''
        df = self.score_stocks_thru_short_trend(date)
        df = df.sort_values(by=['score'], ascending=False)
        df = df.loc[df['code'] >= 8]
        return df
