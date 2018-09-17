from dateutil.relativedelta import relativedelta
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
from models import Session, convert_to_py_type
import basic_method as bm
from models import table_exists

global_h_data = {}

# 回测的实现
# 2017-04-28 开始有150天线
class DayBacktest:
    def __init__(self, start_date='2017-04-28', end_date=datetime.now().strftime("%Y-%m-%d"), money=100000):
        self.truncate_tables()
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.profile = Pf(t='f')
        self.profile.set_money(money)
        self.sell_stocks_pool = []
        self.buy_stocks_pool = []

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
        df = ts.get_hist_data('000661', start=self.start_date.strftime('%Y-%m-%d'))
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True)
        df = df.loc[df.index >= self.start_date]
        df = df.loc[df.index <= self.end_date]
        return df.index

    def run(self):
        for dt in self.datetime_indexes():
            self.handle_bar(dt)

    def get_h_data_df(self, code, date):
        df = data_pool.get_hist_data_df(code, date)
        return df

    def start_d(self):
        '''
        开始时间减去一年，为了做移动平均值
        '''
        return self.start_date - relativedelta(years=1)

    def get_price(self, code, date):
        '''
        获得某个股票的收盘价格
        '''
        df = data_pool.get_hist_data_df(code, date)
        df = df.sort_index(ascending=True)
        return df.tail(1)['close'].sum()

    def get_open_price(self, code, date):
        df = data_pool.get_hist_data_df(code, date)
        df = df.sort_index(ascending=True)
        return df.tail(1)['open'].sum()

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

    def current_stocks(self):
        '''
        返回现在存在的股票models
        '''
        return self.profile.get_current_stocks()

    def block_size(self):
        '''
        每只股票占总资产的比例
        获得block的size
        '''
        return 0.2

    def get_block_money(self, date, type='close'):
        '''
        每只股票可以买的最大值
        获得block值
        '''
        all_count = self.profile.get_profile_account_thru_date(date, type=type)
        block_money = all_count * self.block_size()
        return block_money

    def stock_int(self, count):
        '''
        100的整数
        '''
        return int(count/100) * 100

class TestDayBacktest(DayBacktest):

    def handle_bar(self, date):
        code = '603568'
        df = self.get_hi_data_df(code, date)
        if bm.revert_point_1(df):
            print(date)


class SepaDayBacktest(DayBacktest):

    def handle_bar(self, date):
        '''
        每个时间窗口的处理函数
        '''
        # -------------------- 早上买和卖
        # 卖昨天指定需要卖的票
        self.sell_stocks(date)
        # 买昨天指定需要买的票
        self.buy_stocks(date)

        # -------------------- 下午判断买和卖
        self.stocks_pool = self.get_stocks_pool_thru_trend(date)
        # 第二天需要卖掉的股票
        self.sell_stocks_pool = self.get_sell_stocks_pool_thru_short_trend(date)
        # 第二天需要买的股票
        self.buy_stocks_pool = self.get_buy_stocks_pool_thru_short_trend(date)

    def sell_stocks(self, date):
        '''
        卖掉指定股票
        '''
        if len(self.sell_stocks_pool) > 0:
            for stock_item in self.sell_stocks_pool:
                code = stock_item['code']
                count = stock_item['count']
                self.profile.sell_stock(code, count, self.get_open_price(code, date), date=date)

    def buy_stocks(self, date):
        '''
        买股票
        '''
        # block_size = 0.2
        # all_count = self.profile.get_profile_account_thru_date(date, type='open')
        # block_money = all_count * block_size
        block_money = self.get_block_money(date, type='open')
        if len(self.buy_stocks_pool) > 0:
            for index, row in self.buy_stocks_pool.iterrows():
                code = row['code']
                price = self.get_price(code, date)
                stock = self.profile.get_current_stock(code, date=date)
                if stock.count == 0 and 100 * price <= self.profile.get_money():
                    count = self.stock_int(block_money / price)
                    self.profile.buy_stock(code, count, price, date=date)
                elif stock.count != 0 and 100 * price <= self.profile.get_money() and stock.price < price:
                    count = self.stock_int(block_money / price)
                    self.profile.buy_stock(code, count, price, date=date)

    def get_stocks_pool_thru_trend(self, date):
        '''
        根据长期sepa和基本面趋势模型
        获得股票池
        '''
        all_codes_df = self.get_all_codes_df()
        data = []
        labels = ['code', 'name', 'c_name']
        indexes = []
        for index, row in all_codes_df.iterrows():
            hist_df = self.get_h_data_df(row['code'], date)
            growth_df = self.get_growth_data_df(row['code'], date)
            if (hist_df is not None) and basic_method.sepa_step_check(hist_df, 3 * 20) and (growth_df is not None) and basic_method.growth_check(growth_df):
                data.append((row['code'], row['name'], row['c_name']))
                indexes.append(row['code'])
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        return df

    def get_sell_stocks_pool_thru_short_trend(self, date):
        '''
        获得明天需要卖掉的股票
        '''
        result = []
        leave_level = 0.10
        top_level = 0.20
        top_level_1 = 0.10
        stocks = self.current_stocks()
        session = Session()
        block_money = self.get_block_money(date, type='close')
        for stock in stocks:
            session.add(stock)

            max_price = stock.max_price or stock.price
            code = stock.code
            today_price = self.get_price(code, date)

            # 止损卖出
            # 跌了移动平均线的10%，第二天全部卖出
            if today_price <= max_price * (1 - leave_level):
                result.append({ 'code': code, 'count': stock.count })
            # 止盈卖出
            # 20%的时候，卖出至少3/4
            elif ((today_price - stock.price) / stock.price >= top_level) and stock.count * today_price > block_money * 1/4:
                if (stock.count * 1/2) >= 100:
                    result.append({ 'code': code, 'count': self.stock_int(stock.count * 1/2) })
                else:
                    result.append({ 'code': code, 'count': stock.count })

            # 10%的时候，卖出至少1/2
            elif ((today_price - stock.price) / stock.price >= top_level_1) and stock.count * today_price > block_money * 1/2:
                if stock.count * 1/2 >= 100:
                    result.append({ 'code': code, 'count': self.stock_int(stock.count * 1/2) })
                else:
                    result.append({ 'code': code, 'count': stock.count })

            # 设置移动最大值，目的移动止损点
            if today_price > max_price:
                stock.max_price = convert_to_py_type(today_price)

        session.commit()
        session.expunge_all()
        session.close()
        return result


    def get_buy_stocks_pool_thru_short_trend(self, date):
        '''
        获得明天需要买的股票
        '''
        code_df = self.stocks_pool.copy()
        result = []
        for index, row in code_df.iterrows():
            df = self.get_h_data_df(row['code'], date)
            result.append(bm.revert_point_1(df))
        return code_df.loc[result]
