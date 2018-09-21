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

    def trade_cal(self):
        '''
        获得时间序列
        '''
        df = data_pool.trade_cal(self.start_date, self.end_date)
        return df

    def run(self):
        for index, row in self.trade_cal().iterrows():
            if row['sse_is_open'] == 1 or row['szse_is_open'] == 1:
                self.sse_is_open = row['sse_is_open']
                self.szse_is_open = row['szse_is_open']
                self.today = row['date']
                self.handle_bar(self.today)

    def get_daily_data_df(self, ts_code, date):
        '''
        获得日线数据
        '''
        return data_pool.get_daily_data_df(ts_code, date)

    def start_d(self):
        '''
        开始时间减去一年，为了做移动平均值
        '''
        return self.start_date - relativedelta(years=1)

    def get_price(self, ts_code, date):
        '''
        获得某个股票的收盘价格
        '''
        return data_pool.get_price(ts_code, date)

    def get_open_price(self, ts_code, date):
        '''
        获得某个股票的开盘价格
        '''
        return data_pool.get_open_price(ts_code, date)

    def stock_basic_df(self):
        '''
        获得所有的股票
        '''
        return data_pool.stock_basic_df()

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

    def convert_ts_code_to_code(self, ts_code):
        '''
        ts_code 2 code
        '''
        return data_pool.convert_ts_code_to_code(ts_code)

    def convert_code_to_ts_code(self, code):
        '''
        code 2 ts_code
        '''
        return data_pool.convert_code_to_ts_code(code)

class TestDayBacktest(DayBacktest):

    def handle_bar(self, date):
        ts_code = self.convert_code_to_ts_code('603568')
        df = self.get_daily_data_df(ts_code, date)
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
                ts_code = stock_item['ts_code']
                count = stock_item['count']
                price = self.get_open_price(ts_code, date)
                if price is None:
                    continue
                self.profile.sell_stock(ts_code, count, price, date=date)

    def buy_stocks(self, date):
        '''
        买股票
        '''
        block_money = self.get_block_money(date, type='open')
        if len(self.buy_stocks_pool) > 0:
            for index, row in self.buy_stocks_pool.iterrows():
                ts_code = row['ts_code']
                price = self.get_open_price(ts_code, date)
                if price is None:
                    next
                stock = self.profile.get_current_stock(ts_code, date=date)
                if stock.count == 0 and 100 * price <= self.profile.get_money():
                    count = self.stock_int(block_money / price)
                    self.profile.buy_stock(ts_code, count, price, date=date)
                elif stock.count != 0 and 100 * price <= self.profile.get_money() and stock.price < price:
                    count = self.stock_int(block_money / price)
                    self.profile.buy_stock(ts_code, count, price, date=date)

    def get_stocks_pool_thru_trend(self, date):
        '''
        根据长期sepa和基本面趋势模型
        获得股票池
        '''
        all_codes_df = self.stock_basic_df()
        data = []
        labels = ['ts_code', 'code']
        indexes = []
        for index, row in all_codes_df.iterrows():
            ts_code = row['ts_code']
            code = row['code']
            daily_df = self.get_daily_data_df(ts_code, date)
            growth_df = self.get_growth_data_df(code, date)
            if (daily_df is not None) and basic_method.sepa_step_check(daily_df, 3 * 20) and (growth_df is not None) and basic_method.growth_check(growth_df):
                data.append((ts_code, code))
                indexes.append(ts_code)
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        return df

    def get_sell_stocks_pool_thru_short_trend(self, date):
        '''
        获得明天需要卖掉的股票
        '''
        result = []
        leave_level = 0.10
        top_level = 0.20
        top_level_1 = 0.15
        stocks = self.current_stocks()
        session = Session()
        block_money = self.get_block_money(date, type='close')

        for stock in stocks:
            session.add(stock)
            max_price = stock.max_price or stock.price
            ts_code = stock.code
            today_price = self.get_price(ts_code, date)
            if today_price is None:
                continue

            # 止损卖出
            # 跌了移动平均线的10%，第二天全部卖出
            if today_price <= max_price * (1 - leave_level):
                result.append({ 'ts_code': ts_code, 'count': stock.count })
            # 止盈卖出
            # 20%的时候，卖出至少3/4
            elif ((today_price - stock.price) / stock.price >= top_level) and stock.count * today_price > block_money * 1/4:
                if (stock.count * 1/2) >= 100:
                    result.append({ 'ts_code': ts_code, 'count': self.stock_int(stock.count * 1/2) })
                else:
                    result.append({ 'ts_code': ts_code, 'count': stock.count })

            # 15%的时候，卖出至少1/2
            elif ((today_price - stock.price) / stock.price >= top_level_1) and stock.count * today_price > block_money * 1/2:
                if stock.count * 1/2 >= 100:
                    result.append({ 'ts_code': ts_code, 'count': self.stock_int(stock.count * 1/2) })
                else:
                    result.append({ 'ts_code': ts_code, 'count': stock.count })

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
        codes_df = self.stocks_pool.copy()
        result = []
        for index, row in codes_df.iterrows():
            df = self.get_daily_data_df(row['ts_code'], date)
            result.append(bm.revert_point_1(df))
        return codes_df.loc[result]
