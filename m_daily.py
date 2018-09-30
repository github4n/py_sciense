from dateutil.relativedelta import relativedelta
from pf import Pf
import pandas as pd
import stock_pool as sp
import tushare as ts
from datetime import datetime
from models import conn
from models import Session, FRecord, FStock, FProfile, industry_codes, FProfileList
import talib as ta
from sqlalchemy.sql import select
import basic_method
import data_pool
from models import Session, convert_to_py_type
import basic_method as bm
from models import table_exists
from backtest import DayBacktest

class MDaily(DayBacktest):
    def __init__(self):
        self.start_date = datetime.now()
        self.end_date = datetime.now()
        self.profile = Pf(t='m')
        self.sell_stocks_pool = []
        self.buy_stocks_pool = []

    def handle_bar(self, date):
        print('%s\n'%(date.to_pydatetime().strftime('%Y-%m-%d')))
        self.stocks_pool = self.get_stocks_pool_thru_trend(date)
        # 第二天需要卖掉的股票
        self.sell_stocks_pool = self.get_sell_stocks_pool_thru_short_trend(date)
        # 第二天需要买的股票
        self.buy_stocks_pool = self.get_buy_stocks_pool_thru_short_trend(date)
        # 打印要买的股票
        self.print_buy_stocks_pool()
        # 打印要卖的股票
        self.print_sell_stocks_pool()

    def print_buy_stocks_pool(self):
        for index, row in self.buy_stocks_pool.iterrows():
            ts_code = row['ts_code']
            print("%s要买入\n"%(ts_code))

    def print_sell_stocks_pool(self):
        for stock_item in self.sell_stocks_pool:
            ts_code = stock_item['ts_code']
            print("%s要卖出\n"%(ts_code))

    def get_stocks_pool_thru_trend(self, date):
        '''
        根据长期sepa和基本面趋势模型
        获得股票池
        '''
        all_codes_df = self.stock_basic_df()
        data = []
        labels = ['ts_code', 'code']
        indexes = []
        # 牛市的时候应该适当提高
        # 熊市的时候应该适当降低
        epsg_low_level = 40
        for index, row in all_codes_df.iterrows():
            ts_code = row['ts_code']
            code = row['code']
            growth_df = self.get_growth_data_df(code, date)
            if basic_method.sepa_check_from_cache(ts_code, date, 3 * 20) and \
               basic_method.rps_check(self.rps_data_df(ts_code, date)) and \
               basic_method.has_growth_than_x_for_n_quarters(growth_df, 1, epsg_low_level, type='epsg') and \
               basic_method.growth_check(growth_df):
                data.append((ts_code, code))
                indexes.append(ts_code)
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        return df

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

    def get_sell_stocks_pool_thru_short_trend(self, date):
        '''
        获得明天需要卖掉的股票
        '''
        result = []

        leave_level = 0.06
        leave_level_1 = 0.10

        top_level = 0.20
        top_level_1 = 0.15

        stocks = self.current_stocks()
        session = Session()

        for stock in stocks:
            session.add(stock)
            max_price = stock.max_price or stock.price
            price = stock.price
            ts_code = stock.code
            today_price = self.get_price(ts_code, date)
            if today_price is None:
                continue

            # 当股价没有涨过0.05就直接跌，那么0.08的跌幅离开
            # 当股价涨过了0.05，那么0.10的跌幅离开
            l_level = leave_level if ((max_price - price) / price) <= 0.05 else leave_level_1

            # 止损卖出
            # 跌了移动平均线的10%，第二天全部卖出
            if today_price <= max_price * (1 - l_level):
                result.append({ 'ts_code': ts_code, 'count': stock.count })
            # 止盈卖出
            # 20%的时候，卖出至少3/4
            elif ((today_price - stock.price) / stock.price >= top_level):
                if (stock.count * 1/2) >= 100:
                    result.append({ 'ts_code': ts_code, 'count': self.stock_int(stock.count * 1/2) })
                else:
                    result.append({ 'ts_code': ts_code, 'count': stock.count })

            # 15%的时候，卖出至少1/2
            elif ((today_price - stock.price) / stock.price >= top_level_1):
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
