from models import Session, MRecord, MStock, MProfile, FRecord, FStock, FProfile, convert_date, FProfileList, MProfileList
from datetime import datetime
import tushare as ts
import pandas as pd
import numpy as np
import data_pool

class Pf:
    def __init__(self, t='m'):
        self.t = t

    def set_money(self, money):
        money = self.convert_to_py_type(money)
        profile = self.get_profile()
        profile.money = self.convert_to_py_type(money)
        session = Session()
        session.add(profile)
        session.commit()
        session.expunge_all()
        session.close()

    def get_money(self):
        return self.get_profile().money

    def get_profile_account_thru_date(self, date, type='close'):
        '''
        用于回测
        获得账号内现金和股票的市值
        '''
        return self.get_profile().money + self.get_profile_stocks_df_thru_date(date, type=type)['account'].sum()

    def get_profile_stocks_df_thru_date(self, date, type='close'):
        '''
        用于回测
        获得所有股票的dataframe, 用于回测
        '''
        data = []
        labels = ['code', 'price', 'trade_price', 'date', 'count', 'account']
        indexes = []
        current_stocks = self.get_current_stocks()
        for stock in current_stocks:
            if type == 'close':
                trade_price = data_pool.get_price(stock.code, date)
            else:
                trade_price = data_pool.get_open_price(stock.code, date)
            data.append((stock.code, stock.price, trade_price, stock.date, stock.count, trade_price * stock.count))
            indexes.append(stock.code)
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        df.index.name = 'code_index'
        return df

    def get_profile_account(self):
        '''
        获得账号内现金和股票的市值
        '''
        return self.get_profile().money + self.get_profile_stocks_df()['account'].sum()

    def get_profile_stocks_df(self):
        '''
        获得股票的dataframe
        code:股票代码
        price:购买的价格
        trade_price:当前的价格
        account:所有的市值
        date:购买日期
        count:数量
        '''
        data = []
        labels = ['code', 'price', 'trade_price', 'date', 'count', 'account']
        indexes = []
        current_stocks = self.get_current_stocks()
        today_df = ts.get_today_all()
        for stock in current_stocks:
            stock_df = today_df.loc[today_df['code'] == stock.code]
            trade_price = stock_df.iloc[0]['trade']
            data.append((stock.code, stock.price, trade_price, stock.date, stock.count, trade_price * stock.count))
            indexes.append(stock.code)
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        df.index.name = 'code_index'
        return df

    def convert_date(self, date):
        '''
        获得有效的时间
        '''
        return convert_date(date)

    def get_current_stocks(self):
        session = Session()
        stocks = session.query(self.get_stock_class()).filter_by(status=1).all()
        session.expunge_all()
        session.close()
        return stocks

    def buy_stock(self, code, count, price, date=None):
        '''
        买股票
        '''
        date = self.convert_date(date)
        count = self.convert_to_py_type(count)
        price = self.convert_to_py_type(price)
        if count == 0:
            return
        session = Session()
        need_money = count * price
        profile = self.get_profile()
        session.add(profile)
        if need_money > profile.money:
            session.expunge_all()
            session.close()
            return
        record = self.get_record_class()(code=code, count=count, price=price, date=date, type=1)
        stock = self.get_current_stock(code, date=date)
        session.add(record)
        session.add(stock)
        stock.price = self.convert_to_py_type(((stock.count * stock.price) + (count * price)) / (stock.count + count))
        stock.count = self.convert_to_py_type(stock.count + count)
        profile.money = self.convert_to_py_type(profile.money - need_money)
        session.commit()
        session.expunge_all()
        session.close()

    def sell_stock(self, code, count, price, date=None):
        '''
        卖股票
        '''
        date = self.convert_date(date)
        count = self.convert_to_py_type(count)
        price = self.convert_to_py_type(price)
        if count == 0:
            return
        all_money = count * price
        profile = self.get_profile()
        stock = self.get_current_stock(code, date=date)
        session = Session()
        session.add(stock)
        session.add(profile)
        if stock.count < count:
            session.expunge_all()
            session.close()
            return
        record = self.get_record_class()(code=code, count=count, price=price, date=date, type=0)
        session.add(record)
        if stock.count > count:
            stock.count = self.convert_to_py_type(stock.count - count)
            new_stop_stock = self.get_stock_class()(code=code, count=count, price=stock.price, date=stock.date, stop_date=date, status=0, stop_price=price)
            session.add(new_stop_stock)
        else:
            stock.status = 0
            stock.stop_price = price
            stock.stop_date = date
        profile.money = self.convert_to_py_type(profile.money + all_money)
        session.commit()
        session.expunge_all()
        session.close()

    def snapshot(self, date=None):
        '''
        记录某一时刻的股票情况
        '''
        try:
            session = Session()
            date = self.convert_date(date)
            stocks = []
            stocks_df = self.get_profile_stocks_df_thru_date(date, type='close')
            for index, row in stocks_df.iterrows():
                stock_d = self.convert_date(row['date'])
                stock_d = None if stock_d is None else stock_d.strftime('%Y-%m-%d')
                stocks.append({'code': self.convert_to_py_type(row['code']),
                               'price': self.convert_to_py_type(row['price']),
                               'trade_price': self.convert_to_py_type(row['trade_price']),
                               'date': stock_d,
                               'count': self.convert_to_py_type(row['count']),
                               'account': self.convert_to_py_type(row['account'])})
            record = self.get_profile_list_class()(money=self.convert_to_py_type(self.get_money()), profile_account=self.convert_to_py_type(self.get_profile_account_thru_date(date, type='close')), date=date, stocks=stocks)
            session.add(record)
            session.commit()
        except Exception as e:
            print("%s"%(str(e)))
            session.rollback()
        finally:
            session.close()

    def convert_to_py_type(self, v):
        '''
        转化为python默认的类型
        '''
        if isinstance(v, np.generic):
            return np.asscalar(v)
        else:
            return v

    def get_current_stock(self, code, date=None):
        date = self.convert_date(date)
        session = Session()
        stock = session.query(self.get_stock_class()).filter_by(code=code, status=1).first()
        if stock is None:
            stock = self.get_stock_class()(code=code, status=1, date=date, count=0, price=0)
        session.expunge_all()
        session.close()
        return stock

    def get_profile(self):
        session = Session()
        profile = session.query(self.get_profile_class()).first()
        if profile is None:
            new_profile = self.get_profile_class()(money=0.0)
            session.add(new_profile)
            session.commit()
        profile = session.query(self.get_profile_class()).first()
        session.expunge_all()
        session.close()
        return profile

    def get_profile_class(self):
        if self.t == 'm':
            return MProfile
        else:
            return FProfile

    def get_record_class(self):
        if self.t == 'm':
            return MRecord
        else:
            return FRecord

    def get_stock_class(self):
        if self.t == 'm':
            return MStock
        else:
            return FStock

    def get_profile_list_class(self):
        if self.t == 'm':
            return MProfileList
        else:
            return FProfileList
