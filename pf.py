from models import Session, MRecord, MStock, MProfile, FRecord, FStock, FProfile
from datetime import datetime
import tushare as ts
import pandas as pd

class Pf:
    def __init__(self, t='m'):
        self.t = t

    def set_money(self, money):
        profile = self.get_profile()
        profile.money = money
        session = Session()
        session.add(profile)
        session.commit()
        session.close()

    def get_money(self):
        return self.get_profile().money

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

    def get_current_stocks(self):
        session = Session()
        stocks = session.query(self.get_stock_class()).filter_by(status=1).all()
        session.close()
        return stocks

    def buy_stock(self, code, count, price):
        '''
        买股票
        '''
        need_money = count * price
        profile = self.get_profile()
        if need_money > profile.money:
            return
        session = Session()
        record = self.get_record_class()(code=code, count=count, price=price, date=datetime.now(), type=1)
        stock = self.get_current_stock(code)
        session.add(record)
        session.add(profile)
        session.add(stock)
        stock.price = ((stock.count * stock.price) + (count * price)) / (stock.count + count)
        stock.count = stock.count + count
        profile.money = profile.money - need_money
        session.commit()
        session.close()

    def sell_stock(self, code, count, price):
        '''
        卖股票
        '''
        all_money = count * price
        profile = self.get_profile()
        stock = self.get_current_stock(code)
        if stock.count < count:
            return
        session = Session()
        record = self.get_record_class()(code=code, count=count, price=price, date=datetime.now(), type=0)
        session.add(record)
        session.add(stock)
        session.add(profile)
        if stock.count > count:
            stock.count = stock.count - count
            new_stop_stock = self.get_stock_class()(code=code, count=count, price=stock.price, date=stock.date, stop_date=datetime.now(), status=0, stop_price=price)
            session.add(new_stop_stock)
        else:
            stock.status = 0
            stock.stop_date = datetime.now()
        profile.money = profile.money + all_money
        session.commit()
        session.close()

    def get_current_stock(self, code):
        session = Session()
        stock = session.query(self.get_stock_class()).filter_by(code=code, status=1).first()
        if stock is None:
            stock = self.get_stock_class()(code=code, status=1, date=datetime.now(), count=0, price=0)
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
