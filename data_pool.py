import tushare as ts
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import talib as ta
import numpy as np
from models import conn, industry_codes, engine, convert_date
from datetime import datetime, timedelta
from sqlalchemy.sql import select
from models import Session, FRecord, FStock, FProfile, industry_codes, table_exists, df_to_db
import time

# global variable to cache for performance
global_growth_data = {}
global_profit_data = {}
global_hist_data = {}
global_daily_data = {}
all_codes_df = []
global_stock_basic_df = []
# 抓取h_data的开始时间
start_d = '2010-01-01'

daily_start = '20100101'

TOKEN = '415f2e5e4461aa8d71e8d0f2142e95007d19eaed3ed8d90eddf29f46'
ts.set_token(TOKEN)

pro = ts.pro_api()

def daily_data_table_name(ts_code):
    return "%s_daily"%(ts_code.replace('.', '_'))

def get_daily_data_df(ts_code, date):
    '''
    从数据库中获取daily数据
    '''
    global global_daily_data
    if ts_code in global_daily_data:
        df = global_daily_data[ts_code]
    else:
        df = get_daily_data(ts_code)
        if df is not None:
            df.index = pd.to_datetime(df.index)
            df = df.sort_index(ascending=True)
        global_daily_data[ts_code] = df
    if df is None:
        return df
    return df.loc[df.index <= date]

def get_daily_data(ts_code):
    table_name = daily_data_table_name(ts_code)
    if table_exists(table_name):
        df = read_df_from_db(table_name, index_col='date')
        df.index = pd.to_datetime(df.index)
        return df
    else:
        return None

def fetch_daily_data_to_db(ts_code, end_date=None):
    '''
    抓日数据
    '''
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    if not isinstance(end_date, str):
        end_date = convert_date(end_date).strftime("%Y%m%d")
    start_date = get_daily_start_date(ts_code)
    table_name = daily_data_table_name(ts_code)
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    df.index = pd.to_datetime(df['trade_date'])
    df.index.name = 'date'
    if table_exists(table_name):
        df_to_db(table_name, df, if_exists='append')
    else:
        df_to_db(table_name, df)

def get_daily_start_date(ts_code):
    table_name = daily_data_table_name(ts_code)
    if table_exists(table_name):
        old_df = get_daily_data(ts_code)
        if old_df is None or len(old_df) == 0:
            start_date = daily_start
        else:
            old_df = old_df.sort_index(ascending=True)
            start_date = (convert_date(old_df.index[-1]) + timedelta(days=1)).strftime("%Y%m%d")
    else:
        start_date = daily_start
    return start_date

def fetch_stock_basic_to_db():
    '''
    获得股票列表到数据库
    '''
    df = pro.stock_basic(exchange_id='')
    df_to_db('stock_basic', df)


def fetch_trade_cal_to_db(start_date='20100101', end_date='2020-01-01'):
    '''
    抓取交易日历到数据库
    '''
    sse_df = pro.trade_cal(exchange_id='SSE', start_date=start_date, end_date=end_date)
    szse_df = pro.trade_cal(exchange_id='SZSE', start_date=start_date, end_date=end_date)
    sse_df['sse_is_open'] = sse_df['is_open']
    sse_df['szse_is_open'] = szse_df['is_open']
    sse_df['date'] = pd.to_datetime(sse_df['cal_date'])
    df = sse_df.loc[:, ['sse_is_open', 'szse_is_open']]
    df.index = sse_df['date']
    df_to_db('trade_cal', df)

def stock_basic_df():
    '''
    股票列表
    '''
    global global_stock_basic_df
    if len(global_stock_basic_df) == 0:
        df = read_df_from_db('stock_basic', index_col='ts_code')
        df['ts_code'] = df.index
        df['code'] = df['symbol']
        global_stock_basic_df = df.copy()
    else:
        df = global_stock_basic_df.copy()
    return df

def convert_code_to_ts_code(code):
    '''
    code to ts_code
    '''
    df = stock_basic_df()
    df = df.loc[df['code'] == code]
    if len(df) == 0:
        return None
    else:
        return df.iloc[-1]['ts_code']

def trade_cal(start_date, end_date):
    '''
    获得指定时间段的交易日历
    '''
    df = read_df_from_db('trade_cal', index_col='date')
    df = df.sort_index(ascending=True)
    df.index = pd.to_datetime(df.index)
    df['date'] = df.index
    df = df.loc[df['date'] >= start_date]
    df = df.loc[df['date'] <= end_date]
    return df

def read_df_from_db(table_name, index_col='date'):
    '''
    从数据库中读取数据组成dataframe
    '''
    return pd.read_sql_table(table_name, engine,index_col=index_col)

def get_hist_data_df(code, date):
    '''
    获得hist data
    '''
    global global_hist_data
    if code in global_hist_data:
        if global_hist_data[code] is None:
            return None
        df = global_hist_data[code].copy()
    else:
        df = get_hist_data(code)
        if df is None:
            global_hist_data[code] = None
            return df
        df.index = pd.to_datetime(df.index)
        global_hist_data[code] = df.copy()
    df = df.sort_index(ascending=True)
    df = df.loc[df.index <= date]
    return df

def get_price(ts_code, date):
    '''
    获得该天股票的close price
    '''
    df = get_daily_data_df(ts_code, date)
    df = df.sort_index(ascending=True)
    if df is None or len(df) == 0 or df.index[-1] != date:
        return None
    else:
        return df.tail(1)['close'].sum()

def get_open_price(ts_code, date):
    '''
    获得该天股票的open price
    '''
    df = get_daily_data_df(ts_code, date)
    df = df.sort_index(ascending=True)
    if df is None or len(df) == 0 or df.index[-1] != date:
        return None
    else:
        return df.tail(1)['open'].sum()

def get_h_data(code):
    '''
    获取数据，并且存在数据库中
    '''
    if code in ['600656', '600832', '600806', '601268', '600222']:
        return None

    table_name = "%s_hlong"%(code)
    if table_exists(table_name):
        return read_df_from_db(table_name, index_col='date')
    else:
        time.sleep(60 * 3)
        try:
            df = ts.get_h_data(code, start=start_d)
            df_to_db(table_name, df)
            return df
        except Exception as inst:
            print(inst)
            print("%s\n"%(code))
            return None

def get_hist_data(code):
    '''
    获取数据，并且存在数据库中
    '''
    table_name = "%s_hist"%(code)
    if table_exists(table_name):
        return read_df_from_db(table_name, index_col='date')
    else:
        df = ts.get_hist_data(code)
        df_to_db(table_name, df)
        return df

def get_all_codes_df():
    '''
    获得所有的股票代码的dataframe
    '''
    global all_codes_df
    if len(all_codes_df) == 0:
        sql = select([industry_codes])
        result = conn.execute(sql)
        data = []
        labels = ['code', 'name', 'c_name']
        indexes = []
        for row in result:
            data.append((row['code'], row['name'], row['c_name']))
            indexes.append(row['code'])
        df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
        all_codes_df = df
    return all_codes_df.copy()

def get_profit_data_df(code, date):
    '''
    获得股票的基本面信息
    '''
    month = date.month
    year = date.year
    data = []
    indexes = []
    if month in range(1, 5):
        quarter = 1
    elif month in range(6, 9):
        quarter = 2
    else:
        quarter = 3
    for i in range(10):
        if i != 0:
            year, quarter = get_last_quarter(year, quarter)
        profit_df = get_profit_data_thru_code(code, year, quarter)
        if (profit_df is not None) and (len(profit_df) > 0):
            indexes.append(convert_year_quarter_to_datetime(year, quarter))
            data.append(profit_df.iloc[0])
    df = pd.DataFrame.from_records(data, index=indexes)
    df.index.name = 'date'
    return df

def get_growth_data_df(code, date):
    '''
    获得股票的成长信息
    '''
    month = date.month
    year = date.year
    data = []
    indexes = []
    if month in range(1, 5):
        quarter = 1
    elif month in range(6, 9):
        quarter = 2
    else:
        quarter = 3
    for i in range(10):
        if i != 0:
            year, quarter = get_last_quarter(year, quarter)
        growth_df = get_growth_data_thru_code(code, year, quarter)
        if (growth_df is not None) and (len(growth_df) > 0):
            indexes.append(convert_year_quarter_to_datetime(year, quarter))
            data.append(growth_df.iloc[0])
    df = pd.DataFrame.from_records(data, index=indexes)
    df.index.name = 'date'
    return df

def convert_ts_code_to_code(ts_code):
    if '.' in ts_code:
        return ts_code.split('.')[0]
    else:
        return ts_code

def get_last_quarter(year, quarter):
    '''
    获得上个季度
    '''
    quarter = quarter - 1
    if quarter == 0:
        year = year - 1
        quarter = 4
    return (year, quarter)

def convert_year_quarter_to_datetime(year, quarter):
    '''
    提供year和quarter然后返回datetime
    '''
    date_str = "%s-%s"%(year, quarter * 3)
    return datetime.strptime(date_str, '%Y-%m')


def get_profit_data_thru_code(code, year, quarter):
    '''
    获得单个股票的利润情况
    code,代码
    name,名称
    roe,净资产收益率(%)
    net_profit_ratio,净利率(%)
    gross_profit_rate,毛利率(%)
    net_profits,净利润(万元)
    esp,每股收益
    business_income,营业收入(百万元)
    bips,每股主营业务收入(元)
    '''
    k = "%s-%s"%(year, quarter)
    global global_profit_data
    if k in global_profit_data:
        df = global_profit_data[k]
    else:
        df = get_profit_data(year, quarter)
        global_profit_data[k] = df
    if df is None:
        return None
    else:
        return df[df['code'] == code]

def get_profit_data(year, quarter):
    '''
    抓取，可以存在数据库
    '''
    k = "%s-%s"%(year, quarter)
    table_name = "%s_profit"%(k)
    if table_exists(table_name):
        df = read_df_from_db(table_name, index_col='code')
        df['code'] = df.index
    else:
        try:
            df = ts.get_profit_data(year, quarter)
            df_to_db(table_name, df)
        except OSError as e:
            df = None
    return df

def get_growth_data(year, quarter):
    '''
    抓取，可以存在数据库
    '''
    k = "%s-%s"%(year, quarter)
    table_name = "%s_growth"%(k)
    if table_exists(table_name):
        df = read_df_from_db(table_name, index_col='code')
        df['code'] = df.index
    else:
        try:
            df = ts.get_growth_data(year, quarter)
            df_to_db(table_name, df)
        except OSError as e:
            df = None
    return df

def get_growth_data_thru_code(code, year, quarter):
    '''
    获得单个股票的成长情况
    code,代码
    name,名称
    mbrg,主营业务收入增长率(%)
    nprg,净利润增长率(%)
    nav,净资产增长率
    targ,总资产增长率
    epsg,每股收益增长率
    seg,股东权益增长率
    '''
    k = "%s-%s"%(year, quarter)
    global global_growth_data
    if k in global_growth_data:
        df = global_growth_data[k]
    else:
        df = get_growth_data(year, quarter)
        global_growth_data[k] = df
    if df is None:
        return None
    else:
        return df[df['code'] == code]
