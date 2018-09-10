import tushare as ts
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import talib as ta
import numpy as np
from models import conn, industry_codes, engine
import datetime
from sqlalchemy.sql import select
from models import Session, FRecord, FStock, FProfile, industry_codes

# global variable to cache for performance
global_growth_data = {}
global_profit_data = {}
global_hist_data = {}
all_codes_df = []

def get_hist_data_df(code, date):
    '''
    获得hist data
    '''
    global global_hist_data
    if code in global_hist_data:
        df = global_hist_data[code].copy()
    else:
        df = ts.get_hist_data(code)
        df.index = pd.to_datetime(df.index)
        global_profit_data[code] = df.copy()
    df = df.sort_index(ascending=True)
    df = df.loc[df.index <= date]
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
    return datetime.datetime.strptime(date_str, '%Y-%m')


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
    print("\nGet %s profit on %s year and %s quarter\n"%(code, year, quarter))
    k = "%s-%s"%(year, quarter)
    global global_profit_data
    if k in global_profit_data:
        df = global_profit_data[k]
    else:
        df = ts.get_profit_data(year, quarter)
        global_profit_data[k] = df
    if df is None:
        return None
    else:
        return df[df['code'] == code]


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
    print("\nGet %s growth info on %s year and %s quarter\n"%(code, year, quarter))
    k = "%s-%s"%(year, quarter)
    global global_growth_data
    if k in global_growth_data:
        df = global_growth_data[k]
    else:
        df = ts.get_growth_data(year, quarter)
        global_growth_data[k] = df
    if df is None:
        return None
    else:
        return df[df['code'] == code]
