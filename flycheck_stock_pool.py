import tushare as ts
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import talib as ta
import numpy as np
from models import conn, industry_codes, engine
import datetime


# global variable to cache for performance
global_growth_data = {}
global_profit_data = {}

# SEPA 系统选股算法
# code: 股票的代码
# days: 200SMA递增时间
def sepa_step1_check(code, days):
    df = ts.get_hist_data(code)
    if df is None:
        return False
    df = df.sort_index(ascending=True)
    # SMA 50 150 200
    for i in [50, 150, 200]:
        df['SMA{}'.format(i)] = ta.SMA(df.close.values, timeperiod=i)
    # SMA200 diff
    sma200_df = df.loc[:, ['SMA200']]
    df['df_v'] = sma200_df.diff()
    df = df.sort_index(ascending=False)
    sma200_up_ndays = True
    for index, row in (df[0:days]).iterrows():
        sma200_up_ndays = sma200_up_ndays and (row['df_v'] >= 0)
    cur_value = df.close.values[0]
    cur_sma50 = df.SMA50.values[0]
    cur_sma150 = df.SMA150.values[0]
    cur_sma200 = df.SMA200.values[0]
    return sma200_up_ndays and (cur_value >= cur_sma50) and (cur_sma50 > cur_sma150) and (cur_sma150 > cur_sma200)

def basic_info_step1_check(code, year, quarter):
    '''
    基本面的算法
    盈利三个季度
    净利润增速增加三个季度
    主营业务增速增加三个季度
    净利润增速大于20%三个季度
    主营业务增速大于20%三个季度
    '''
    return has_profit_than_x_for_n_quarters(code ,year, quarter, 3, 0) and growth_bigger_than_last_for_n_quarters(code, year, quarter, 3, 'nprg') and growth_bigger_than_last_for_n_quarters(code, year, quarter, 3, 'mbrg') and has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'nprg') and has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'mbrg')

def has_basic_info_for_n_quarters(code, year, quarter, n):
    '''
    是否有季度财报 for n 个季度
    '''
    p_dfs = []
    for i in range(n):
        if i == 0:
            p_dfs.append(get_profit_data_thru_code(code, year, quarter))
        else:
            year, quarter = get_last_quarter(year, quarter)
            p_dfs.append(get_profit_data_thru_code(code, year, quarter))
    exist = True
    for p_df in p_dfs:
        exist = exist and len(p_df) > 0
    return exist


def has_profit_than_x_for_n_quarters(code, year, quarter, n, x, type = 'net_profits'):
    '''
    已经盈利信息大于等于x for n quarters
    type:
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
    p_dfs = []
    for i in range(n):
        if i == 0:
            p_dfs.append(get_profit_data_thru_code(code, year, quarter))
        else:
            year, quarter = get_last_quarter(year, quarter)
            p_dfs.append(get_profit_data_thru_code(code, year, quarter))

    result = True
    for p_df in p_dfs:
        positive = positive and p_df[type] > x
    return result

def has_growth_than_x_for_n_quarters(code, year, quarter, n, x, type = 'nprg'):
    '''
    连续n个季度有增长
    type:
    mbrg,主营业务收入增长率(%)
    nprg,净利润增长率(%)
    nav,净资产增长率
    targ,总资产增长率
    epsg,每股收益增长率
    seg,股东权益增长率
    '''
    g_dfs = []
    for i in range(n):
        if i == 0:
            g_dfs.append(get_growth_data_thru_code(code, year, quarter))
        else:
            year, quarter = get_last_quarter(year, quarter)
            g_dfs.append(get_growth_data_thru_code(code, year, quarter))

    result = True
    for g_df in g_dfs:
        result = result and g_df[type] >= x
    return result



def growth_bigger_than_last_for_n_quarters(code, year, quarter, n, type = 'nprg'):
    '''
    加速增长
    type:
    mbrg,主营业务收入增长率(%)
    nprg,净利润增长率(%)
    nav,净资产增长率
    targ,总资产增长率
    epsg,每股收益增长率
    seg,股东权益增长率
    '''
    g_dfs = []
    for i in range(n):
        if i == 0:
            g_dfs.append(get_growth_data_thru_code(code, year, quarter))
        else:
            year, quarter = get_last_quarter(year, quarter)
            g_dfs.append(get_growth_data_thru_code(code, year, quarter))
    bigger = True
    new_p = None
    for g_df in g_dfs:
        if not (new_p is None):
            bigger = bigger and new_p > g_df[type]
            new_p = g_df[type]
        else:
            new_p = g_df[type]

def get_last_quarter(year, quarter):
    '''
    获得上个季度
    '''
    quarter = quarter - 1
    if quarter == 0:
        year = year - 1
        quarter = 4
    return (year, quarter)

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
        df = ts.get_profit_data(year, quarter)
        global_profit_data[k] = df
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
    k = "%s-%s"%(year, quarter)
    global global_profit_data
    if k in global_growth_data:
        df = global_growth_data[key]
    else:
        df = ts.get_growth_data(year, quarter)
        global_growth_data[k] = df
    return df[df['code'] == code]


# 产生具体的SEPA的股票数据
# 存在 {date}_days_sepa_step1_codes 数据库表中
def produce_sepa_codes(monthes):
    # 获得所有行业的股票
    s = select([industry_codes])
    result = conn.execute(s)
    data = []
    indexes = []
    labels = ['code', 'name', 'c_name']
    days = monthes * 20
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    for row in result:
        if sepa_step1_check(row['code'], days):
            data.append((row['code'], row['name'], row['c_name']))
            indexes.append(row['code'])
    r_df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
    r_df.index.name = 'code_index'
    r_df.to_sql(today_str + '_' + str(days) +  '_' + 'sepa_step1_codes', engine, if_exists='replace', dtype={'code_index': sa.VARCHAR(255)})
