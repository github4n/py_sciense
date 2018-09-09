import tushare as ts
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import talib as ta
import numpy as np
from models import conn, industry_codes, engine
import datetime

def sepa_step_check(df, days):
    '''
    1. 200天均线是递增的，并且坚持了days天数据
    2. 当前收盘价大于五十天局限的值
    3. 五十天天均线值大于一百五十天的均线值
    4. 一百五十天的均线值大于两百天的均线值
    <5> 当前股价至少出于一年内最高股价的25%以内
    <6> 当前股价至少比最近一年最低股价至少高30%
    '''
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
    one_year_max = df.iloc[:261]['close'].max()
    one_year_min = df.iloc[:261]['close'].min()
    sma200_up_ndays = True
    for index, row in (df[0:days]).iterrows():
        sma200_up_ndays = sma200_up_ndays and (row['df_v'] >= 0)
    cur_value = df.close.values[0]
    cur_sma50 = df.SMA50.values[0]
    cur_sma150 = df.SMA150.values[0]
    cur_sma200 = df.SMA200.values[0]
    # <5>
    high_result = (one_year_max - cur_value)/one_year_max < 0.25
    # <6>
    low_result = (cur_value - one_year_min)/one_year_min > 0.3
    return sma200_up_ndays and (cur_value >= cur_sma50) and (cur_sma50 > cur_sma150) and (cur_sma150 > cur_sma200) and high_result and low_result

def growth_check(df, up_quarter=3, bigger_quarter=2, nprg_lowlevel=20, epsg_lowlevel=20, mbrg_lowlevel=10, step=2):
    '''
    基本面增速检测
    '''
    rows = up_quarter if up_quarter > bigger_quarter else bigger_quarter
    rows = rows + step
    return len(df) >= rows and \
        has_growth_than_x_for_n_quarters(df, up_quarter, nprg_lowlevel, type='nprg') and \
        has_growth_than_x_for_n_quarters(df, up_quarter, epsg_lowlevel, type='epsg') and \
        has_growth_than_x_for_n_quarters(df, up_quarter, mbrg_lowlevel, type='mbrg') and \
        has_growth_bigger_for_n_quarters_thru_sma(df, bigger_quarter, column='nprg') and \
        has_growth_bigger_for_n_quarters_thru_sma(df, bigger_quarter, column='mbrg') and \
        has_growth_bigger_for_n_quarters_thru_sma(df, bigger_quarter, column='epsg')

def has_growth_bigger_for_n_quarters_thru_sma(df, n, column='nprg', step=2):
    '''
    连续n个季度，加速增长
    '''
    df = add_sma(df, column, step)
    k = "SMA_%s_%s"%(column, step)
    df = df.sort_index(ascending=True)
    sma_column = df.loc[:, [k]]
    diff_df = sma_column.diff()
    diff_df = diff_df.sort_index(ascending=False)
    diff_df = diff_df.head(n)
    return (diff_df[k] > 0).all()

def has_growth_than_x_for_n_quarters(df, n, x, type='nprg'):
    '''
    连续n个季度有增长, 并且大于x
    type:
    mbrg,主营业务收入增长率(%)
    nprg,净利润增长率(%)
    nav,净资产增长率
    targ,总资产增长率
    epsg,每股收益增长率
    seg,股东权益增长率
    '''
    df = df.sort_index(ascending=False)
    df = df.head(n)
    return (df[type] >= x).all()


def add_sma(df, column, step=2):
    '''
    添加column的step大小的sma
    添加的column名字为 ``"SMA_%s_%s"%(column, step)``
    '''
    df = df.sort_index(ascending=True)
    df["SMA_%s_%s"%(column, step)] = ta.SMA(df[column].values, timeperiod=step)
    return df
