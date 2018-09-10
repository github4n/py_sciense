import tushare as ts
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import select
import talib as ta
import numpy as np
from models import conn, industry_codes, engine
import datetime

SMA_FAST = 50
SMA_SLOW = 200
RSI_PERIOD = 14
RSI_AVG_PERIOD = 15
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
STOCH_K = 14
STOCH_D = 3
SIGNAL_TOL = 3
Y_AXIS_SIZE = 12


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

# SMA 指标
def add_sma(df, column, step=2):
    '''
    添加column的step大小的sma
    添加的column名字为 ``"SMA_%s_%s"%(column, step)``
    '''
    df = df.sort_index(ascending=True)
    df["SMA_%s_%s"%(column, step)] = ta.SMA(df[column].values, timeperiod=step)
    return df

# RSI 指标算法
def add_rsi(df, column='close', rsi_period=RSI_PERIOD):
    '''
    Relative Strength Index (RSI) is a momentum oscillator that measures the speed and change of price movements.
    RSI is considered overbought when above 70 and oversold when below 30
                 100
    RSI = 100 - --------
                 1 + RS

    RS = Average Gain / Average Loss
    First Average Gain = Sum of Gains over the past 14 periods / 14.
    First Average Loss = Sum of Losses over the past 14 periods / 14

    --------------------

    rsi_test 为 rsi 小于50卖方力量比价足, 并且开始转向向上
    '''
    # 确定Ascending排序
    df = df.sort_index(ascending=True)
    df['rsi'] = ta.RSI(df[column].values, rsi_period)
    df['sma_r'] = pd.Series.rolling(df.rsi, RSI_AVG_PERIOD).mean()
    # rsi 小于50卖方力量比价足, 并且开始转向向上
    df['rsi_test'] = np.where((df.rsi < 50) & (df.rsi > df.rsi.shift(1)), 1, 0)
    return df

# Stoch 指标
def add_stoch(df, stoch_k=STOCH_K, stoch_d=STOCH_D):
    '''
    Stochastic Oscillator is a momentum indicator comparing the closing price of a security to the range of its prices over a certain period of time.
    K = 100(C - L14)/(H14 - L14)
    C = the most recent closing price
    L14 = the low of the 14 previous trading sessions
    H14 = the highest price traded during the same 14-day period
    K = the current market rate for the currency pair
    D = 3-period moving average of K
    https://www.investopedia.com/terms/s/stochasticoscillator.asp

    --------------------

    stoch_k_test 是 stoch 小于50卖方力量比价足，并且开始转向向上
    '''
    df = df.sort_index(ascending=True)
    df['stoch_k'], df['stoch_d'] = ta.STOCH(df.high.values, df.low.values, df.close.values, slowk_period=stoch_k, slowd_period=stoch_d)
    df['stoch_k_test'] = np.where((df.stoch_k < 50) & (df.stoch_k > df.stoch_k.shift(1)), 1, 0)
    return df

# MACD 指标
def add_macd(df, column='close', fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL):
    '''
    The MACD Line is the difference between fast EMA and slow EMA. (dif)
    Signal line is 9 period EMA of the MACD Line. (dea)
    MACD Histogram is the difference between MACD Line and Signal line. (macd)

    --------------------
    macd_test 是 dif大于dea
    '''
    df = df.sort_index(ascending=True)
    df['macd'], df['macdSignal'], df['macdHist'] = ta.MACD(df[column].values, fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL)
    df['macd_test'] = np.where((df.macd > df.macdSignal), 1, 0)
    return df

def vcp_test(df, volumn_column='volume', close_column='close', days=10):
    '''
    VCP的检测
    '''
    volumn_df = df.copy()
    close_df = df.copy()

    # 成交量sma变小
    add_sma(volumn_df, volumn_column, step=5)
    k = "SMA_%s_%s"%(volumn_column, 5)
    volumn_df = volumn_df.sort_index(ascending=True)
    volumn_df['volumn_lower_test'] = np.where(volumn_df[k] <= volumn_df[k].shift(1), 1, 0)
    volumn_df = volumn_df.tail(days)

    # bias的绝对值的sma变小
    k2 = "SMA_%s_%s"%(close_column, 20)
    add_sma(close_df, close_column, step=20)
    close_df['ABS_BIAS'] = (close_df['close'] - close_df[k2]).abs()
    add_sma(close_df, 'ABS_BIAS', 20)
    k3 = "SMA_%s_%s"%('ABS_BIAS', 20)
    close_df = close_df.sort_index(ascending=True)
    close_df['bias_lower_test'] = np.where(close_df[k3] <= close_df[k3].shift(1), 1, 0)
    close_df = df.tail(days)

    return (volumn_df['volumn_lower_test'] == 1).all() and \
        (close_df['bias_lower_test'] == 1).all()