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

# global variable to cache for performance
global_growth_data = {}
global_profit_data = {}

# SEPA 系统选股算法
# code: 股票的代码
# days: 200SMA递增时间
def sepa_step1_check(code, days):
    '''
    1. 200天均线是递增的，并且坚持了days天数据
    2. 当前收盘价大于五十天局限的值
    3. 五十天天均线值大于一百五十天的均线值
    4. 一百五十天的均线值大于两百天的均线值
    <5> 当前股价至少出于一年内最高股价的25%以内
    <6> 当前股价至少比最近一年最低股价至少高30%
    '''
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

def basic_info_step1_check(code, year, quarter):
    '''
    基本面的算法
    盈利三个季度
    净利润增速增加三个季度
    主营业务增速增加三个季度
    净利润增速大于20%三个季度
    主营业务增速大于20%三个季度
    每股收益增长率大于20%三个季度
    '''
    return has_profit_than_x_for_n_quarters(code ,year, quarter, 3, 0) and \
        growth_bigger_than_last_for_n_quarters(code, year, quarter, 3, 'nprg') and \
        growth_bigger_than_last_for_n_quarters(code, year, quarter, 3, 'mbrg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'nprg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'mbrg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'epsg')

def basic_info_step2_check(code, year, quarter):
    '''
    基本面的算法
    盈利三个季度
    sma净利润增速增加三个季度
    sma主营业务增速增加三个季度
    净利润增速大于20%三个季度
    主营业务增速大于20%三个季度
    每股收益增长率大于20%三个季度
    '''
    return has_profit_than_x_for_n_quarters(code ,year, quarter, 3, 0) and \
        has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'nprg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'mbrg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'epsg') and \
        growth_bigger_than_last_for_n_quarters_thru_sma(code, year, quarter, 3, 'nprg') and \
        growth_bigger_than_last_for_n_quarters_thru_sma(code, year, quarter, 3, 'mbrg')

def basic_info_step_check(code, year, quarter, up_quarter_count=3, big_up_quarter_count=2, lowlevel=20):
    '''
    基本面算法：
    1. 盈利, 并且保持up_quarter_count个季度
    2. 净利润增长率大于lowlevel，并且保持up_quarter_count个季度
    3. 主营业务收入增长率大于lowlevel，并且保持up_quarter_count个季度
    4. 每股收益增长率增长率大于lowlevel，并且保持up_quarter_count个季度
    5. sma净利润增速增加big_up_quarter_count个季度
    6. sma主营业务增速增加big_up_quarter_count个季度
    '''
    return has_profit_than_x_for_n_quarters(code ,year, quarter, up_quarter_count, 0) and \
        has_growth_than_x_for_n_quarters(code, year, quarter, up_quarter_count, lowlevel, 'nprg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, up_quarter_count, lowlevel, 'mbrg') and \
        has_growth_than_x_for_n_quarters(code, year, quarter, up_quarter_count, lowlevel, 'epsg') and \
        growth_bigger_than_last_for_n_quarters_thru_sma(code, year, quarter, big_up_quarter_count, 'nprg') and \
        growth_bigger_than_last_for_n_quarters_thru_sma(code, year, quarter, big_up_quarter_count, 'mbrg')

def growth_bigger_than_last_for_n_quarters_thru_sma(code, year, quarter, n, column='nprg', step=2):
    '''
    计算增长sma加速
    '''
    df = get_growth_data_df_thru_code(code, year, quarter, n + step)
    df = add_sma(df, column, step)
    k = "SMA_%s_%s"%(column, step)
    df = df.sort_index(ascending=True)
    sma_column = df.loc[:, [k]]
    diff_df = sma_column.diff()
    diff_df = diff_df.sort_index(ascending=True).iloc[step:]
    return (diff_df[k] > 0).all()


def add_sma(df, column, step=2):
    '''
    添加column的step大小的sma
    添加的column名字为 ``"SMA_%s_%s"%(column, step)``
    '''
    df = df.sort_index(ascending=True)
    df["SMA_%s_%s"%(column, step)] = ta.SMA(df[column].values, timeperiod=step)
    return df

def add_rsi(df, column='clse', rsi_period=14):
    '''
    Relative Strength Index (RSI) is a momentum oscillator that measures the speed and change of price movements.
    RSI is considered overbought when above 70 and oversold when below 30
                 100
    RSI = 100 - --------
                 1 + RS

    RS = Average Gain / Average Loss
    First Average Gain = Sum of Gains over the past 14 periods / 14.
    First Average Loss = Sum of Losses over the past 14 periods / 14
    '''
    return df
def add_stoch(df):
    '''
    Stochastic Oscillator is a momentum indicator comparing the closing price of a security to the range of its prices over a certain period of time.
    K = 100(C - L14)/(H14 - L14)
    C = the most recent closing price
    L14 = the low of the 14 previous trading sessions
    H14 = the highest price traded during the same 14-day period
    K = the current market rate for the currency pair
    D = 3-period moving average of K
    https://www.investopedia.com/terms/s/stochasticoscillator.asp
    '''

def add_macd(df, column='close', fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL):
    '''
    The MACD Line is the difference between fast EMA and slow EMA. (dif)
    Signal line is 9 period EMA of the MACD Line. (dea)
    MACD Histogram is the difference between MACD Line and Signal line. (macd)

    '''
    df['macd'], df['macdSignal'], df['macdHist'] = ta.MACD(df[column].values, fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL)
    df['macd_test'] = np.where((df.macd > df.macdSignal), 1, 0)
    return df

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

def has_growth_info_for_n_quarters(code, year, quarter, n):
    '''
    是否有季度财报 for n 个季度
    '''
    g_dfs = []
    for i in range(n):
        if i == 0:
            g_dfs.append(get_growth_data_thru_code(code, year, quarter))
        else:
            year, quarter = get_last_quarter(year, quarter)
            g_dfs.append(get_growth_data_thru_code(code, year, quarter))
    exist = True
    for p_df in g_dfs:
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
        result = result and (p_df[type] > x).all()
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
        result = result and (g_df[type] >= x).all()
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
        if new_p is None:
            new_p = g_df[type]
        else:
            new_p = g_df[type]
            bigger = bigger and (new_p > g_df[type]).all()
    return bigger

def get_profit_data_df_thru_code(code, year, quarter, n):
    '''
    最近n个季度的profit data 的dataframe, 以时间为index
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
    dfs = []
    indexes = []
    for i in range(n):
        if i != 0:
            year, quarter = get_last_quarter(year, quarter)
        indexes.append(convert_year_quarter_to_datetime(year, quarter))
        # iloc slice convert dataframe to seriazer
        dfs.append(get_profit_data_thru_code(code, year, quarter).iloc[0])
    df = pd.DataFrame.from_records(dfs, index=indexes)
    df.index.name = 'date'
    return df

def get_growth_data_df_thru_code(code, year, quarter, n):
    '''
    最近n个季度的growth信息的dataframe, 以时间为index
    code,代码
    name,名称
    mbrg,主营业务收入增长率(%)
    nprg,净利润增长率(%)
    nav,净资产增长率
    targ,总资产增长率
    epsg,每股收益增长率
    seg,股东权益增长率
    '''
    dfs = []
    indexes = []
    for i in range(n):
        if i != 0:
            year, quarter = get_last_quarter(year, quarter)
        indexes.append(convert_year_quarter_to_datetime(year, quarter))
        dfs.append(get_growth_data_thru_code(code, year, quarter).iloc[0])
    df = pd.DataFrame.from_records(dfs, index=indexes)
    df.index.name = 'date'
    return df

def convert_year_quarter_to_datetime(year, quarter):
    '''
    提供year和quarter然后返回datetime
    '''
    date_str = "%s-%s"%(year, quarter * 3)
    return datetime.datetime.strptime(date_str, '%Y-%m')

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
    print("\nGet %s profit on %s year and %s quarter\n"%(code, year, quarter))
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
    print("\nGet %s growth info on %s year and %s quarter\n"%(code, year, quarter))
    k = "%s-%s"%(year, quarter)
    global global_growth_data
    if k in global_growth_data:
        df = global_growth_data[k]
    else:
        df = ts.get_growth_data(year, quarter)
        global_growth_data[k] = df
    return df[df['code'] == code]

def get_code_data_from_industry_codes(code):
    '''
    返回该code的股票信息，从industry_codes表中抓取
    '''
    sql_str = "select * from `industry_codes` where `industry_codes`.`code` = '%s'"%(code)
    result = conn.execute(sql_str)
    r_list = list(result)
    if len(r_list) == 0:
        print("\n 没有在industry codes 中找到该code %s\n"%(code))
        return None
    return dict(zip(result.keys(), r_list[0]))

def get_max_dd_codes_df(vol=400, type=[-1, 0, 1], date=None):
    '''
    1, -1, 0: 买盘、卖盘、中性盘
    返回成交量倒序的df
    '''
    s = select([industry_codes])
    result = conn.execute(s)
    data = []
    indexes = []
    labels = ['code', 'name', 'c_name', 'vol']
    for row in result:
        n = get_dd_sum_thru_code(row['code'], date=date, vol=vol, type=type)
        if n is None:
            data.append((row['code'], row['name'], row['c_name'], 0))
        else:
            data.append((row['code'], row['name'], row['c_name'], n))
        indexes.append(row['code'])
    r_df = pd.DataFrame.from_records(data, columns=labels, index=indexes)
    r_df.index.name = 'code_index'
    r_df = r_df.sort_values(by=['vol'], ascending = False)
    return r_df

# 获得一个股票大单的量
def get_dd_sum_thru_code(code, vol=400, type=[-1, 0, 1], date=None):
    '''
    1, -1, 0: 买盘、卖盘、中性盘
    返回最后的总和
    '''
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    type_options = {
        1: '买盘',
        -1: '卖盘',
        0: '中性盘'
    }
    type_list = []

    for i in type:
        type_list.append(type_options[i])

    df = ts.get_sina_dd(code, date=date, vol=vol)

    if df is None:
        return None
    else:
        return df[df['type'].isin(type_list)]['volume'].sum()

def get_dd_df_thru_code(code, vol=400, date=None):
    '''
    获得某个股票的大单df
    '''
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    df = ts.get_sina_dd(code, date=date, vol=vol)
    return df

def get_hist_dd_thru_code(code, vol=400, type=[-1 , 0, 1], date=None):
    '''
    获得code的dd数据
    返回dataframe
    '''
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    type_options = {
        1: '买盘',
        -1: '卖盘',
        0: '中性盘'
    }
    type_list = []

    for i in type:
        type_list.append(type_options[i])

    df = ts.get_sina_dd(code, date=date, vol=vol)

    if df is None:
        return None
    else:
        return df[df['type'].isin(type_list)]


# 从数据库中加载到dataframe
def load_codes_from_db(table_name, index_column = 'code_index'):
    return pd.read_sql("select * from `%s`"%(table_name), con=conn, index_col=index_column)

# 把一个表里面的股票，进行basic_info_step1的验证
def basic_info_step1_for_sepa(table_name, year, quarter):
    df = load_codes_from_db(table_name)
    succ_arr = []
    fail_arr = []
    no_basic_arr = []
    for index, row in df.iterrows():
        if has_basic_info_for_n_quarters(row['code'], year, quarter, 3)  and has_growth_info_for_n_quarters(row['code'], year, quarter, 3):
            if basic_info_step1_check(row['code'], year, quarter):
                succ_arr.append(row['name'])
            else:
                fail_arr.append(row['name'])
        else:
            no_basic_arr.append(row['name'])
    for name in succ_arr:
        print("\n%s 通过了"%(name))
    for name in fail_arr:
        print("\n%s 失败了"%(name))
    for name in no_basic_arr:
        print("\n%s 暂时没有中报"%(name))


# 把一个表里的数据，进行basic_info_step2的验证
def basic_info_step2_for_sepa(table_name, year, quarter):
    df = load_codes_from_db(table_name)
    succ_arr = []
    fail_arr = []
    no_basic_arr = []
    for index, row in df.iterrows():
        print("\n处理 %s\n"%(row['name']))
        if has_basic_info_for_n_quarters(row['code'], year, quarter, 5) and has_growth_info_for_n_quarters(row['code'], year, quarter, 5):
            if basic_info_step2_check(row['code'], year, quarter):
                succ_arr.append(row['name'])
            else:
                fail_arr.append(row['name'])
        else:
            no_basic_arr.append(row['name'])
    for name in succ_arr:
        print("\n%s 通过了"%(name))
    for name in fail_arr:
        print("\n%s 失败了"%(name))
    for name in no_basic_arr:
        print("\n%s 暂时没有中报"%(name))

def basic_info_step_for_sepa(table_name, year, quarter, up_quarter_count=3, big_up_quarter_count=2, lowlevel=20):
    df = load_codes_from_db(table_name)
    succ_arr = []
    fail_arr = []
    no_basic_arr = []
    for index, row in df.iterrows():
        print("\n处理 %s\n"%(row['name']))
        if has_basic_info_for_n_quarters(row['code'], year, quarter, 5) and has_growth_info_for_n_quarters(row['code'], year, quarter, 5):
            if basic_info_step_check(row['code'], year, quarter, up_quarter_count, big_up_quarter_count, lowlevel):
                succ_arr.append(row['name'])
            else:
                fail_arr.append(row['name'])
        else:
            no_basic_arr.append(row['name'])
    for name in succ_arr:
        print("\n%s 通过了"%(name))
    for name in fail_arr:
        print("\n%s 失败了"%(name))
    for name in no_basic_arr:
        print("\n%s 暂时没有中报"%(name))
    return None

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
