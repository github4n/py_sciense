import matplotlib.pyplot as plt
import stock_pool as sp
import tushare as ts
import pandas as pd
from models import conn, industry_codes, engine
import numpy as np
import talib as ta
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
from matplotlib.dates import date2num
from mpl_finance import candlestick_ohlc as candlestick
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

def show_growth_thru_code(code, year, quarter, n, columns=['nprg', 'mbrg'],step = 2):
    '''
    展示公司增长能力的图标
    code,代码
    name,名称
    mbrg,主营业务收入增长率(%)
    nprg,净利润增长率(%)
    nav,净资产增长率
    targ,总资产增长率
    epsg,每股收益增长率
    seg,股东权益增长率
    '''
    code_data = sp.get_code_data_from_industry_codes(code)
    if code_data is None:
        print("\n 在industry codes中找不到该code%s"%(code))
        return False
    can_do = False
    if sp.has_growth_info_for_n_quarters(code, year, quarter, n + step):
        can_do = True
        df = sp.get_growth_data_df_thru_code(code, year, quarter, n + step)
    else:
        for i in range(3):
            year, quarter = sp.get_last_quarter(year, quarter)
            if sp.has_growth_info_for_n_quarters(code, year, quarter, n + step):
                can_do = True
                break
        if can_do:
            df = sp.get_growth_data_df_thru_code(code, year, quarter, n + step)
    if can_do:
        if columns == 'all':
            df = df[['mbrg', 'nprg', 'nav', 'targ', 'epsg', 'seg']]
        else:
            df = df[columns]
        print("\n %s 如图：\n"%(code_data['name']))
        ax = df.plot(title="%s growth chart"%(code_data['code']))
        ax.set_xlabel("Date")
        ax.set_ylabel("Percentage")
        return True
    else:
        print("\n股票 %s 没有足够相关信息来展示!\n")
        return False


def show_macd_thru_code(code, days=100):
    '''
    展示macd图
    '''
    df = ts.get_hist_data(code)
    # 转化str类型的index为datetime类型的index
    df.index = pd.to_datetime(df.index)
    df = sp.add_macd(df)
    df = df.sort_index(ascending=False).iloc[:days]
    f2, ax2 = plt.subplots(figsize = (8,4))
    ax2.plot(df.index, df['macd'], color='green', lw=1,label='MACD Line(26,12)')
    ax2.plot(df.index, df['macdSignal'], color='purple', lw=1, label='Signal Line(9)')
    ax2.fill_between(df.index, df['macdHist'], color = 'gray', alpha=0.5, label='MACD Histogram')
    ax2.set(title = 'MACD(26,12,9)', ylabel='MACD')
    ax2.legend(loc = 'upper right')
    ax2.grid(False)
    plt.show()
    return None


def show_tech_thru_code(code, days=100):
    '''
    展示蜡烛图
    展示RSI
    展示MACD
    https://wiki.fintechki.com/index.php?title=How_to_draw_MACD_Charts
    '''
    ticker = code
    df = ts.get_hist_data(code)
    sec_id = df.sort_index(ascending=True)
    sec_id.index = pd.to_datetime(sec_id.index)
    part_sec_id = sec_id.sort_index(ascending=False).iloc[0:days]
    # Data for matplotlib finance plot
    sec_id_ochl = np.array(pd.DataFrame({'0': date2num(part_sec_id.index),
                                         '1':part_sec_id.open,
                                         '2':part_sec_id.close,
                                         '3':part_sec_id.high,
                                         '4':part_sec_id.low}))

    analysis = pd.DataFrame(index = sec_id.index)

    analysis['sma_f'] = pd.Series.rolling(sec_id.close, SMA_FAST).mean()
    analysis['sma_s'] = pd.Series.rolling(sec_id.close, SMA_SLOW).mean()
    analysis['rsi'] = ta.RSI(sec_id.close.values, RSI_PERIOD)

    analysis['sma_r'] = pd.Series.rolling(analysis.rsi, RSI_AVG_PERIOD).mean()
    analysis['macd'], analysis['macdSignal'], analysis['macdHist'] = ta.MACD(sec_id.close.values, fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL)
    analysis['stoch_k'], analysis['stoch_d'] = ta.STOCH(sec_id.high.values, sec_id.low.values, sec_id.close.values, slowk_period=STOCH_K, slowd_period=STOCH_D)

    analysis['sma'] = np.where(analysis.sma_f > analysis.sma_s, 1, 0)
    analysis['macd_test'] = np.where((analysis.macd > analysis.macdSignal), 1, 0)
    analysis['stoch_k_test'] = np.where((analysis.stoch_k < 50) & (analysis.stoch_k > analysis.stoch_k.shift(1)), 1, 0)
    analysis['rsi_test'] = np.where((analysis.rsi < 50) & (analysis.rsi > analysis.rsi.shift(1)), 1, 0)

    analysis = analysis.sort_index(ascending=False)
    analysis = analysis.iloc[:100]


    # Prepare plot
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True)
    ax1.set_ylabel(ticker, size=20)

    #size plot
    fig.set_size_inches(15,30)

    # Plot candles
    candlestick(ax1, sec_id_ochl, width=0.5, colorup='g', colordown='r', alpha=1)

    # Draw Moving Averages
    analysis.sma_f.plot(ax=ax1, c='r')
    analysis.sma_s.plot(ax=ax1, c='g')

    #RSI
    ax2.set_ylabel('RSI', size=Y_AXIS_SIZE)
    analysis.rsi.plot(ax = ax2, c='g', label = 'Period: ' + str(RSI_PERIOD))
    analysis.sma_r.plot(ax = ax2, c='r', label = 'MA: ' + str(RSI_AVG_PERIOD))
    ax2.axhline(y=30, c='b')
    ax2.axhline(y=50, c='black')
    ax2.axhline(y=70, c='b')
    ax2.set_ylim([0,100])
    handles, labels = ax2.get_legend_handles_labels()
    ax2.legend(handles, labels)

    # Draw MACD computed with Talib
    ax3.set_ylabel('MACD: '+ str(MACD_FAST) + ', ' + str(MACD_SLOW) + ', ' + str(MACD_SIGNAL), size=Y_AXIS_SIZE)
    analysis.macd.plot(ax=ax3, color='b', label='Macd')
    analysis.macdSignal.plot(ax=ax3, color='g', label='Signal')
    ax3.fill_between(analysis.index, analysis['macdHist'], color = 'gray', alpha=0.5, label='Histogram')
    ax3.axhline(0, lw=2, color='0')
    handles, labels = ax3.get_legend_handles_labels()
    ax3.legend(handles, labels)

    # Stochastic plot
    ax4.set_ylabel('Stoch (k,d)', size=Y_AXIS_SIZE)
    analysis.stoch_k.plot(ax=ax4, label='stoch_k:'+ str(STOCH_K), color='r')
    analysis.stoch_d.plot(ax=ax4, label='stoch_d:'+ str(STOCH_D), color='g')
    handles, labels = ax4.get_legend_handles_labels()
    ax4.legend(handles, labels)
    ax4.axhline(y=20, c='b')
    ax4.axhline(y=50, c='black')
    ax4.axhline(y=80, c='b')

    plt.show()

    return None
