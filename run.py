# 在ipython中跑,
# %matplotlib qt
# from importlib import reload
# 使用示例
import stock_pool as sp
import pandas as pd
import datetime
import talib as ta
import numpy as np
code = '000681'
year = 2018
quarter = 2

sp.growth_bigger_than_last_for_n_quarters(code, year, quarter, 3, 'nprg')
sp.growth_bigger_than_last_for_n_quarters(code, year, quarter, 3, 'mbrg')
sp.has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'nprg')
sp.has_growth_than_x_for_n_quarters(code, year, quarter, 3, 20, 'mbrg')

df = sp.get_profit_data_df_thru_code(code, year, quarter, 3)
g_df = sp.get_growth_data_df_thru_code(code, year, quarter, 3)

narr = np.array([1.0,2.0,3.0,4.0,5.0,6.0,7.0])
rr = ta.SMA(narr, timeperiod=1)

sp.growth_bigger_than_last_for_n_quarters_thru_sma(code, year, quarter, 3, 'mbrg')

g_df = sp.get_growth_data_df_thru_code(code, year, quarter, 3)
g_df = sp.add_sma(g_df, 'mbrg', 2)


df = pd.read_sql('select * from `2018-08-27_60_sepa_step1_codes`', con=conn)

df = pd.read_sql('select * from `2018-08-27_60_sepa_step1_codes`', con=conn, index_col='code_index')


df = sp.load_codes_from_db('2018-08-27_60_sepa_step1_codes')

for index, row in df.iterrows():
   print(row['name'])


code = '603939'


r = sp.has_basic_info_for_n_quarters(code, year, quarter, 5) and sp.has_growth_info_for_n_quarters(code, year, quarter, 5)


import matplotlib.pyplot as plt

def get_indicator(df, indicator):
        ret_df = df
        if 'MACD' in indicator:
            macd, macdsignal, macdhist = ta.MACD(df.close.values, fastperiod=12, slowperiod=26, signalperiod=9)
            ret_df = KlineData._merge_dataframe(pd.DataFrame([macd, macdsignal, macdhist]).T.rename(columns={0: "macddif", 1: "macddem", 2: "macdhist"}), ret_df)
            ret_df = KlineData._merge_dataframe(line_intersections(ret_df, columns=['macddif', 'macddem']), ret_df)
        if 'MFI' in indicator:
            real = ta.MFI(df.high.values, df.low.values, df.close.values, df.volume.values, timeperiod=14)
            ret_df = KlineData._merge_dataframe(pd.DataFrame([real]).T.rename(columns={0: "mfi"}), ret_df)
        if 'ATR' in indicator:
            real = ta.NATR(df.high.values, df.low.values, df.close.values, timeperiod=14)
            ret_df = KlineData._merge_dataframe(pd.DataFrame([real]).T.rename(columns={0: "atr"}), ret_df)
        if 'ROCR' in indicator:
            real = ta.ROCR(df.close.values, timeperiod=10)
            ret_df = KlineData._merge_dataframe(pd.DataFrame([real]).T.rename(columns={0: "rocr"}), ret_df)
        ret_df['date'] = pd.to_datetime(ret_df['date'], format='%Y-%m-%d')
        return ret_df


analysis = pd.DataFrame(index = df.index)

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL =9

analysis['macd'], analysis['macdSignal'], analysis['macdHist'] = ta.MACD(df.close.as_matrix(), fastperiod=MACD_FAST, slowperiod=MACD_SLOW, signalperiod=MACD_SIGNAL)



fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True)
ax1.set_ylabel(ticker, size=20)

#size plot
fig.set_size_inches(15,30)


analysis.macd.plot(ax=ax3, color='b', label='Macd')
analysis.macdSignal.plot(ax=ax3, color='g', label='Signal')
analysis.macdHist.plot(ax=ax3, color='r', label='Hist')



# from pandas_datareader import data
import tushare as ts
import pandas as pd
import numpy as np
import talib as ta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
from matplotlib.dates import date2num
from mpl_finance import candlestick_ohlc as candlestick
import datetime

days = 100

ticker = 'OPK'

# Download sample data
code = '000661'
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

# Technical Analysis
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

analysis = pd.DataFrame(index = sec_id.index)

# analysis['sma_f'] = pd.rolling_mean(sec_id.Close, SMA_FAST)
analysis['sma_f'] = pd.Series.rolling(sec_id.close, SMA_FAST).mean()
analysis['sma_s'] = pd.Series.rolling(sec_id.close, SMA_SLOW).mean()
analysis['rsi'] = ta.RSI(sec_id.close.values, RSI_PERIOD)
# analysis['sma_r'] = pd.rolling_mean(analysis.rsi, RSI_AVG_PERIOD) # check shift
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
# analysis.macdHist.plot(ax=ax3, color='r', label='Hist')
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
