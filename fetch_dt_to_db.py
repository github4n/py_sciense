import sqlalchemy as sa
from sqlalchemy import create_engine
import tushare as ts
from datetime import datetime
import data_pool as dp
import time
import backtest
from fetch_data import FetchData


engine = create_engine('mysql+pymysql://root:1234@127.0.0.1/best_stocks')

df = ts.get_hist_data('000861')
df.to_sql('hist_data', engine, if_exists='append', dtype={'date': sa.VARCHAR(255)})


# ---------- 按行业分类
industry_codes = ts.get_industry_classified()
industry_codes.to_sql('industry_codes', engine)

# ---------- 按概念分类
concept_codes = ts.get_concept_classified()
concept_codes.to_sql('concept_codes', engine, if_exists='replace', dtype={'date': sa.VARCHAR(255)})

# ---------- 上证50
sz_50 = ts.get_sz50s()
sz_50.to_sql('sz_50_codes', engine, if_exists='replace', dtype={'date': sa.VARCHAR(255)})

# ---------- 中证50
zz_500 = ts.get_zz500s()
zz_500.to_sql('zz_500_codes', engine, if_exists='replace', dtype={'date': sa.VARCHAR(255)})

# ---------- 抓交易日期到股票池
dp.fetch_trade_cal_to_db()

# ---------- 抓股票池子
dp.fetch_stock_basic_to_db()

# ---------- 抓近期的daily数据
end_date = '20180915'
codes_df = dp.stock_basic_df()
for index, row in codes_df.iterrows():
    dp.fetch_daily_data_to_db(row['ts_code'], end_date=end_date)

# ---------- 抓取股票的增长数据
date = datetime.now()
for index, row in codes_df.iterrows():
    dp.get_growth_data_df(row['symbol'], date)

# ---------- 计算每天的rps
c_rps = backtest.ComputeRps(start_date='2014-01-01', end_date='2018-09-01')
c_rps.run()

# ---------- 计算每天的sepa列表
sepa = backtest.ComputeSepa(start_date='2014-01-01', end_date='2018-09-01')
sepa.run()

# ---------- 每天跑的更新数据
fetch_data = FetchData()
fetch_data.run()

year = 2012
quarter = 1
for i in range(10):
    if i != 0:
        year, quarter = dp.get_last_quarter(year, quarter)
    dp.get_growth_data(year, quarter)
