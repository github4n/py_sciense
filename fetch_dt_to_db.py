import sqlalchemy as sa
from sqlalchemy import create_engine
import tushare as ts
from datetime import datetime
import data_pool as dp
import time


engine = create_engine('mysql+pymysql://wpzero:1234@127.0.0.1/best_stocks')

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



date = datetime.strptime('2014-01-01', '%Y-%m-%d')
all_codes_df = dp.get_all_codes_df()
for index, row in all_codes_df.iterrows():
    time.sleep(60 * 5)
    dp.get_h_data(row['code'], date)
