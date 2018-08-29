import matplotlib.pyplot as plt
import stock_pool as sp
import tushare as ts
import pandas as pd
from models import conn, industry_codes, engine

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
