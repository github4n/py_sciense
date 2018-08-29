# 在ipython中跑

import stock_pool as sp
import pandas as pd
import datetime
import talib as ta
import numpy as np

sp.produce_sepa_codes(3)

sp.basic_info_step1_for_sepa('2018-08-27_60_sepa_step1_codes', 2018, 2)

sp.basic_info_step2_for_sepa('2018-08-27_60_sepa_step1_codes', 2018, 2)

sp.basic_info_step_for_sepa('2018-08-28_80_sepa_step1_codes', 2018, 2)
