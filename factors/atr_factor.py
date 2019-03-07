#  -*- coding: utf-8 -*-


from utils.database import DB_CONN
from utils.stock_util import get_all_codes_pro,get_daily_conn_name
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback


def compute_atr(begin_date, end_date):
    """
    计算给定周期内的ATR，把结果保存到数据库中
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    # 获取所有股票代码
    codes = get_all_codes_pro()

    # 计算每个股票的信号
    for code in codes:
        try:
            daily_cursor = DB_CONN[get_daily_conn_name('qfq','E')].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'high': True, 'low': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])
            df_daily_shift_1 = df_daily.shift(1)
            # 当前交易日的最高价-最低价的绝对值
            df_daily['current_span'] = df_daily_shift_1['high'] - df_daily_shift_1['low']

            # 上一交易日收盘价-当前交易日最低价的绝对值

            # 上一交易日收盘价-当前交易日最高价的绝对值

            # 以上三个数的最大值即为ATR

            # 设置日期作为索引
            df_daily.set_index(['date'], 1, inplace=True)
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()
