#  -*- coding: utf-8 -*-


from utils.database import DB_CONN
from utils.stock_util import get_all_codes_pro, get_daily_conn_name
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback
from utils.tushare_base import TuShareBase


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
            daily_cursor = DB_CONN[get_daily_conn_name('qfq', 'E')].find(
                {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}, 'amount': {'$gte': 0},
                 'index': False},
                sort=[('trade_date', ASCENDING)],
                projection={'trade_date': True, 'high': True, 'low': True, 'pre_close': True, '_id': False}
            )

            df_daily = DataFrame([daily for daily in daily_cursor])

            # 当前交易日的最高价-最低价的绝对值
            df_daily['current_span'] = df_daily['high'] - df_daily['low']

            # 上一交易日收盘价-当前交易日最低价的绝对值
            df_daily['close_low'] = (df_daily['pre_close'] - df_daily['low']).abs()

            # 上一交易日收盘价-当前交易日最高价的绝对值
            df_daily['close_high'] = (df_daily['pre_close'] - df_daily['high']).abs()

            # 以上三个数的最大值即为ATR
            df_daily['atr'] = df_daily[['current_span', 'close_low', 'close_high']].max(axis=1)

            # 抛掉不用的数据
            df_daily.drop(['current_span', 'close_low', 'close_high'], 1, inplace=True)

            print(df_daily)
            # 将信号保存到数据库
            ts_base = TuShareBase()
            ts_base.save_data(df=df_daily, collection=DB_CONN[get_daily_conn_name('qfq', 'E')],
                              filter_fields=['ts_code', 'trade_date'])
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()


if __name__ == '__main__':
    compute_atr('2015-01-01', '2015-12-31')
