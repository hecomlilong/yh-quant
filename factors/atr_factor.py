#  -*- coding: utf-8 -*-


from utils.database import DB_CONN
from utils.stock_util import get_codes, get_daily_conn_name, get_begin_end_date
from pymongo import ASCENDING
from pandas import DataFrame
import traceback
from utils.tushare_base import TuShareBase

"""
ATR平均真实波幅
TR:真实波幅
1、当前交易日的最高价与最低价间的波幅
2、前一交易日收盘价与当个交易日最高价间的波幅
3、前一交易日收盘价与当个交易日最低价间的波幅
ATR:TR的N日移动平均    参数：N 天数，一般取14,10天、20天乃至65天都有

"""


class ATR:
    name = 'atr'

    def __init__(self):
        self.conn_name = get_daily_conn_name('qfq', 'E', 'D')

    def compute(self, ts_code='', begin_date='', end_date='', day_count=14):
        """
        计算给定周期内的ATR，把结果保存到数据库中
        :param ts_code: 股票代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        :param day_count: 天数
        """

        begin_date, end_date = get_begin_end_date(begin_date, end_date)

        codes = get_codes(ts_code)
        if day_count < 1:
            return
        n = day_count - 1

        # 计算每个股票的信号
        for code in codes:
            try:
                daily_cursor = DB_CONN[self.conn_name].find(
                    {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}},
                    sort=[('trade_date', ASCENDING)],
                    projection={'ts_code': True, 'trade_date': True, 'high': True, 'low': True, 'pre_close': True,
                                '_id': False}
                )

                df_daily = DataFrame([daily for daily in daily_cursor])

                # 当前交易日的最高价-最低价的绝对值
                df_daily['current_span'] = df_daily['high'] - df_daily['low']

                # 上一交易日收盘价-当前交易日最低价的绝对值
                df_daily['close_low'] = (df_daily['pre_close'] - df_daily['low']).abs()

                # 上一交易日收盘价-当前交易日最高价的绝对值
                df_daily['close_high'] = (df_daily['pre_close'] - df_daily['high']).abs()

                # 以上三个数的最大值即为TR
                df_daily['tr_%s' % day_count] = df_daily[['current_span', 'close_low', 'close_high']].max(axis=1)

                # 计算ATR
                target = []
                for i in df_daily.index:
                    target.append(0.0)
                    if i >= n:
                        target[i] = df_daily.iloc[i - n:i + 1]['tr_%s' % day_count].sum() / day_count

                df_daily['atr_%s' % day_count] = target
                # 抛掉不用的数据
                df_daily.drop(['current_span', 'close_low', 'close_high'], 1, inplace=True)

                # 将信号保存到数据库
                ts_base = TuShareBase()
                ts_base.save_data(df=df_daily, collection=DB_CONN[self.name],
                                  filter_fields=['ts_code', 'trade_date'])
            except:
                print('错误发生： %s' % code, flush=True)
                traceback.print_exc()


if __name__ == '__main__':
    atr = ATR()
    atr.compute(ts_code='000001.SZ', begin_date='20190101', end_date='20190308')
