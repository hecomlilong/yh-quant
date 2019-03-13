#  -*- coding: utf-8 -*-

import traceback

from pandas import DataFrame
from pymongo import ASCENDING

from utils.database import DB_CONN
from utils.stock_util import get_codes, get_begin_end_date, get_daily_conn_name
from utils.tushare_base import TuShareBase


class Boll:
    name = 'boll'

    def __init__(self):
        self.conn_name = get_daily_conn_name('qfq', 'E', 'D')

    def compute(self, ts_code='', begin_date='', end_date=''):
        """
        计算指定日期内的Boll突破上轨和突破下轨信号，并保存到数据库中，
        方便查询使用
        :param ts_code: 股票代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        # 获取所有股票代码
        codes = get_codes(ts_code)

        begin_date, end_date = get_begin_end_date(begin_date, end_date)

        # 计算每一只股票的Boll信号
        for code in codes:
            try:
                # 获取后复权的价格，使用后复权的价格计算BOLL
                daily_cursor = DB_CONN[self.conn_name].find(
                    {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}},
                    sort=[('trade_date', ASCENDING)],
                    projection={'trade_date': True, 'close': True, '_id': False}
                )

                df_daily = DataFrame([daily for daily in daily_cursor])

                if len(df_daily) == 0:
                    continue

                # 计算MB，盘后计算，这里用当日的Close
                df_daily['MB'] = df_daily['close'].rolling(20).mean()
                # 计算STD20，计算20日的标准差
                df_daily['std'] = df_daily['close'].rolling(20).std()

                print(df_daily, flush=True)
                # 计算UP，上轨
                df_daily['UP'] = df_daily['MB'] + 2 * df_daily['std']
                # 计算down，下轨
                df_daily['DOWN'] = df_daily['MB'] - 2 * df_daily['std']

                print(df_daily, flush=True)

                # 将日期作为索引
                df_daily.set_index(['trade_date'], inplace=True)

                # 将close移动一个位置，变为当前索引位置的前收
                last_close = df_daily['close'].shift(1)

                # 将上轨移一位，前一日的上轨和前一日的收盘价都在当日了
                shifted_up = df_daily['UP'].shift(1)
                # 突破上轨，是向上突破，条件是前一日收盘价小于前一日上轨，当日收盘价大于前一日上轨。
                df_daily['up_mask'] = (last_close <= shifted_up) & (df_daily['close'] > shifted_up)

                # 将下轨移一位，前一日的下轨和前一日的收盘价都在当日了
                shifted_down = df_daily['DOWN'].shift(1)
                # 突破下轨，是向下突破，条件是前一日收盘价大于前一日下轨，当日收盘价小于前一日下轨
                df_daily['down_mask'] = (last_close >= shifted_down) & (df_daily['close'] < shifted_down)

                # 对结果进行过滤，只保留向上突破或者向上突破的数据
                df_daily = df_daily[df_daily['up_mask'] | df_daily['down_mask']]
                # 从DataFrame中扔掉不用的数据
                df_daily.drop(['close', 'std', 'MB', 'UP', 'DOWN'], 1, inplace=True)

                # 将信号保存到数据库
                docs = []
                # DataFrame的索引是日期
                for date in df_daily.index:
                    # 保存的数据包括股票代码、日期和信号类型，结合数据集的名字，就表示某只股票在某日
                    doc = {
                        'ts_code': code,
                        'trade_date': date,
                        # 方向，向上突破 up，向下突破 down
                        'direction': 'up' if df_daily.loc[date]['up_mask'] else 'down'
                    }
                    docs.append(doc)
                # 将信号保存到数据库
                ts_base = TuShareBase()
                ts_base.save_data(docs=docs, collection=DB_CONN[self.name],
                                  filter_fields=['ts_code', 'trade_date'])
            except:
                traceback.print_exc()

    def is_boll_break_up(self, code, date):
        """
        查询某只股票是否在某日出现了突破上轨信号
        :param code: 股票代码
        :param date: 日期
        :return: True - 出现了突破上轨信号，False - 没有出现突破上轨信号
        """
        count = DB_CONN[self.name].count({'ts_code': code, 'trade_date': date, 'direction': 'up'})
        return count == 1

    def is_boll_break_down(self, code, date):
        """
        查询某只股票是否在某日出现了突破下轨信号
        :param code: 股票代码
        :param date: 日期
        :return: True - 出现了突破下轨信号，False - 没有出现突破下轨信号
        """
        count = DB_CONN[self.name].count({'ts_code': code, 'trade_date': date, 'direction': 'down'})
        return count == 1


if __name__ == '__main__':
    # 计算指定时间内的boll信号
    boll = Boll()
    boll.compute(ts_code='000001.SZ', begin_date='2019-01-01', end_date='2019-12-31')
