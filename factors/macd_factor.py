#  -*- coding: utf-8 -*-


from utils.database import DB_CONN
from utils.stock_util import get_codes, get_begin_end_date, get_daily_conn_name
from utils.tushare_base import TuShareBase
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback


class MACD(object):
    name = 'macd'

    def __init__(self):
        self.conn_name = get_daily_conn_name('qfq', 'E', 'D')

    def compute(self, ts_code='', begin_date='', end_date=''):
        """
        计算给定周期内的MACD金叉和死叉信号，把结果保存到数据库中
        :param ts_code: 股票代码
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        """
        下面几个参数是计算MACD时的产生，这几个参数的取值都是常用值
        也可以根据需要调整
        """
        # 短时
        short = 12
        # 长时
        long = 26
        # 计算DIFF的M值
        m = 9

        # 获取所有股票代码
        codes = get_codes(ts_code)

        begin_date, end_date = get_begin_end_date(begin_date, end_date)

        # 循环检测所有股票的MACD金叉和死叉信号
        for code in codes:
            try:
                # 获取后复权的价格，使用后复权的价格计算MACD
                daily_cursor = DB_CONN[self.conn_name].find(
                    {'ts_code': code, 'trade_date': {'$gte': begin_date, '$lte': end_date}},
                    sort=[('trade_date', ASCENDING)],
                    projection={'trade_date': True, 'close': True, '_id': False}
                )

                # 将数据存为DataFrame格式
                df_daily = DataFrame([daily for daily in daily_cursor])
                # 设置date作为索引
                df_daily.set_index(['trade_date'], 1, inplace=True)

                # 计算EMA
                # alpha = 2/(N+1)
                # EMA(i) = (1 - alpha) * EMA(i-1) + alpha * CLOSE(i)
                #        = alpha * (CLOSE(i) - EMA(i-1)) + EMA(i-1)
                index = 0
                # 短时EMA列表
                EMA1 = []
                # 长时EMA列表
                EMA2 = []
                # 每天计算短时EMA和长时EMA
                for date in df_daily.index:
                    # 第一天EMA就是当日的close，也就是收盘价
                    if index == 0:
                        # 初始化短时EMA和长时EMA
                        EMA1.append(df_daily.loc[date]['close'])
                        EMA2.append(df_daily.loc[date]['close'])
                    else:
                        # 短时EMA和长时EMA
                        EMA1.append(2 / (short + 1) * (df_daily.loc[date]['close'] - EMA1[index - 1]) + EMA1[index - 1])
                        EMA2.append(2 / (long + 1) * (df_daily.loc[date]['close'] - EMA2[index - 1]) + EMA2[index - 1])

                    index += 1

                # 将短时EMA和长时EMA作为DataFrame的数据列
                df_daily['EMA1'] = EMA1
                df_daily['EMA2'] = EMA2

                # 计算DIFF，短时EMA - 长时EMA
                df_daily['DIFF'] = df_daily['EMA1'] - df_daily['EMA2']

                # 计算DEA，DIFF的EMA，计算公式是： EMA(DIFF，M)
                index = 0
                DEA = []
                for date in df_daily.index:
                    if index == 0:
                        DEA.append(df_daily.loc[date]['DIFF'])
                    else:
                        # M = 9 DEA = EMA(DIFF, 9)
                        DEA.append(2 / (m + 1) * (df_daily.loc[date]['DIFF'] - DEA[index - 1]) + DEA[index - 1])
                    index += 1

                df_daily['DEA'] = DEA

                # 计算DIFF和DEA的差值
                df_daily['delta'] = df_daily['DIFF'] - df_daily['DEA']
                # 将delta的移一位，那么前一天delta就变成了今天的pre_delta
                df_daily['pre_delta'] = df_daily['delta'].shift(1)
                # 金叉，DIFF上穿DEA，前一日DIFF在DEA下面，当日DIFF在DEA上面
                df_daily_gold = df_daily[(df_daily['pre_delta'] <= 0) & (df_daily['delta'] > 0)]
                # 死叉，DIFF下穿DEA，前一日DIFF在DEA上面，当日DIFF在DEA下面
                df_daily_dead = df_daily[(df_daily['pre_delta'] >= 0) & (df_daily['delta'] < 0)]

                # 保存结果到数据库
                docs = []
                for date in df_daily_gold.index:
                    # 保存时以code和date为查询条件，做更新或者新建，所以对code和date建立索引
                    # 通过signal字段表示金叉还是死叉，gold表示金叉
                    docs.append({'ts_code': code, 'trade_date': date, 'signal': 'gold'})

                for date in df_daily_dead.index:
                    # 保存时以code和date为查询条件，做更新或者新建，所以对code和date建立索引
                    # 通过signal字段表示金叉还是死叉，dead表示死叉
                    docs.append({'ts_code': code, 'trade_date': date, 'signal': 'dead'})

                # 将信号保存到数据库
                ts_base = TuShareBase()
                ts_base.save_data(docs=docs, collection=DB_CONN[self.name],
                                  filter_fields=['ts_code', 'trade_date'])
            except:
                print('错误发生： %s' % code, flush=True)
                traceback.print_exc()

    def is_macd_gold(self, code, date):
        """
        判断某只股票在某个交易日是否出现MACD金叉信号
        :param code: 股票代码
        :param date: 日期
        :return: True - 有金叉信号，False - 无金叉信号
        """
        count = DB_CONN[self.name].count({'ts_code': code, 'trade_date': date, 'signal': 'gold'})
        return count == 1

    def is_macd_dead(self, code, date):
        """
        判断某只股票在某个交易日是否出现MACD死叉信号
        :param code: 股票代码
        :param date: 日期
        :return: True - 有死叉信号，False - 无死叉信号
        """
        count = DB_CONN[self.name].count({'ts_code': code, 'trade_date': date, 'signal': 'dead'})
        return count == 1


if __name__ == '__main__':
    macd = MACD()
    macd.compute('000001.SZ', '2019-01-01', '2019-12-31')
