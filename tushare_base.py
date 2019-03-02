#  -*- coding: utf-8 -*-

from pymongo import UpdateOne
from database import DB_CONN
import tushare as ts


class TuShareBase:
    def __init__(self):
        """
        初始化
        """

        # 使用tushare pro
        self.pro = ts.pro_api('427882f17bbd866b6513afab0e4e00238219700c027e2b75a1c22123')
        # # 创建daily数据集
        # self.daily = DB_CONN['daily']
        # # 创建daily_hfq数据集
        # self.daily_hfq = DB_CONN['daily_hfq']

    def test(self):
        df = self.pro.query('trade_cal', exchange='', start_date='20180901', end_date='20181001',
                            fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
        print(type(df))
        print(df)
        # 抓取程序的入口函数


if __name__ == '__main__':
    dc = TuShareBase()
    dc.test()
