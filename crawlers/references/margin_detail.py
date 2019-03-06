#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取沪深两市每日融资融券明细
"""


class MarginDetail(TuShareBase):
    def __init__(self):
        super(MarginDetail, self).__init__()
        """
        初始化
        """

        # 创建margin_detail数据集
        self.conn = DB_CONN['margin_detail']

    def crawl(self, trade_date, ts_code=''):
        """
        :param trade_date   str Y   交易日期
        :param ts_code  str N   TS代码
        :return:
        trade_date  str 交易日期
        ts_code str TS股票代码
        rzye    float   融资余额(元)
        rqye    float   融券余额(元)
        rzmre   float   融资买入额(元)
        rqyl    float   融券余量（手）
        rzche   float   融资偿还额(元)
        rqchl   float   融券偿还量(手)
        rqmcl   float   融券卖出量(股,份,手)
        rzrqye  float   融资融券余额(元)
        """
        if not trade_date:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('margin_detail', trade_date=trade_date, ts_code=ts_code)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

    def crawl_all(self, trade_date):
        if not trade_date:
            return
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(trade_date=trade_date, ts_code=code)


if __name__ == '__main__':
    stock = MarginDetail()
    stock.crawl('20170105')
