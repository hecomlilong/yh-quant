#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取融资融券每日交易汇总数据
"""


class Margin(TuShareBase):
    def __init__(self):
        super(Margin, self).__init__()
        """
        初始化
        """

        # 创建margin数据集
        self.conn = DB_CONN['margin']

    def crawl(self, trade_date, exchange_id=''):
        """
        :param trade_date   str Y   交易日期
        :param exchange_id str N   交易所代码
        :return:
        trade_date  str 交易日期
        exchange_id str 交易所代码（SSE上交所SZSE深交所）
        rzye    float   融资余额(元)
        rzmre   float   融资买入额(元)
        rzche   float   融资偿还额(元)
        rqye    float   融券余额(元)
        rqmcl   float   融券卖出量(股,份,手)
        rzrqye  float   融资融券余额(元)
        """
        if not trade_date:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('margin', exchange_id=exchange_id, trade_date=trade_date, market_type=market_type)
        self.save_data(df=df, collection=self.conn, filter_fields=['exchange_id', 'trade_date'])

    def crawl_all(self, trade_date):
        if not trade_date:
            return

        for exchange_id in ['SSE','SZSE']:
            self.crawl(trade_date=trade_date, exchange_id=exchange_id)


if __name__ == '__main__':
    stock = Margin()
    stock.crawl('20170105')
