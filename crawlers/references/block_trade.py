#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：大宗交易
限量：单次最大1000条，总量不限制
"""


class BlockTrade(TuShareBase):
    def __init__(self):
        super(BlockTrade, self).__init__()
        """
        初始化
        """

        # 创建block_trade数据集
        self.conn = DB_CONN['block_trade']

    def crawl(self, ts_code='',trade_date='',start_date='',end_date=''):
        """
        :param ts_code  str N   TS代码（股票代码和日期至少输入一个参数）
        :param trade_date  str N   交易日期（格式：YYYYMMDD，下同）
        :param start_date  str N   开始日期
        :param end_date    str N   结束日期
        :return:
        ts_code str Y   TS代码
        trade_date  str Y   交易日历
        price   float   Y   成交价
        vol float   Y   成交量（万股）
        amount  float   Y   成交金额
        buyer   str Y   买方营业部
        seller  str Y   卖房营业部
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        if not ts_code and not trade_date:
            return
        df = self.pro.query('block_trade', ts_code=ts_code, trade_date=trade_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['id', 'ts_code','trade_date','price','vol'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = BlockTrade()
    stock.crawl()
