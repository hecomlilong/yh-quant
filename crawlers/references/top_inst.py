#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：龙虎榜机构成交明细
限量：单次最大10000
"""


class TopInst(TuShareBase):
    def __init__(self):
        super(TopInst, self).__init__()
        """
        初始化
        """

        # 创建top_inst数据集
        self.conn = DB_CONN['top_inst']

    def crawl(self, trade_date, ts_code=''):
        """
        :param trade_date   str Y   交易日期
        :param ts_code str N   股票代码
        :return:
        trade_date  str Y   交易日期
        ts_code str Y   TS代码
        exalter str Y   营业部名称
        buy float   Y   买入额（万）
        buy_rate    float   Y   买入占总成交比例
        sell    float   Y   卖出额（万）
        sell_rate   float   Y   卖出占总成交比例
        net_buy float   Y   净成交额（万）
        """
        if not trade_date:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('top_inst', ts_code=ts_code, trade_date=trade_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

    def crawl_all(self, trade_date=''):
        if trade_date == '':
            # 当前日期
            now = datetime.now()
            trade_date = now.strftime('%Y%m%d')
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code, trade_date=trade_date)


if __name__ == '__main__':
    stock = TopInst()
    stock.crawl('20170105')
