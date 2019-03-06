#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取股权质押统计数据
限量：单次最大1000
"""


class PledgeStat(TuShareBase):
    def __init__(self):
        super(PledgeStat, self).__init__()
        """
        初始化
        """

        # 创建pledge_stat数据集
        self.conn = DB_CONN['pledge_stat']

    def crawl(self, ts_code):
        """
        :param ts_code  str Y   TS代码
        :return:
        ts_code str Y   TS代码
        end_date    str Y   截至日期
        pledge_count    int Y   质押次数
        unrest_pledge   float   Y   无限售股质押数量（万）
        rest_pledge float   Y   限售股份质押数量（万）
        total_share float   Y   总股本
        pledge_ratio    float   Y   质押比例
        """
        if not ts_code:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('pledge_stat', ts_code=ts_code)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'end_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = PledgeStat()
    stock.crawl('20170105')
