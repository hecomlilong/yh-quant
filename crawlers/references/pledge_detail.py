#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取股权质押明细数据
限量：单次最大1000
"""


class PledgeStat(TuShareBase):
    def __init__(self):
        super(PledgeStat, self).__init__()
        """
        初始化
        """

        # 创建pledge_detail数据集
        self.conn = DB_CONN['pledge_detail']

    def crawl(self, ts_code):
        """
        :param ts_code  str Y   TS代码
        :return:
        ts_code str Y   TS股票代码
        ann_date    str Y   公告日期
        holder_name str Y   股东名称
        pledge_amount   float   Y   质押数量
        start_date  str Y   质押开始日期
        end_date    str Y   质押结束日期
        is_release  str Y   是否已解押
        release_date    str Y   解押日期
        pledgor str Y   质押方
        holding_amount  float   Y   持股总数
        pledged_amount  float   Y   质押总数
        p_total_ratio   float   Y   本次质押占总股本比例
        h_total_ratio   float   Y   持股总数占总股本比例
        is_buyback  str Y   是否回购
        """
        if not ts_code:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('pledge_detail', ts_code=ts_code)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'ann_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = PledgeStat()
    stock.crawl('20170105')
