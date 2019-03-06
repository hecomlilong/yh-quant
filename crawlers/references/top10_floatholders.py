#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取上市公司前十大流通股东数据。
"""


class Top10FloatHolders(TuShareBase):
    def __init__(self):
        super(Top10FloatHolders, self).__init__()
        """
        初始化
        """

        # 创建top10_floatholders数据集
        self.conn = DB_CONN['top10_floatholders']

    def crawl(self, ts_code, period='', ann_date='', start_date='', end_date=''):
        """
        :param ts_code  str Y   TS代码
        :param period  str N   报告期
        :param ann_date    str N   公告日期
        :param start_date  str N   报告期开始日期
        :param end_date    str N   报告期结束日期
        注：一次取100行记录
        :return:
        ts_code str TS股票代码
        ann_date    str 公告日期
        end_date    str 报告期
        holder_name str 股东名称
        hold_amount float   持有数量（股）
        """
        if not ts_code:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('top10_floatholders', ts_code=ts_code, period=period, ann_date=ann_date,start_date=start_date,end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'ann_date', 'holder_name'])

    def crawl_all(self, start_date='', end_date=''):
        if start_date == '' and end_date == '':
            # 当前日期
            now = datetime.now()
            start_date = now.strftime('%Y%m%d')
            end_date = now.strftime('%Y%m%d')
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code, start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    stock = Top10FloatHolders()
    stock.crawl('20170105')
