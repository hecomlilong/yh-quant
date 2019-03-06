#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取上市公司股东户数数据，数据不定期公布
限量：单次最大3000,总量不限制
"""


class STKHolderNumber(TuShareBase):
    def __init__(self):
        super(STKHolderNumber, self).__init__()
        """
        初始化
        """

        # 创建stk_holdernumber数据集
        self.conn = DB_CONN['stk_holdernumber']

    def crawl(self, ts_code, end_date='', start_date='', end_date=''):
        """
        :param ts_code  str Y   TS代码
        :param enddate  str N   截止日期
        :param start_date  str N   开始日期
        :param end_date    str N   结束日期
        :return:
        ts_code str Y   TS股票代码
        ann_date    str Y   公告日期
        end_date    str Y   截止日期
        holder_num  int Y   股东户数
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('stk_holdernumber', ts_code=ts_code, end_date=end_date,start_date=start_date,end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'ann_date', 'end_date'])

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
    stock = STKHolderNumber()
    stock.crawl('20170105')
