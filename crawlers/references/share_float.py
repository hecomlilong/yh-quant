#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取限售股解禁
限量：单次最大5000条，总量不限制
"""


class ShareFloat(TuShareBase):
    def __init__(self):
        super(ShareFloat, self).__init__()
        """
        初始化
        """

        # 创建share_float数据集
        self.conn = DB_CONN['share_float']

    def crawl(self, ts_code='',ann_date='',float_date='',start_date='',end_date=''):
        """
        :param ts_code  str N   TS股票代码（至少输入一个参数）
        :param ann_date    str N   公告日期（日期格式：YYYYMMDD，下同）
        :param float_date  str N   解禁日期
        :param start_date  str N   解禁开始日期
        :param end_date    str N   解禁结束日期
        :return:
        ts_code str Y   TS代码
        ann_date    str Y   公告日期
        float_date  str Y   解禁日期
        float_share float   Y   流通股份
        float_ratio float   Y   流通股份占总股本比率
        holder_name str Y   股东名称
        share_type  str Y   股份类型
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        if not ts_code and not ann_date and not float_date and not start_date and not end_date:
            return
        df = self.pro.query('share_float', ts_code=ts_code, ann_date=ann_date, float_date=float_date, start_date=start_date, end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['id', 'ts_code','ann_date','float_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = ShareFloat()
    stock.crawl()
