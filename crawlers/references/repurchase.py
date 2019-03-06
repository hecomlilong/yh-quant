#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取上市公司回购股票数据
"""


class Repurchase(TuShareBase):
    def __init__(self):
        super(Repurchase, self).__init__()
        """
        初始化
        """

        # 创建repurchase数据集
        self.conn = DB_CONN['repurchase']

    def crawl(self, ann_date='', start_date='', end_date=''):
        """
        :param ann_date str N   公告日期（任意填参数，如果都不填，单次默认返回2000条）
        :param start_date  str N   公告开始日期
        :param end_date    str N   公告结束日期
        :return:
        ts_code str Y   TS代码
        ann_date    str Y   公告日期
        end_date    str Y   截止日期
        proc    str Y   进度
        exp_date    str Y   过期日期
        vol float   Y   回购数量
        amount  float   Y   回购金额
        high_limit  float   Y   回购最高价
        low_limit   float   Y   回购最低价
        """
        if not ts_code:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('repurchase', ann_date=ann_date,start_date=start_date,end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'ann_date'])

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
    stock = Repurchase()
    stock.crawl('20170105')
