#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取概念股分类，目前只有ts一个来源，未来将逐步增加来源
"""


class Concept(TuShareBase):
    def __init__(self):
        super(Concept, self).__init__()
        """
        初始化
        """

        # 创建concept数据集
        self.conn = DB_CONN['concept']

    def crawl(self, src=''):
        """
        :param src  str N   来源，默认为ts
        :return:
        code    str Y   概念分类ID
        name    str Y   概念分类名称
        src str Y   来源
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('concept', src=src)
        self.save_data(df=df, collection=self.conn, filter_fields=['code', 'name'])

    def crawl_all(self):
        self.crawl(src='ts')


if __name__ == '__main__':
    stock = Concept()
    stock.crawl()
