#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro,get_all_concept
from datetime import datetime

"""
描述：获取概念股分类明细数据
"""


class ConceptDetail(TuShareBase):
    def __init__(self):
        super(ConceptDetail, self).__init__()
        """
        初始化
        """

        # 创建concept_detail数据集
        self.conn = DB_CONN['concept_detail']

    def crawl(self, id):
        """
        :param id   str Y   概念分类ID （id来自概念股分类接口）
        :return:
        id  str Y   概念代码
        ts_code str Y   股票代码
        name    str Y   股票名称
        in_date str N   纳入日期
        out_date    str N   剔除日期
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        if not id:
            return
        df = self.pro.query('concept_detail', id=id)
        self.save_data(df=df, collection=self.conn, filter_fields=['id', 'ts_code','in_date','out_date'])

    def crawl_all(self):
        concepts = get_all_concept()
        for id in concepts:
            self.crawl(id=id)


if __name__ == '__main__':
    stock = ConceptDetail()
    stock.crawl()
