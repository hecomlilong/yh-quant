#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
从tushare获取股票基础数据，保存到本地的MongoDB数据库中
"""


class StockBasicCrawler(TuShareBase):
    def __init__(self):
        super(StockBasicCrawler, self).__init__()
        """
        初始化
        """

        # 创建stock_basic数据集
        self.stock_basic = DB_CONN['stock_basic']

    def crawl(self, is_hs='', exchange='', list_status='L'):
        fields = 'ts_code,symbol,name,area,industry,fullname,enname,market' \
                 ',exchange,curr_type,list_status,list_date,delist_date,is_hs'
        df = self.pro.query('stock_basic', is_hs=is_hs, exchange=exchange, list_status=list_status,
                            fields=fields)
        self.save_data(df=df, collection=self.stock_basic, filter_fields=['ts_code'])


if __name__ == '__main__':
    stock = StockBasicCrawler()
    stock.crawl()
