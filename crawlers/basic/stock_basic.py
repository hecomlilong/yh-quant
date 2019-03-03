#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
"""


class StockBasic(TuShareBase):
    def __init__(self):
        super(StockBasic, self).__init__()
        """
        初始化
        """

        # 创建stock_basic数据集
        self.conn = DB_CONN['stock_basic']

    def crawl(self, is_hs='', exchange='', list_status='L'):
        """
        :param is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
        :param exchange: 交易所 SSE上交所 SZSE深交所 HKEX港交所
        :param list_status: 上市状态： L上市 D退市 P暂停上市
        :return:
        ts_code	str	TS代码
        symbol	str	股票代码
        name	str	股票名称
        area	str	所在地域
        industry	str	所属行业
        fullname	str	股票全称
        enname	str	英文全称
        market	str	市场类型 （主板/中小板/创业板）
        exchange	str	交易所代码
        curr_type	str	交易货币
        list_status	str	上市状态： L上市 D退市 P暂停上市
        list_date	str	上市日期
        delist_date	str	退市日期
        is_hs	str	是否沪深港通标的，N否 H沪股通 S深股通
        """
        fields = 'ts_code,symbol,name,area,industry,fullname,enname,market' \
                 ',exchange,curr_type,list_status,list_date,delist_date,is_hs'
        df = self.pro.query('stock_basic', is_hs=is_hs, exchange=exchange, list_status=list_status,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code'])


if __name__ == '__main__':
    stock = StockBasic()
    stock.crawl()
