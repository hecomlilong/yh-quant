#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
获取各大交易所交易日历数据,默认提取的是上交所
"""


class TradeCal(TuShareBase):
    def __init__(self):
        super(TradeCal, self).__init__()
        """
        初始化
        """

        # 创建trade_cal数据集
        self.conn = DB_CONN['trade_cal']

    def crawl(self, exchange='', start_date='', end_date='', is_open=''):
        """
        :param exchange: 交易所 SSE上交所 SZSE深交所
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param is_open: 是否交易 0休市 1交易
        :return:
exchange	str	Y	交易所 SSE上交所 SZSE深交所
cal_date	str	Y	日历日期
is_open	int	Y	是否交易 0休市 1交易
pretrade_date	str	N	上一个交易日
        """
        fields = 'exchange,cal_date,is_open,pretrade_date'
        df = self.pro.query('trade_cal', exchange=exchange, start_date=start_date, end_date=end_date, is_open=is_open,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['cal_date'])


if __name__ == '__main__':
    stock = TradeCal()
    stock.crawl()
