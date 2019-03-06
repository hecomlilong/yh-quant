#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取各类指数成分和权重，月度数据 ，如需日度指数成分和权重，请联系 waditu@163.com
来源：指数公司网站公开数据
"""


class IndexWeight(TuShareBase):
    def __init__(self):
        super(IndexWeight, self).__init__()
        """
        初始化
        """

        # 创建index_weight数据集
        self.conn = DB_CONN['index_weight']

    def crawl(self, index_code='', trade_date='', start_date='', end_date=''):
        """
        :param index_code: 指数代码（二选一）
        :param trade_date: 交易日期（二选一）
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :return:
        index_code str 指数代码
        con_code    str 成分代码
        trade_date  str 交易日期
        weight  float   权重
        """
        if not index_code and not trade_date:
            return

        df = self.pro.query('index_weight', index_code=index_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['index_code', 'con_date','trade_date'])

    def crawl_all(self):
        trade_dates = get_all_trade_dates()
        for trade_date in trade_dates:
            self.crawl(trade_date=trade_date, start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    stock = IndexWeight()
    stock.crawl()
