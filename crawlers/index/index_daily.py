#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取指数每日行情，还可以通过bar接口获取。由于服务器压力，目前规则是单次调取最多取2800行记录，
可以设置start和end日期补全。指数行情也可以通过通用行情接口获取数据．
"""


class IndexDaily(TuShareBase):
    def __init__(self):
        super(IndexDaily, self).__init__()
        """
        初始化
        """

        # 创建index_daily数据集
        self.conn = DB_CONN['index_daily']

    def crawl(self, ts_code='', trade_date='', start_date='', end_date=''):
        """
        :param ts_code: 股票代码（二选一）
        :param trade_date: 交易日期（二选一）
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :return:
        ts_code str TS指数代码
        trade_date  str 交易日
        close   float   收盘点位
        open    float   开盘点位
        high    float   最高点位
        low float   最低点位
        pre_close   float   昨日收盘点
        change  float   涨跌点
        pct_chg float   涨跌幅
        vol float   成交量（手）
        amount  float   成交额（千元）
        """

        df = self.pro.query('index_daily', ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

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
    stock = IndexDaily()
    stock.crawl()
