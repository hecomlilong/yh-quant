#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
更新时间：交易日每天15点～16点之间
调取说明：每分钟内最多调取200次，超过5000积分无限制
获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据．
"""


class Daily(TuShareBase):
    def __init__(self):
        super(Daily, self).__init__()
        """
        初始化
        """

        # 创建daily数据集
        self.conn = DB_CONN['daily']

    def crawl(self, ts_code='', trade_date='', start_date='', end_date=''):
        """
        :param ts_code: 股票代码（二选一）
        :param trade_date: 交易日期（二选一）
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :return:
        ts_code str 股票代码
        trade_date  str 交易日期
        open    float   开盘价
        high    float   最高价
        low float   最低价
        close   float   收盘价
        pre_close   float   昨收价
        change  float   涨跌额
        pct_chg float   涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
        vol float   成交量 （手）
        amount  float   成交额 （千元）
        """
        if ts_code == '' and trade_date == '':
            return
        fields = 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        if ts_code == '':
            df = self.pro.query('daily', trade_date=trade_date, start_date=start_date, end_date=end_date,
                                fields=fields)
        else:
            df = self.pro.query('daily', ts_code=ts_code, start_date=start_date, end_date=end_date,
                                fields=fields)
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
    stock = Daily()
    stock.crawl()
