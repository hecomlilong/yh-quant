#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

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

    def crawl(self, ts_code='', trade_date='',start_date='',end_date=''):
        """
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
            df = self.pro.query('daily', trade_date=trade_date, start_date=start_date,end_date=end_date,
                            fields=fields)
        else:
            df = self.pro.query('daily', ts_code=ts_code, start_date=start_date,end_date=end_date,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','trade_date'])


if __name__ == '__main__':
    stock = Daily()
    stock.crawl()
