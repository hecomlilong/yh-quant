#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
描述：获取A股周线行情
限量：单次最大3700，总量不限制
"""


class Weekly(TuShareBase):
    def __init__(self):
        super(Weekly, self).__init__()
        """
        初始化
        """

        # 创建Weekly数据集
        self.conn = DB_CONN['weekly']

    def crawl(self, ts_code='', trade_date='',start_date='',end_date=''):
        """
params:
ts_code str N   TS代码 （ts_code,trade_date两个参数任选一）
trade_date  str N   交易日期 （每周五日期，YYYYMMDD格式）
start_date  str N   开始日期
end_date    str N   结束日期

returns:
ts_code str Y   股票代码
trade_date  str Y   交易日期
close   float   Y   周收盘价
open    float   Y   周开盘价
high    float   Y   周最高价
low float   Y   周最低价
pre_close   float   Y   上一周收盘价
change  float   Y   周涨跌额
pct_chg float   Y   周涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
vol float   Y   周成交量
amount  float   Y   周成交额
        """
        if ts_code == '' and trade_date == '':
            return
        fields = 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        if ts_code == '':
            df = self.pro.query('weekly', trade_date=trade_date, start_date=start_date,end_date=end_date,
                            fields=fields)
        else:
            df = self.pro.query('weekly', ts_code=ts_code, start_date=start_date,end_date=end_date,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','trade_date'])


if __name__ == '__main__':
    stock = Weekly()
    stock.crawl()
