#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
描述：获取A股周线行情
限量：单次最大3700，总量不限制
"""


class Monthly(TuShareBase):
    def __init__(self):
        super(Monthly, self).__init__()
        """
        初始化
        """

        # 创建monthly数据集
        self.conn = DB_CONN['monthly']

    def crawl(self, ts_code='', trade_date='', start_date='', end_date=''):
        """

        :param ts_code: TS代码 （ts_code,trade_date两个参数任选一）
        :param trade_date: 交易日期 （每周五日期，YYYYMMDD格式）
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return:
        ts_code	str	Y	股票代码
        trade_date	str	Y	交易日期
        close	float	Y	月收盘价
        open	float	Y	月开盘价
        high	float	Y	月最高价
        low	float	Y	月最低价
        pre_close	float	Y	上月收盘价
        change	float	Y	月涨跌额
        pct_chg	float	Y	月涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
        vol	float	Y	月成交量
        amount	float	Y	月成交额
        """
        if ts_code == '' and trade_date == '':
            return
        fields = 'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        if ts_code == '':
            df = self.pro.query('monthly', trade_date=trade_date, start_date=start_date, end_date=end_date,
                                fields=fields)
        else:
            df = self.pro.query('monthly', ts_code=ts_code, start_date=start_date, end_date=end_date,
                                fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])


if __name__ == '__main__':
    stock = Monthly()
    stock.crawl()
