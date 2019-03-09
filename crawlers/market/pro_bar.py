#  -*- coding: utf-8 -*-

import tushare as ts
from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro, get_daily_conn_name, get_begin_end_date
from datetime import datetime

"""
更新时间：股票和指数通常在15点～17点之间，数字货币实时更新，具体请参考各接口文档明细。
描述：目前整合了股票（未复权、前复权、后复权）、指数、数字货币、ETF基金、期货、期权的行情数据，未来还将整合包括外汇在内的所有交易行情数据，同时提供分钟数据。
"""


class ProBar(TuShareBase):
    def __init__(self):
        super(ProBar, self).__init__()
        """
        初始化
        """
        # 创建复权数据集
        self.conn = ''

    def crawl(self, ts_code='', start_date='', end_date='', freq='D', asset='E', adj='qfq', ma=None, factors=''):
        """
        :param ts_code: 股票代码
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :param freq: 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D， W周线，M月线
        :param asset: 资产类别：E股票 I沪深指数 C数字货币 F期货 FD基金 O期权，默认E
        :param adj: 复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
        :param ma: 均线，支持任意周期的均价和均量，输入任意合理int数值
        :param factors: 股票因子（asset='E'有效）支持 tor换手率 vr量比
        :return:
        ts_code str 股票代码
        trade_date  str 交易日期
        open    float   开盘价
        high    float   最高价
        low float   最低价
        close   float   收盘价
        pre_close   float   昨收价
        change  float   涨跌额
        pct_chg float   涨跌幅
        vol float   成交量 （手）
        amount  float   成交额 （千元）
        """
        if adj is None:
            return
        self.conn = DB_CONN[get_daily_conn_name(adj, asset, freq)]

        # 取复权行情
        df = ts.pro_bar(pro_api=self.pro, ts_code=ts_code, freq=freq, adj=adj, asset=asset, start_date=start_date,
                        end_date=end_date, ma=ma, factors=factors)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

    def crawl_all_stock(self, start_date='', end_date='', freq='D'):
        start_date, end_date = get_begin_end_date(start_date, end_date)
        codes = get_all_codes_pro()
        if isinstance(freq, list):
            for freq_i in freq:
                for code in codes:
                    self.crawl(ts_code=code, freq=freq_i, start_date=start_date, end_date=end_date)
        elif isinstance(freq, str):
            for code in codes:
                self.crawl(ts_code=code, freq=freq, start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    stock = ProBar()
    # stock.crawl(ts_code='000001.SZ')
    stock.crawl_all_stock(start_date='20190101', end_date='2019-03-08')
