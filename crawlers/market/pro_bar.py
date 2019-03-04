#  -*- coding: utf-8 -*-

import tushare as ts
from utils.database import DB_CONN
from utils.tushare_base import TuShareBase


class ProBar(TuShareBase):
    def __init__(self):
        super(ProBar, self).__init__()
        """
        初始化
        """

        # 创建daily数据集
        # self.conn = DB_CONN['daily']

    def crawl(self, ts_code='', start_date='', end_date='', freq='D', asset='E', adj=None, ma=None):
        """
        :param ts_code: 股票代码
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :param freq: 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D， W周线，M月线
        :param freq: 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D， W周线，M月线
        :param freq: 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D， W周线，M月线
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
