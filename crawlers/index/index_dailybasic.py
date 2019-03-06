#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：目前只提供上证综指，深证成指，上证50，中证500，中小板指，创业板指的每日指标数据
数据来源：Tushare社区统计计算
数据历史：从2004年1月开始提供
"""


class IndexDailyBasic(TuShareBase):
    def __init__(self):
        super(IndexDailyBasic, self).__init__()
        """
        初始化
        """

        # 创建index_dailybasic数据集
        self.conn = DB_CONN['index_dailybasic']

    def crawl(self, ts_code='', trade_date='', start_date='', end_date=''):
        """
        :param trade_date   str N   交易日期 （格式：YYYYMMDD，比如20181018，下同）
        :param ts_code str N   TS代码
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :return:
        ts_code str Y   TS代码
        trade_date  str Y   交易日期
        total_mv    float   Y   当日总市值（元）
        float_mv    float   Y   当日流通市值（元）
        total_share float   Y   当日总股本（股）
        float_share float   Y   当日流通股本（股）
        free_share  float   Y   当日自由流通股本（股）
        turnover_rate   float   Y   换手率
        turnover_rate_f float   Y   换手率(基于自由流通股本)
        pe  float   Y   市盈率
        pe_ttm  float   Y   市盈率TTM
        pb  float   Y   市净率
        """
        if not ts_code and not trade_date:
            return
        df = self.pro.query('index_dailybasic', ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

    def crawl_all(self, start_date='', end_date=''):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code, start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    stock = IndexDailyBasic()
    stock.crawl()
