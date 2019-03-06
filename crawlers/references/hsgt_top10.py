#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取沪股通、深股通每日前十大成交详细数据
"""


class HSGTTop10(TuShareBase):
    def __init__(self):
        super(HSGTTop10, self).__init__()
        """
        初始化
        """

        # 创建hsgt_top10数据集
        self.conn = DB_CONN['hsgt_top10']

    def crawl(self, ts_code='', trade_date='', start_date='', end_date='', market_type=''):
        """
        :param ts_code  str N   股票代码（二选一）
        :param trade_date  str N   交易日期（二选一）
        :param start_date  str N   开始日期
        :param end_date    str N   结束日期
        :param market_type str N   市场类型（1：沪市 3：深市）
        :return:
        trade_date  str 交易日期
        ts_code str 股票代码
        name    str 股票名称
        close   float   收盘价
        change  float   涨跌额
        rank    int 资金排名
        market_type str 市场类型（1：沪市 3：深市）
        amount  float   成交金额（元）
        net_amount  float   净成交金额（元）
        buy float   买入金额（元）
        sell    float   卖出金额（元）
        """
        if not ts_code and not trade_date:
            return
        fields = 'trade_date,ts_code,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('hsgt_top10', ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date, market_type=market_type
                                fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date','market_type'])

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
    stock = HSGTTop10()
    stock.crawl()
