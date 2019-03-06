#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：龙虎榜每日交易明细
数据历史： 2005年至今
限量：单次最大10000
"""


class TopList(TuShareBase):
    def __init__(self):
        super(TopList, self).__init__()
        """
        初始化
        """

        # 创建top_list数据集
        self.conn = DB_CONN['top_list']

    def crawl(self, trade_date, ts_code=''):
        """
        :param trade_date   str Y   交易日期
        :param ts_code str N   股票代码
        :return:
        trade_date  str Y   交易日期
        ts_code str Y   TS代码
        name    str Y   名称
        close   float   Y   收盘价
        pct_change  float   Y   涨跌幅
        turnover_rate   float   Y   换手率
        amount  float   Y   总成交额
        l_sell  float   Y   龙虎榜卖出额
        l_buy   float   Y   龙虎榜买入额
        l_amount    float   Y   龙虎榜成交额
        net_amount  float   Y   龙虎榜净买入额
        net_rate    float   Y   龙虎榜净买额占比
        amount_rate float   Y   龙虎榜成交额占比
        float_values    float   Y   当日流通市值
        reason  str Y   上榜理由
        """
        if not trade_date:
            return
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('top_list', ts_code=ts_code, trade_date=trade_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

    def crawl_all(self, trade_date=''):
        if trade_date == '':
            # 当前日期
            now = datetime.now()
            trade_date = now.strftime('%Y%m%d')
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code, trade_date=trade_date)


if __name__ == '__main__':
    stock = TopList()
    stock.crawl('20170105')
