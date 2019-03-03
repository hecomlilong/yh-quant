#  -*- coding: utf-8 -*-

from pymongo import UpdateOne
from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
从tushare获取公募基金数据列表，包括场内和场外基金，保存到本地的MongoDB数据库中
"""


class FundBasic(TuShareBase):
    def __init__(self):
        super(FundBasic, self).__init__()
        """
        初始化
        """

        # 创建fund数据集
        self.fund = DB_CONN['fund_basic']

    def crawl(self, market='E'):
        """
        抓取公募基金数据列表，包括场内和场外基金
        :param market: 交易市场: E场内 O场外（默认E）
        """

        fund_df = self.pro.query('fund_basic', market=market)
        self.save_data(df=fund_df, collection=self.fund, filter_fields=['ts_code'])


# 抓取程序的入口函数
if __name__ == '__main__':
    dc = FundBasic()
    dc.crawl('E')  # 交易市场: E场内
    dc.crawl('O')  # 交易市场: O场外
