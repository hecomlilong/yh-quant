#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_fund_codes
from time import sleep
import traceback

"""
从tushare获取公募基金持仓数据，保存到本地的MongoDB数据库中
"""


class FundPortfolio(TuShareBase):
    def __init__(self):
        super(FundPortfolio, self).__init__()
        """
        初始化
        """

        # 创建fund_portfolio数据集
        self.fund_portfolio = DB_CONN['fund_portfolio']

    def crawl(self):
        """
        抓取公募基金持仓数据
        """

        # 获取基金代码
        codes = get_all_fund_codes()
        for code in codes:
            try:
                fund_portfolio_df = self.pro.query('fund_portfolio', ts_code=code)
                self.save_data(df=fund_portfolio_df, collection=self.fund_portfolio,
                               filter_fields=['ts_code', 'ann_date', 'end_date', 'symbol'])
                sleep(1)
            except:
                print(traceback.print_exc())


# 抓取程序的入口函数
if __name__ == '__main__':
    dc = FundPortfolio()
    dc.crawl()
