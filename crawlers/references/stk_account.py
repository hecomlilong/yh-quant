#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取股票账户开户数据，统计周期为一周
"""


class STKAccount(TuShareBase):
    def __init__(self):
        super(STKAccount, self).__init__()
        """
        初始化
        """

        # 创建stk_account数据集
        self.conn = DB_CONN['stk_account']

    def crawl(self, date='',start_date='',end_date=''):
        """
        :param date str N   日期
        :param start_date  str N   开始日期
        :param end_date    str N   结束日期
        :return:
        date    str Y   统计周期
        weekly_new  float   Y   本周新增（万）
        total   float   Y   期末总账户数（万）
        weekly_hold float   Y   本周持仓账户数（万）
        weekly_trade    float   Y   本周参与交易账户数（万）
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('stk_account', date=date, start_date=start_date, end_date=end_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['date'])

    def crawl_all(self):
        # codes = get_all_codes_pro()
        # for code in codes:
        #     self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = STKAccount()
    stock.crawl()
