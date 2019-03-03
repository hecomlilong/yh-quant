#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
获取新股上市列表数据
"""


class NewShare(TuShareBase):
    def __init__(self):
        super(NewShare, self).__init__()
        """
        初始化
        """

        # 创建new_share数据集
        self.conn = DB_CONN['new_share']

    def crawl(self, start_date='', end_date=''):
        """
        :param start_date: 上网发行开始日期
        :param end_date: 上网发行结束日期
        :return:
        ts_code	str	Y	TS股票代码
        sub_code	str	Y	申购代码
        name	str	Y	名称
        ipo_date	str	Y	上网发行日期
        issue_date	str	Y	上市日期
        amount	float	Y	发行总量（万股）
        market_amount	float	Y	上网发行总量（万股）
        price	float	Y	发行价格
        pe	float	Y	市盈率
        limit_amount	float	Y	个人申购上限（万股）
        funds	float	Y	募集资金（亿元）
        ballot	float	Y	中签率
        """
        fields = 'ts_code,sub_code,name,ipo_date,issue_date,amount,market_amount,price,pe,limit_amount,funds,ballot'
        df = self.pro.query('new_share', start_date=start_date, end_date=end_date,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'sub_code'])


if __name__ == '__main__':
    stock = NewShare()
    stock.crawl()
