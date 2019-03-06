#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取沪股通、深股通、港股通每日资金流向数据
"""


class MoneyFlowHSGT(TuShareBase):
    def __init__(self):
        super(MoneyFlowHSGT, self).__init__()
        """
        初始化
        """

        # 创建moneyflow_hsgt数据集
        self.conn = DB_CONN['moneyflow_hsgt']

    def crawl(self, trade_date='', start_date='', end_date=''):
        """
        :param trade_date: 交易日期（二选一）
        :param start_date: 开始日期(YYYYMMDD)（二选一）
        :param end_date: 结束日期(YYYYMMDD)
        :return:
        trade_date  str 交易日期
        ggt_ss  str 港股通（上海）
        ggt_sz  str 港股通（深圳）
        hgt str 沪股通（百万元）
        sgt str 深股通（百万元）
        north_money str 北向资金（百万元）
        south_money str 南向资金（百万元）
        """
        if not start_date and not trade_date:
            return
        fields = 'trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money'
        df = self.pro.query('moneyflow_hsgt', trade_date=trade_date, start_date=start_date, end_date=end_date,
                                fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['trade_date'])

    def crawl_all(self, start_date='', end_date=''):
        if start_date == '' and end_date == '':
            # 当前日期
            now = datetime.now()
            start_date = now.strftime('%Y%m%d')
            end_date = now.strftime('%Y%m%d')
        # todo 组装start_date和end_date使得每次请求数据小于60条
        self.crawl(start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    stock = MoneyFlowHSGT()
    stock.crawl()
