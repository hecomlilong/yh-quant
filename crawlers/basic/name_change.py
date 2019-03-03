#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
历史名称变更记录
"""


class NameChange(TuShareBase):
    def __init__(self):
        super(NameChange, self).__init__()
        """
        初始化
        """

        # 创建name_change数据集
        self.conn = DB_CONN['name_change']

    def crawl(self, ts_code='', start_date='', end_date=''):
        """

        :param ts_code: TS代码
        :param start_date: 公告开始日期
        :param end_date: 公告结束日期
        :return:
        ts_code	str	Y	TS代码
        name	str	Y	证券名称
        start_date	str	Y	开始日期
        end_date	str	Y	结束日期
        ann_date	str	Y	公告日期
        change_reason	str	Y	变更原因
        """
        fields = 'ts_code,name,start_date,end_date,ann_date,change_reason'
        df = self.pro.query('namechange', ts_code=ts_code, start_date=start_date, end_date=end_date,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'start_date'])


if __name__ == '__main__':
    stock = NameChange()
    stock.crawl()
