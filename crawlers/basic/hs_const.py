#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
获取沪股通、深股通成分数据
"""


class HSConst(TuShareBase):
    def __init__(self):
        super(HSConst, self).__init__()
        """
        初始化
        """

        # 创建hs_const数据集
        self.conn = DB_CONN['hs_const']

    def crawl(self, hs_type='SH', is_new=1):
        """
        :param hs_type: 类型 SH 沪股通 SZ 深股通
        :param is_new: 1 是 0 否 (默认1)
        :return:
        ts_code	str	Y	TS代码
        hs_type	str	Y	沪深港通类型SH沪SZ深
        in_date	str	Y	纳入日期
        out_date	str	Y	剔除日期
        is_new	str	Y	是否最新 1是 0否
        """
        fields = 'ts_code,hs_type,in_date,out_date,is_new'
        df = self.pro.query('hs_const', hs_type=hs_type, is_new=is_new,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code'])


if __name__ == '__main__':
    stock = HSConst()
    stock.crawl()
