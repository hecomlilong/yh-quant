#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：分红送股数据
"""


class Dividend(TuShareBase):
    def __init__(self):
        super(Dividend, self).__init__()
        """
        初始化
        """

        # 创建dividend数据集
        self.conn = DB_CONN['dividend']

    def crawl(self, ts_code='', ann_date='', record_date='', ex_date='', imp_ann_date=''):
        """
        params:ts_code  str N   TS代码
        params:ann_date    str N   公告日
        params:record_date str N   股权登记日期
        params:ex_date str N   除权除息日
        params:imp_ann_date    str N   实施公告日
        以上参数至少有一个不能为空
        :return:
        ts_code str Y   TS代码
        end_date    str Y   分红年度
        ann_date    str Y   预案公告日
        div_proc    str Y   实施进度
        stk_div float   Y   每股送转
        stk_bo_rate float   Y   每股送股比例
        stk_co_rate float   Y   每股转增比例
        cash_div    float   Y   每股分红（税后）
        cash_div_tax    float   Y   每股分红（税前）
        record_date str Y   股权登记日
        ex_date str Y   除权除息日
        pay_date    str Y   派息日
        div_listdate    str Y   红股上市日
        imp_ann_date    str Y   实施公告日
        base_date   str N   基准日
        base_share  float   N   基准股本（万）
        """
        if not ts_code and not ann_date and not record_date and not ex_date and not imp_ann_date:
            return
        df = self.pro.query('dividend', ts_code=ts_code,ann_date=ann_date,record_date=record_date,ex_date=ex_date,imp_ann_date=imp_ann_date)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','end_date','ex_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = Dividend()
    stock.crawl('000001.SZ')
