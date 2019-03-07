#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
更新时间：不定期
描述：获取股票每日停复牌信息
"""


class Suspend(TuShareBase):
    def __init__(self):
        super(Suspend, self).__init__()
        """
        初始化
        """

        # 创建suspend数据集
        self.conn = DB_CONN['suspend']

    def crawl(self, ts_code='', suspend_date='', resume_date=''):
        """
        params:ts_code str N   股票代码(三选一)
        params:suspend_date    str N   停牌日期(三选一)
        params:resume_date str N   复牌日期(三选一)
        :return:
        ts_code str 股票代码
        suspend_date    str 停牌日期
        resume_date str 复牌日期
        ann_date    str 公告日期
        suspend_reason  str 停牌原因
        reason_type str 停牌原因类别
        """
        if not ts_code and not suspend_date and not resume_date:
            return
        fields = 'ts_code,suspend_date,resume_date,ann_date,suspend_reason,reason_type'
        df = self.pro.query('suspend', ts_code=ts_code, suspend_date=suspend_date, resume_date=resume_date, fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'suspend_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = Suspend()
    stock.crawl('000001.SZ')
