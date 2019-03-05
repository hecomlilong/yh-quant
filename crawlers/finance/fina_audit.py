#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取上市公司定期财务审计意见数据
"""


class FinaAudit(TuShareBase):
    def __init__(self):
        super(FinaAudit, self).__init__()
        """
        初始化
        """

        # 创建fina_audit数据集
        self.conn = DB_CONN['fina_audit']

    def crawl(self, ts_code, ann_date='', start_date='', end_date='', period=''):
        """
        params:ts_code  str Y   股票代码
        params:ann_date    str N   公告日期
        params:start_date  str N   报告期开始日期
        params:end_date    str N   报告期结束日期
        params:period  str N   报告期(每个季度最后一天的日期，比如20171231表示年报)

        :return:
        ts_code str TS股票代码
        ann_date    str 公告日期
        end_date    str 报告期
        audit_result    str 审计结果
        audit_fees  float   审计总费用（元）
        audit_agency    str 会计事务所
        audit_sign  str 签字会计师
        """
        if not ts_code:
            return
        
        df = self.pro.query('fina_audit', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','ann_date','end_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = FinaAudit()
    stock.crawl('000001.SZ')
