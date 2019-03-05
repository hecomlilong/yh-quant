#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取业绩预告数据
"""


class Forecast(TuShareBase):
    def __init__(self):
        super(Forecast, self).__init__()
        """
        初始化
        """

        # 创建forecast数据集
        self.conn = DB_CONN['forecast']

    def crawl(self, ts_code='', ann_date='', start_date='', end_date='', period='',type=''):
        """
        params:ts_code  str N   股票代码
        params:ann_date    str N   公告日期 (二选一)
        params:start_date  str N   公告开始日期
        params:end_date    str N   公告结束日期
        params:period  str N   报告期 (二选一) (每个季度最后一天的日期，比如20171231表示年报)
        params:type    str N   预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
        :return:
        ts_code str TS股票代码
        ann_date    str 公告日期
        end_date    str 报告期
        type    str 业绩预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
        p_change_min    float   预告净利润变动幅度下限（%）
        p_change_max    float   预告净利润变动幅度上限（%）
        net_profit_min  float   预告净利润下限（万元）
        net_profit_max  float   预告净利润上限（万元）
        last_parent_net float   上年同期归属母公司净利润
        first_ann_date  str 首次公告日
        summary str 业绩预告摘要
        change_reason   str 业绩变动原因
        """

        fields = 'ts_code,ann_date,f_ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason'
        
        df = self.pro.query('forecast', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period,type=type, fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','ann_date','end_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = Forecast()
    stock.crawl('000001.SZ')
