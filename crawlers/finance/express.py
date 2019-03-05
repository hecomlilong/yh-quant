#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取上市公司业绩快报
"""


class Express(TuShareBase):
    def __init__(self):
        super(Express, self).__init__()
        """
        初始化
        """

        # 创建express数据集
        self.conn = DB_CONN['express']

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
        revenue float   营业收入(元)
        operate_profit  float   营业利润(元)
        total_profit    float   利润总额(元)
        n_income    float   净利润(元)
        total_assets    float   总资产(元)
        total_hldr_eqy_exc_min_int  float   股东权益合计(不含少数股东权益)(元)
        diluted_eps float   每股收益(摊薄)(元)
        diluted_roe float   净资产收益率(摊薄)(%)
        yoy_net_profit  float   去年同期修正后净利润
        bps float   每股净资产
        yoy_sales   float   同比增长率:营业收入
        yoy_op  float   同比增长率:营业利润
        yoy_tp  float   同比增长率:利润总额
        yoy_dedu_np float   同比增长率:归属母公司股东的净利润
        yoy_eps float   同比增长率:基本每股收益
        yoy_roe float   同比增减:加权平均净资产收益率
        growth_assets   float   比年初增长率:总资产
        yoy_equity  float   比年初增长率:归属母公司的股东权益
        growth_bps  float   比年初增长率:归属于母公司股东的每股净资产
        or_last_year    float   去年同期营业收入
        op_last_year    float   去年同期营业利润
        tp_last_year    float   去年同期利润总额
        np_last_year    float   去年同期净利润
        eps_last_year   float   去年同期每股收益
        open_net_assets float   期初净资产
        open_bps    float   期初每股净资产
        perf_summary    str 业绩简要说明
        is_audit    int 是否审计： 1是 0否
        remark  str 备注
        """
        if not ts_code:
            return
        
        df = self.pro.query('express', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','ann_date','end_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = Express()
    stock.crawl('000001.SZ')
