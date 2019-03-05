#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获得上市公司主营业务构成，分地区和产品两种方式
"""


class FinaMainBZ(TuShareBase):
    def __init__(self):
        super(FinaMainBZ, self).__init__()
        """
        初始化
        """

        # 创建fina_mainbz数据集
        self.conn = DB_CONN['fina_mainbz']

    def crawl(self, ts_code, type='', start_date='', end_date='', period=''):
        """
        params:ts_code  str Y   股票代码
        params:type    str N   类型：P按产品 D按地区（请输入大写字母P或者D）
        params:start_date  str N   报告期开始日期
        params:end_date    str N   报告期结束日期
        params:period  str N   报告期(每个季度最后一天的日期，比如20171231表示年报)

        :return:
        ts_code str TS代码
        end_date    str 报告期
        bz_item str 主营业务来源
        bz_sales    float   主营业务收入(元)
        bz_profit   float   主营业务利润(元)
        bz_cost float   主营业务成本(元)
        curr_type   str 货币代码
        update_flag str 是否更新
        """
        if not ts_code:
            return
        
        df = self.pro.query('fina_mainbz', ts_code=ts_code,type=type,start_date=start_date,end_date=end_date,period=period)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','end_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = FinaMainBZ()
    stock.crawl('000001.SZ')
