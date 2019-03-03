#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase

"""
获取上市公司基础信息
"""


class StockCompany(TuShareBase):
    def __init__(self):
        super(StockCompany, self).__init__()
        """
        初始化
        """

        # 创建stock_company数据集
        self.conn = DB_CONN['stock_company']

    def crawl(self, exchange='SSE'):
        """
        :param exchange: 交易所代码 ，SSE上交所 SZSE深交所 ，默认SSE
        :return:
        ts_code	str	Y	股票代码
        exchange	str	Y	交易所代码 ，SSE上交所 SZSE深交所
        chairman	str	Y	法人代表
        manager	str	Y	总经理
        secretary	str	Y	董秘
        reg_capital	float	Y	注册资本
        setup_date	str	Y	注册日期
        province	str	Y	所在省份
        city	str	Y	所在城市
        introduction	str	N	公司介绍
        website	str	Y	公司主页
        email	str	Y	电子邮件
        office	str	N	办公室
        employees	int	Y	员工人数
        main_business	str	N	主要业务及产品
        business_scope	str	N	经营范围
        """
        fields = 'ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,' \
                 'city,introduction,website,email,office,employees,main_business,business_scope'
        df = self.pro.query('stock_company', exchange=exchange,
                            fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code'])


if __name__ == '__main__':
    stock = StockCompany()
    stock.crawl()
