#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
描述：获取指数基础信息。
"""


class IndexBasic(TuShareBase):
    def __init__(self):
        super(IndexBasic, self).__init__()
        """
        初始化
        """

        # 创建index_basic数据集
        self.conn = DB_CONN['index_basic']

    def crawl(self, market, publisher='', category=''):
        """
        :param market   str Y   交易所或服务商
        :param publisher   str N   发布商
        :param category    str N   指数类别
        :return:
        ts_code str TS代码
        name    str 简称
        fullname    str 指数全称
        market  str 市场
        publisher   str 发布方
        index_type  str 指数风格
        category    str 指数类别
        base_date   str 基期
        base_point  float   基点
        list_date   str 发布日期
        weight_rule str 加权方式
        desc    str 描述
        exp_date    str 终止日期
市场说明(market)

市场代码    说明
MSCI    MSCI指数
CSI 中证指数
SSE 上交所指数
SZSE    深交所指数
CICC    中金所指数
SW  申万指数
CNI 国证指数
OTH 其他指数
指数列表

主题指数
规模指数
策略指数
风格指数
综合指数
成长指数
价值指数
有色指数
化工指数
能源指数
其他指数
外汇指数
基金指数
商品指数
债券指数
行业指数
贵金属指数
农副产品指数
软商品指数
油脂油料指数
非金属建材指数
煤焦钢矿指数
谷物指数
一级行业指数
二级行业指数
三级行业指数
        """
        # fields = 'trade_date,exchange_id,name,close,change,rank,market_type,amount,net_amount,buy,sell'
        df = self.pro.query('index_basic', market=market, publisher=publisher,category=category)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'base_date'])

    def crawl_all(self):
        for market in ['MSCI','CSI','SSE','SZSE','CICC','SW','CNI','OTH']:
            self.crawl(market=market)


if __name__ == '__main__':
    stock = IndexBasic()
    stock.crawl(market='SW')
