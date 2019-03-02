#  -*- coding: utf-8 -*-

from pymongo import UpdateOne
from database import DB_CONN
from tushare_base import TuShareBase

"""
从tushare获取公募基金数据列表，包括场内和场外基金，保存到本地的MongoDB数据库中
"""


class FundBasicCrawler(TuShareBase):
    def __init__(self):
        super(FundBasicCrawler, self).__init__()
        """
        初始化
        """

        # 创建fund数据集
        self.fund = DB_CONN['fund_basic']

    def crawl(self, market='E'):
        """
        抓取公募基金数据列表，包括场内和场外基金
        :param market: 交易市场: E场内 O场外（默认E）
        """

        fund_df = self.pro.query('fund_basic', market=market)
        self.save_data(df=fund_df, collection=self.fund)

    def save_data(self, df, collection, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param df: 包含数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了数据中保存的字段，需要额外保存的字段
        """

        # 数据更新的请求列表
        update_requests = []

        # 将DataFrame中的行情数据，生成更新数据的请求
        for df_index in df.index:
            # 将DataFrame中的一行数据转dict
            doc = dict(df.loc[df_index])

            # 如果指定了其他字段，则更新dict
            if extra_fields is not None:
                doc.update(extra_fields)

            # 生成一条数据库的更新请求
            # 注意：
            # 需要在ts_code字段上增加索引，否则随着数据量的增加，
            # 写入速度会变慢，需要创建索引。创建索引需要在MongoDB-shell中执行命令式：
            # db.fund.createIndex({'ts_code':1},{'background':true})
            update_requests.append(
                UpdateOne(
                    {'ts_code': doc['ts_code']},
                    {'$set': doc},
                    upsert=True)
            )
        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存公募基金数据, 插入：%4d条, 更新：%4d条' %
                  (update_result.upserted_count, update_result.modified_count),
                  flush=True)


# 抓取程序的入口函数
if __name__ == '__main__':
    dc = FundBasicCrawler()
    dc.crawl('E')  # 交易市场: E场内
    dc.crawl('O')  # 交易市场: O场外
