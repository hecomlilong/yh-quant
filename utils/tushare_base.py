#  -*- coding: utf-8 -*-

import tushare as ts
from pymongo import UpdateOne, ASCENDING
from pandas import DataFrame


class TuShareBase:
    def __init__(self):
        """
        初始化
        """

        # 使用tushare pro
        self.pro = ts.pro_api('427882f17bbd866b6513afab0e4e00238219700c027e2b75a1c22123')

    def create_index(self, collection, index_fields=None):
        if index_fields is None or collection is None:
            return False
        return collection.create_index(index_fields)

    def save_data(self, df=None, collection=None, docs=None, filter_fields=None, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param df: 包含数据的DataFrame
        :param docs: 包含数据的list
        :param collection: 要保存的数据集
        :param filter_fields: 要保存的数据集的过滤条件
        :param extra_fields: 除了数据中保存的字段，需要额外保存的字段
        """

        if filter_fields is None or collection is None:
            return

        # 创建索引
        index = []
        for filter_field in filter_fields:
            index.append((filter_field, ASCENDING))
        res = self.create_index(collection, index)
        if res is not False:
            print('创建索引成功:%s' % res, flush=True)

        # 数据更新的请求列表
        update_requests = []

        # 将DataFrame中的行情数据，生成更新数据的请求
        if isinstance(df, DataFrame):
            for df_index in df.index:
                # 将DataFrame中的一行数据转dict
                doc = dict(df.loc[df_index])

                # 组装mongo数据过滤条件
                m_filter = {}
                for field in filter_fields:
                    m_filter[field] = doc[field]

                # 如果指定了其他字段，则更新dict
                if extra_fields is not None:
                    doc.update(extra_fields)

                # 生成一条数据库的更新请求
                update_requests.append(
                    UpdateOne(m_filter, {'$set': doc}, upsert=True)
                )

        elif isinstance(docs, list):
            for doc in docs:
                # 组装mongo数据过滤条件
                m_filter = {}
                for field in filter_fields:
                    m_filter[field] = doc[field]

                # 如果指定了其他字段，则更新dict
                if extra_fields is not None:
                    doc.update(extra_fields)

                # 生成一条数据库的更新请求
                update_requests.append(
                    UpdateOne(m_filter, {'$set': doc}, upsert=True)
                )

        # 如果写入的请求列表不为空，则保存都数据库中
        if len(update_requests) > 0:
            # 批量写入到数据库中，批量写入可以降低网络IO，提高速度
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存数据, 插入：%4d条, 更新：%4d条' %
                  (update_result.upserted_count, update_result.modified_count),
                  flush=True)


if __name__ == '__main__':
    dc = TuShareBase()
    dc.test()
