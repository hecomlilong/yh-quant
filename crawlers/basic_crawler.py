#  -*- coding: utf-8 -*-

import traceback
from datetime import datetime, timedelta

import tushare as ts
from pandas.io import json
from pymongo import UpdateOne

from utils.database import DB_CONN
from utils.stock_util import get_trading_dates
import math

"""
从tushare获取股票基础数据，保存到本地的MongoDB数据库中
"""


def crawl_basic(begin_date=None, end_date=None):
    """
    抓取指定时间范围内的股票基础信息
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """

    # 如果没有指定开始日期，则默认为前一日
    if begin_date is None:
        begin_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 如果没有指定结束日期，则默认为前一日
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 获取指定日期范围的所有交易日列表
    all_dates = get_trading_dates(begin_date, end_date)

    # 按照每个交易日抓取
    for date in all_dates:
        try:
            # 抓取当日的基本信息
            crawl_basic_at_date(date)
        except:
            print('抓取股票基本信息时出错，日期：%s' % date, flush=True)


def crawl_basic_at_date(date):
    """
    从Tushare抓取指定日期的股票基本信息
    :param date: 日期
    """
    # 从TuShare获取基本信息，index是股票代码列表
    df_basics = ts.get_stock_basics(date)

    # 如果当日没有基础信息，在不做操作
    if df_basics is None:
        return

    # 初始化更新请求列表
    update_requests = []
    # 获取所有股票代码集合
    codes = list(set(df_basics.index))  #codes = list(set(df_basics.index))[:2]
    # 按照股票代码提取所有数据
    for code in codes:
        # 获取一只股票的数据
        doc = dict(df_basics.loc[code])
        try:
            # not doc['timeToMarket'] for value == 0
            # math.isnan(doc['timeToMarket']) for value is nan
            # dtype of doc['timeToMarket'] can be: numpy.series float64
            if not doc['timeToMarket'] or math.isnan(doc['timeToMarket']):
                continue
            pos = str(doc['timeToMarket']).find(".")
            if pos >= 0:
                local_time = str(doc['timeToMarket'])[0:pos]
            else:
                local_time = str(doc['timeToMarket'])

            # API返回的数据中，上市日期是一个int类型。将上市日期，20180101转换为2018-01-01的形式
            time_to_market = datetime \
                .strptime(local_time, '%Y%m%d') \
                .strftime('%Y-%m-%d')

            # 将总股本和流通股本转为数字
            totals = float(doc['totals'])
            outstanding = float(doc['outstanding'])

            # 组合成基本信息文档
            doc.update({
                # 股票代码
                'code': code,
                # 日期
                'date': date,
                # 上市日期
                'timeToMarket': time_to_market,
                # 流通股本
                'outstanding': outstanding,
                # 总股本
                'totals': totals
            })

            # 生成更新请求，需要按照code和date创建索引
            # db.basic.createIndex({'code':1,'date':1},{'background':true})
            # tushare
            # numpy.int64/numpy.float64等数据类型，保存到mongodb时无法序列化。
            # 解决办法：这里使用pandas.json强制转换成json字符串，然后再转换成dict。int64/float64转换成int,float
            update_requests.append(
                UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': json.loads(json.dumps(doc))}, upsert=True))
        except:
            print('发生异常，股票代码：%s，日期：%s' % (code, date), flush=True)
            print(type(doc['timeToMarket']),flush=True)
            print(doc, flush=True)
            print(traceback.print_exc())

    # 如果抓到了数据
    if len(update_requests) > 0:
        update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)

        print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %
              (date, update_result.upserted_count, update_result.modified_count), flush=True)


if __name__ == '__main__':
    # crawl_basic('2017-01-01', '2017-12-31')
    crawl_basic('2016-08-09', '2019-03-01')