#  -*- coding: utf-8 -*-


from pymongo import ASCENDING
from utils.database import DB_CONN
from datetime import datetime, timedelta


def get_trading_dates(begin_date=None, end_date=None):
    """
    获取指定日期范围的按照正序排列的交易日列表
    如果没有指定日期范围，则获取从当期日期向前365个自然日内的所有交易日
    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: 日期列表
    """

    # 当前日期
    now = datetime.now()
    # 开始日期，默认今天向前的365个自然日
    if begin_date is None:
        # 当前日期减去365天
        one_year_ago = now - timedelta(days=365)
        # 转化为str类型
        begin_date = one_year_ago.strftime('%Y-%m-%d')

    # 结束日期默认为今天
    if end_date is None:
        end_date = now.strftime('%Y-%m-%d')

    # 用上证综指000001作为查询条件，因为指数是不会停牌的，所以可以查询到所有的交易日
    daily_cursor = DB_CONN.daily.find(
        {'code': '000001', 'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
        sort=[('date', ASCENDING)],
        projection={'date': True, '_id': False})

    # 转换为日期列表
    dates = [x['date'] for x in daily_cursor]

    return dates


def get_all_codes():
    """
    获取所有股票代码列表
    :return: 股票代码列表
    """

    # 通过distinct函数拿到所有不重复的股票代码列表
    return DB_CONN.basic.distinct('code')


def get_all_codes_pro():
    """
    获取所有股票代码列表
    :return: 股票代码列表
    """

    # 通过distinct函数拿到所有不重复的股票代码列表
    return DB_CONN.stock_basic.distinct('ts_code')


def get_all_concept():
    return DB_CONN.concept.distinct('code')


def get_all_fund_codes():
    """
    获取所有基金代码列表
    :return: 基金代码列表
    """

    # 通过distinct函数拿到所有不重复的基金代码列表
    return DB_CONN.fund_basic.distinct('ts_code')


def get_daily_conn_name(adj='qfq', asset='E', freq='D'):
    """

    :param adj: qfq前复权 hfq后复权
    :param asset: E股票 I沪深指数 C数字货币 F期货 FD基金 O期权，默认E
    :param freq: 数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D， W周线，M月线
    :return:
    """
    if not adj:
        return ''
    return 'daily_%s_%s_%s' % (adj, asset, freq)


def get_codes(ts_code=''):
    if ts_code:
        if isinstance(ts_code, str):
            codes = [ts_code]
        elif isinstance(ts_code, list):
            codes = ts_code
        else:
            codes = []
    else:
        # 获取所有股票代码
        codes = get_all_codes_pro()
    return codes


def get_begin_end_date(begin_date='', end_date=''):
    if not begin_date and not end_date:
        # 当前日期
        now = datetime.now()
        begin_date = now.strftime('%Y%m%d')
        end_date = now.strftime('%Y%m%d')
    begin_date = ''.join(begin_date.split("-"))
    end_date = ''.join(end_date.split("-"))
    return begin_date, end_date


if __name__ == '__main__':
    get_all_codes()
