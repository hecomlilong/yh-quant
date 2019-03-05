#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro
from datetime import datetime

"""
更新时间：交易日每日15点～17点之间
描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
"""


class DailyBasic(TuShareBase):
    def __init__(self):
        super(DailyBasic, self).__init__()
        """
        初始化
        """

        # 创建daily_basic数据集
        self.conn = DB_CONN['daily_basic']

    def crawl(self, ts_code='', trade_date='', start_date='', end_date=''):
        """
        :param ts_code: 股票代码（二选一）
        :param trade_date: 交易日期（二选一）
        :param start_date: 开始日期(YYYYMMDD)
        :param end_date: 结束日期(YYYYMMDD)
        :return:
        ts_code	str	TS股票代码
        trade_date	str	交易日期
        close	float	当日收盘价
        turnover_rate	float	换手率（%）
        turnover_rate_f	float	换手率（自由流通股）
        volume_ratio	float	量比
        pe	float	市盈率（总市值/净利润）
        pe_ttm	float	市盈率（TTM）
        pb	float	市净率（总市值/净资产）
        ps	float	市销率
        ps_ttm	float	市销率（TTM）
        total_share	float	总股本 （万）
        float_share	float	流通股本 （万）
        free_share	float	自由流通股本 （万）
        total_mv	float	总市值 （万元）
        circ_mv	float	流通市值（万元）
        """
        if ts_code == '' and trade_date == '':
            return
        fields = 'ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,' \
                 'pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,free_share,total_mv,circ_mv'
        if ts_code == '':
            df = self.pro.query('daily_basic', trade_date=trade_date, start_date=start_date, end_date=end_date,
                                fields=fields)
        else:
            df = self.pro.query('daily_basic', ts_code=ts_code, start_date=start_date, end_date=end_date,
                                fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code', 'trade_date'])

    def crawl_all(self, start_date='', end_date=''):
        if start_date == '' and end_date == '':
            # 当前日期
            now = datetime.now()
            start_date = now.strftime('%Y%m%d')
            end_date = now.strftime('%Y%m%d')
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code, start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    stock = DailyBasic()
    stock.crawl()
