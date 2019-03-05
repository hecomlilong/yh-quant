#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取上市公司财务指标数据，为避免服务器压力，现阶段每次请求最多返回60条记录，可通过设置日期多次请求获取更多数据。
"""


class FinaIndicator(TuShareBase):
    def __init__(self):
        super(FinaIndicator, self).__init__()
        """
        初始化
        """

        # 创建fina_indicator数据集
        self.conn = DB_CONN['fina_indicator']

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
        eps float   基本每股收益
        dt_eps  float   稀释每股收益
        total_revenue_ps    float   每股营业总收入
        revenue_ps  float   每股营业收入
        capital_rese_ps float   每股资本公积
        surplus_rese_ps float   每股盈余公积
        undist_profit_ps    float   每股未分配利润
        extra_item  float   非经常性损益
        profit_dedt float   扣除非经常性损益后的净利润
        gross_margin    float   毛利
        current_ratio   float   流动比率
        quick_ratio float   速动比率
        cash_ratio  float   保守速动比率
        invturn_days    float   存货周转天数
        arturn_days float   应收账款周转天数
        inv_turn    float   存货周转率
        ar_turn float   应收账款周转率
        ca_turn float   流动资产周转率
        fa_turn float   固定资产周转率
        assets_turn float   总资产周转率
        op_income   float   经营活动净收益
        valuechange_income  float   价值变动净收益
        interst_income  float   利息费用
        daa float   折旧与摊销
        ebit    float   息税前利润
        ebitda  float   息税折旧摊销前利润
        fcff    float   企业自由现金流量
        fcfe    float   股权自由现金流量
        current_exint   float   无息流动负债
        noncurrent_exint    float   无息非流动负债
        interestdebt    float   带息债务
        netdebt float   净债务
        tangible_asset  float   有形资产
        working_capital float   营运资金
        networking_capital  float   营运流动资本
        invest_capital  float   全部投入资本
        retained_earnings   float   留存收益
        diluted2_eps    float   期末摊薄每股收益
        bps float   每股净资产
        ocfps   float   每股经营活动产生的现金流量净额
        retainedps  float   每股留存收益
        cfps    float   每股现金流量净额
        ebit_ps float   每股息税前利润
        fcff_ps float   每股企业自由现金流量
        fcfe_ps float   每股股东自由现金流量
        netprofit_margin    float   销售净利率
        grossprofit_margin  float   销售毛利率
        cogs_of_sales   float   销售成本率
        expense_of_sales    float   销售期间费用率
        profit_to_gr    float   净利润/营业总收入
        saleexp_to_gr   float   销售费用/营业总收入
        adminexp_of_gr  float   管理费用/营业总收入
        finaexp_of_gr   float   财务费用/营业总收入
        impai_ttm   float   资产减值损失/营业总收入
        gc_of_gr    float   营业总成本/营业总收入
        op_of_gr    float   营业利润/营业总收入
        ebit_of_gr  float   息税前利润/营业总收入
        roe float   净资产收益率
        roe_waa float   加权平均净资产收益率
        roe_dt  float   净资产收益率(扣除非经常损益)
        roa float   总资产报酬率
        npta    float   总资产净利润
        roic    float   投入资本回报率
        roe_yearly  float   年化净资产收益率
        roa2_yearly float   年化总资产报酬率
        roe_avg float   平均净资产收益率(增发条件)
        opincome_of_ebt float   经营活动净收益/利润总额
        investincome_of_ebt float   价值变动净收益/利润总额
        n_op_profit_of_ebt  float   营业外收支净额/利润总额
        tax_to_ebt  float   所得税/利润总额
        dtprofit_to_profit  float   扣除非经常损益后的净利润/净利润
        salescash_to_or float   销售商品提供劳务收到的现金/营业收入
        ocf_to_or   float   经营活动产生的现金流量净额/营业收入
        ocf_to_opincome float   经营活动产生的现金流量净额/经营活动净收益
        capitalized_to_da   float   资本支出/折旧和摊销
        debt_to_assets  float   资产负债率
        assets_to_eqt   float   权益乘数
        dp_assets_to_eqt    float   权益乘数(杜邦分析)
        ca_to_assets    float   流动资产/总资产
        nca_to_assets   float   非流动资产/总资产
        tbassets_to_totalassets float   有形资产/总资产
        int_to_talcap   float   带息债务/全部投入资本
        eqt_to_talcapital   float   归属于母公司的股东权益/全部投入资本
        currentdebt_to_debt float   流动负债/负债合计
        longdeb_to_debt float   非流动负债/负债合计
        ocf_to_shortdebt    float   经营活动产生的现金流量净额/流动负债
        debt_to_eqt float   产权比率
        eqt_to_debt float   归属于母公司的股东权益/负债合计
        eqt_to_interestdebt float   归属于母公司的股东权益/带息债务
        tangibleasset_to_debt   float   有形资产/负债合计
        tangasset_to_intdebt    float   有形资产/带息债务
        tangibleasset_to_netdebt    float   有形资产/净债务
        ocf_to_debt float   经营活动产生的现金流量净额/负债合计
        ocf_to_interestdebt float   经营活动产生的现金流量净额/带息债务
        ocf_to_netdebt  float   经营活动产生的现金流量净额/净债务
        ebit_to_interest    float   已获利息倍数(EBIT/利息费用)
        longdebt_to_workingcapital  float   长期债务与营运资金比率
        ebitda_to_debt  float   息税折旧摊销前利润/负债合计
        turn_days   float   营业周期
        roa_yearly  float   年化总资产净利率
        roa_dp  float   总资产净利率(杜邦分析)
        fixed_assets    float   固定资产合计
        profit_prefin_exp   float   扣除财务费用前营业利润
        non_op_profit   float   非营业利润
        op_to_ebt   float   营业利润／利润总额
        nop_to_ebt  float   非营业利润／利润总额
        ocf_to_profit   float   经营活动产生的现金流量净额／营业利润
        cash_to_liqdebt float   货币资金／流动负债
        cash_to_liqdebt_withinterest    float   货币资金／带息流动负债
        op_to_liqdebt   float   营业利润／流动负债
        op_to_debt  float   营业利润／负债合计
        roic_yearly float   年化投入资本回报率
        profit_to_op    float   利润总额／营业收入
        q_opincome  float   经营活动单季度净收益
        q_investincome  float   价值变动单季度净收益
        q_dtprofit  float   扣除非经常损益后的单季度净利润
        q_eps   float   每股收益(单季度)
        q_netprofit_margin  float   销售净利率(单季度)
        q_gsprofit_margin   float   销售毛利率(单季度)
        q_exp_to_sales  float   销售期间费用率(单季度)
        q_profit_to_gr  float   净利润／营业总收入(单季度)
        q_saleexp_to_gr float   销售费用／营业总收入 (单季度)
        q_adminexp_to_gr    float   管理费用／营业总收入 (单季度)
        q_finaexp_to_gr float   财务费用／营业总收入 (单季度)
        q_impair_to_gr_ttm  float   资产减值损失／营业总收入(单季度)
        q_gc_to_gr  float   营业总成本／营业总收入 (单季度)
        q_op_to_gr  float   营业利润／营业总收入(单季度)
        q_roe   float   净资产收益率(单季度)
        q_dt_roe    float   净资产单季度收益率(扣除非经常损益)
        q_npta  float   总资产净利润(单季度)
        q_opincome_to_ebt   float   经营活动净收益／利润总额(单季度)
        q_investincome_to_ebt   float   价值变动净收益／利润总额(单季度)
        q_dtprofit_to_profit    float   扣除非经常损益后的净利润／净利润(单季度)
        q_salescash_to_or   float   销售商品提供劳务收到的现金／营业收入(单季度)
        q_ocf_to_sales  float   经营活动产生的现金流量净额／营业收入(单季度)
        q_ocf_to_or float   经营活动产生的现金流量净额／经营活动净收益(单季度)
        basic_eps_yoy   float   基本每股收益同比增长率(%)
        dt_eps_yoy  float   稀释每股收益同比增长率(%)
        cfps_yoy    float   每股经营活动产生的现金流量净额同比增长率(%)
        op_yoy  float   营业利润同比增长率(%)
        ebt_yoy float   利润总额同比增长率(%)
        netprofit_yoy   float   归属母公司股东的净利润同比增长率(%)
        dt_netprofit_yoy    float   归属母公司股东的净利润-扣除非经常损益同比增长率(%)
        ocf_yoy float   经营活动产生的现金流量净额同比增长率(%)
        roe_yoy float   净资产收益率(摊薄)同比增长率(%)
        bps_yoy float   每股净资产相对年初增长率(%)
        assets_yoy  float   资产总计相对年初增长率(%)
        eqt_yoy float   归属母公司的股东权益相对年初增长率(%)
        tr_yoy  float   营业总收入同比增长率(%)
        or_yoy  float   营业收入同比增长率(%)
        q_gr_yoy    float   营业总收入同比增长率(%)(单季度)
        q_gr_qoq    float   营业总收入环比增长率(%)(单季度)
        q_sales_yoy float   营业收入同比增长率(%)(单季度)
        q_sales_qoq float   营业收入环比增长率(%)(单季度)
        q_op_yoy    float   营业利润同比增长率(%)(单季度)
        q_op_qoq    float   营业利润环比增长率(%)(单季度)
        q_profit_yoy    float   净利润同比增长率(%)(单季度)
        q_profit_qoq    float   净利润环比增长率(%)(单季度)
        q_netprofit_yoy float   归属母公司股东的净利润同比增长率(%)(单季度)
        q_netprofit_qoq float   归属母公司股东的净利润环比增长率(%)(单季度)
        equity_yoy  float   净资产同比增长率
        rd_exp  float   研发费用
        """
        if not ts_code:
            return
        
        df = self.pro.query('fina_indicator', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','ann_date','end_date'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = FinaIndicator()
    stock.crawl('000001.SZ')
