#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取上市公司财务利润表数据
"""


class Income(TuShareBase):
    def __init__(self):
        super(Income, self).__init__()
        """
        初始化
        """

        # 创建income数据集
        self.conn = DB_CONN['income']

    def crawl(self, ts_code, ann_date='', start_date='', end_date='', period='',report_type=1,comp_type=''):
        """
        params:ts_code  str Y   股票代码
        params:ann_date    str N   公告日期
        params:start_date  str N   报告期开始日期
        params:end_date    str N   报告期结束日期
        params:period  str N   报告期(每个季度最后一天的日期，比如20171231表示年报)
        params:report_type str N   报告类型： 参考下表说明
        params:comp_type   str N   公司类型：1一般工商业 2银行 3保险 4证券
主要报表类型说明

代码  类型  说明
1   合并报表    上市公司最新报表（默认）
2   单季合并    单一季度的合并报表
3   调整单季合并表 调整后的单季合并报表（如果有）
4   调整合并报表  本年度公布上年同期的财务报表数据，报告期为上年度
5   调整前合并报表 数据发生变更，将原数据进行保留，即调整前的原数据
6   母公司报表   该公司母公司的财务报表数据
7   母公司单季表  母公司的单季度表
8   母公司调整单季表    母公司调整后的单季表
9   母公司调整表  该公司母公司的本年度公布上年同期的财务报表数据
10  母公司调整前报表    母公司调整之前的原始财务报表数据
11  调整前合并报表 调整之前合并报表原数据
12  母公司调整前报表    母公司报表发生变更前保留的原数据
        :return:
        ts_code str TS股票代码
        ann_date    str 公告日期
        f_ann_date  str 实际公告日期，即发生过数据变更的最终日期
        end_date    str 报告期
        report_type str 报告类型： 参考下表说明
        comp_type   str 公司类型：1一般工商业 2银行 3保险 4证券
        basic_eps   float   基本每股收益
        diluted_eps float   稀释每股收益
        total_revenue   float   营业总收入 (元，下同)
        revenue float   营业收入
        int_income  float   利息收入
        prem_earned float   已赚保费
        comm_income float   手续费及佣金收入
        n_commis_income float   手续费及佣金净收入
        n_oth_income    float   其他经营净收益
        n_oth_b_income  float   加:其他业务净收益
        prem_income float   保险业务收入
        out_prem    float   减:分出保费
        une_prem_reser  float   提取未到期责任准备金
        reins_income    float   其中:分保费收入
        n_sec_tb_income float   代理买卖证券业务净收入
        n_sec_uw_income float   证券承销业务净收入
        n_asset_mg_income   float   受托客户资产管理业务净收入
        oth_b_income    float   其他业务收入
        fv_value_chg_gain   float   加:公允价值变动净收益
        invest_income   float   加:投资净收益
        ass_invest_income   float   其中:对联营企业和合营企业的投资收益
        forex_gain  float   加:汇兑净收益
        total_cogs  float   营业总成本
        oper_cost   float   减:营业成本
        int_exp float   减:利息支出
        comm_exp    float   减:手续费及佣金支出
        biz_tax_surchg  float   减:营业税金及附加
        sell_exp    float   减:销售费用
        admin_exp   float   减:管理费用
        fin_exp float   减:财务费用
        assets_impair_loss  float   减:资产减值损失
        prem_refund float   退保金
        compens_payout  float   赔付总支出
        reser_insur_liab    float   提取保险责任准备金
        div_payt    float   保户红利支出
        reins_exp   float   分保费用
        oper_exp    float   营业支出
        compens_payout_refu float   减:摊回赔付支出
        insur_reser_refu    float   减:摊回保险责任准备金
        reins_cost_refund   float   减:摊回分保费用
        other_bus_cost  float   其他业务成本
        operate_profit  float   营业利润
        non_oper_income float   加:营业外收入
        non_oper_exp    float   减:营业外支出
        nca_disploss    float   其中:减:非流动资产处置净损失
        total_profit    float   利润总额
        income_tax  float   所得税费用
        n_income    float   净利润(含少数股东损益)
        n_income_attr_p float   净利润(不含少数股东损益)
        minority_gain   float   少数股东损益
        oth_compr_income    float   其他综合收益
        t_compr_income  float   综合收益总额
        compr_inc_attr_p    float   归属于母公司(或股东)的综合收益总额
        compr_inc_attr_m_s  float   归属于少数股东的综合收益总额
        ebit    float   息税前利润
        ebitda  float   息税折旧摊销前利润
        insurance_exp   float   保险业务支出
        undist_profit   float   年初未分配利润
        distable_profit float   可分配利润
        """
        if not ts_code:
            return
        fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
        
        df = self.pro.query('income', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period,report_type=report_type,comp_type=comp_type, fields=fields)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','f_ann_date','end_date','report_type'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = Income()
    stock.crawl('000001.SZ')
