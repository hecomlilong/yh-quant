#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取上市公司资产负债表
"""


class BalanceSheet(TuShareBase):
    def __init__(self):
        super(BalanceSheet, self).__init__()
        """
        初始化
        """

        # 创建balancesheet数据集
        self.conn = DB_CONN['balancesheet']

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
        f_ann_date  str 实际公告日期
        end_date    str 报告期
        report_type str 报表类型：见下方详细说明
        comp_type   str 公司类型：1一般工商业 2银行 3保险 4证券
        total_share float   期末总股本
        cap_rese    float   资本公积金 (元，下同)
        undistr_porfit  float   未分配利润
        surplus_rese    float   盈余公积金
        special_rese    float   专项储备
        money_cap   float   货币资金
        trad_asset  float   交易性金融资产
        notes_receiv    float   应收票据
        accounts_receiv float   应收账款
        oth_receiv  float   其他应收款
        prepayment  float   预付款项
        div_receiv  float   应收股利
        int_receiv  float   应收利息
        inventories float   存货
        amor_exp    float   长期待摊费用
        nca_within_1y   float   一年内到期的非流动资产
        sett_rsrv   float   结算备付金
        loanto_oth_bank_fi  float   拆出资金
        premium_receiv  float   应收保费
        reinsur_receiv  float   应收分保账款
        reinsur_res_receiv  float   应收分保合同准备金
        pur_resale_fa   float   买入返售金融资产
        oth_cur_assets  float   其他流动资产
        total_cur_assets    float   流动资产合计
        fa_avail_for_sale   float   可供出售金融资产
        htm_invest  float   持有至到期投资
        lt_eqt_invest   float   长期股权投资
        invest_real_estate  float   投资性房地产
        time_deposits   float   定期存款
        oth_assets  float   其他资产
        lt_rec  float   长期应收款
        fix_assets  float   固定资产
        cip float   在建工程
        const_materials float   工程物资
        fixed_assets_disp   float   固定资产清理
        produc_bio_assets   float   生产性生物资产
        oil_and_gas_assets  float   油气资产
        intan_assets    float   无形资产
        r_and_d float   研发支出
        goodwill    float   商誉
        lt_amor_exp float   长期待摊费用
        defer_tax_assets    float   递延所得税资产
        decr_in_disbur  float   发放贷款及垫款
        oth_nca float   其他非流动资产
        total_nca   float   非流动资产合计
        cash_reser_cb   float   现金及存放中央银行款项
        depos_in_oth_bfi    float   存放同业和其它金融机构款项
        prec_metals float   贵金属
        deriv_assets    float   衍生金融资产
        rr_reins_une_prem   float   应收分保未到期责任准备金
        rr_reins_outstd_cla float   应收分保未决赔款准备金
        rr_reins_lins_liab  float   应收分保寿险责任准备金
        rr_reins_lthins_liab    float   应收分保长期健康险责任准备金
        refund_depos    float   存出保证金
        ph_pledge_loans float   保户质押贷款
        refund_cap_depos    float   存出资本保证金
        indep_acct_assets   float   独立账户资产
        client_depos    float   其中：客户资金存款
        client_prov float   其中：客户备付金
        transac_seat_fee    float   其中:交易席位费
        invest_as_receiv    float   应收款项类投资
        total_assets    float   资产总计
        lt_borr float   长期借款
        st_borr float   短期借款
        cb_borr float   向中央银行借款
        depos_ib_deposits   float   吸收存款及同业存放
        loan_oth_bank   float   拆入资金
        trading_fl  float   交易性金融负债
        notes_payable   float   应付票据
        acct_payable    float   应付账款
        adv_receipts    float   预收款项
        sold_for_repur_fa   float   卖出回购金融资产款
        comm_payable    float   应付手续费及佣金
        payroll_payable float   应付职工薪酬
        taxes_payable   float   应交税费
        int_payable float   应付利息
        div_payable float   应付股利
        oth_payable float   其他应付款
        acc_exp float   预提费用
        deferred_inc    float   递延收益
        st_bonds_payable    float   应付短期债券
        payable_to_reinsurer    float   应付分保账款
        rsrv_insur_cont float   保险合同准备金
        acting_trading_sec  float   代理买卖证券款
        acting_uw_sec   float   代理承销证券款
        non_cur_liab_due_1y float   一年内到期的非流动负债
        oth_cur_liab    float   其他流动负债
        total_cur_liab  float   流动负债合计
        bond_payable    float   应付债券
        lt_payable  float   长期应付款
        specific_payables   float   专项应付款
        estimated_liab  float   预计负债
        defer_tax_liab  float   递延所得税负债
        defer_inc_non_cur_liab  float   递延收益-非流动负债
        oth_ncl float   其他非流动负债
        total_ncl   float   非流动负债合计
        depos_oth_bfi   float   同业和其它金融机构存放款项
        deriv_liab  float   衍生金融负债
        depos   float   吸收存款
        agency_bus_liab float   代理业务负债
        oth_liab    float   其他负债
        prem_receiv_adva    float   预收保费
        depos_received  float   存入保证金
        ph_invest   float   保户储金及投资款
        reser_une_prem  float   未到期责任准备金
        reser_outstd_claims float   未决赔款准备金
        reser_lins_liab float   寿险责任准备金
        reser_lthins_liab   float   长期健康险责任准备金
        indept_acc_liab float   独立账户负债
        pledge_borr float   其中:质押借款
        indem_payable   float   应付赔付款
        policy_div_payable  float   应付保单红利
        total_liab  float   负债合计
        treasury_share  float   减:库存股
        ordin_risk_reser    float   一般风险准备
        forex_differ    float   外币报表折算差额
        invest_loss_unconf  float   未确认的投资损失
        minority_int    float   少数股东权益
        total_hldr_eqy_exc_min_int  float   股东权益合计(不含少数股东权益)
        total_hldr_eqy_inc_min_int  float   股东权益合计(含少数股东权益)
        total_liab_hldr_eqy float   负债及股东权益总计
        lt_payroll_payable  float   长期应付职工薪酬
        oth_comp_income float   其他综合收益
        oth_eqt_tools   float   其他权益工具
        oth_eqt_tools_p_shr float   其他权益工具(优先股)
        lending_funds   float   融出资金
        acc_receivable  float   应收款项
        st_fin_payable  float   应付短期融资款
        payables    float   应付款项
        hfs_assets  float   持有待售的资产
        hfs_sales   float   持有待售的负债
        """
        if not ts_code:
            return
        # fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
        
        df = self.pro.query('balancesheet', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period,report_type=report_type,comp_type=comp_type)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','f_ann_date','end_date','report_type'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = BalanceSheet()
    stock.crawl('000001.SZ')
