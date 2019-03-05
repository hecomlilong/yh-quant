#  -*- coding: utf-8 -*-

from utils.database import DB_CONN
from utils.tushare_base import TuShareBase
from utils.stock_util import get_all_codes_pro

"""
描述：获取上市公司现金流量表
"""


class CashFlow(TuShareBase):
    def __init__(self):
        super(CashFlow, self).__init__()
        """
        初始化
        """

        # 创建cashflow数据集
        self.conn = DB_CONN['cashflow']

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
        comp_type   str 公司类型：1一般工商业 2银行 3保险 4证券
        report_type str 报表类型：见下方详细说明
        net_profit  float   净利润 (元，下同)
        finan_exp   float   财务费用
        c_fr_sale_sg    float   销售商品、提供劳务收到的现金
        recp_tax_rends  float   收到的税费返还
        n_depos_incr_fi float   客户存款和同业存放款项净增加额
        n_incr_loans_cb float   向中央银行借款净增加额
        n_inc_borr_oth_fi   float   向其他金融机构拆入资金净增加额
        prem_fr_orig_contr  float   收到原保险合同保费取得的现金
        n_incr_insured_dep  float   保户储金净增加额
        n_reinsur_prem  float   收到再保业务现金净额
        n_incr_disp_tfa float   处置交易性金融资产净增加额
        ifc_cash_incr   float   收取利息和手续费净增加额
        n_incr_disp_faas    float   处置可供出售金融资产净增加额
        n_incr_loans_oth_bank   float   拆入资金净增加额
        n_cap_incr_repur    float   回购业务资金净增加额
        c_fr_oth_operate_a  float   收到其他与经营活动有关的现金
        c_inf_fr_operate_a  float   经营活动现金流入小计
        c_paid_goods_s  float   购买商品、接受劳务支付的现金
        c_paid_to_for_empl  float   支付给职工以及为职工支付的现金
        c_paid_for_taxes    float   支付的各项税费
        n_incr_clt_loan_adv float   客户贷款及垫款净增加额
        n_incr_dep_cbob float   存放央行和同业款项净增加额
        c_pay_claims_orig_inco  float   支付原保险合同赔付款项的现金
        pay_handling_chrg   float   支付手续费的现金
        pay_comm_insur_plcy float   支付保单红利的现金
        oth_cash_pay_oper_act   float   支付其他与经营活动有关的现金
        st_cash_out_act float   经营活动现金流出小计
        n_cashflow_act  float   经营活动产生的现金流量净额
        oth_recp_ral_inv_act    float   收到其他与投资活动有关的现金
        c_disp_withdrwl_invest  float   收回投资收到的现金
        c_recp_return_invest    float   取得投资收益收到的现金
        n_recp_disp_fiolta  float   处置固定资产、无形资产和其他长期资产收回的现金净额
        n_recp_disp_sobu    float   处置子公司及其他营业单位收到的现金净额
        stot_inflows_inv_act    float   投资活动现金流入小计
        c_pay_acq_const_fiolta  float   购建固定资产、无形资产和其他长期资产支付的现金
        c_paid_invest   float   投资支付的现金
        n_disp_subs_oth_biz float   取得子公司及其他营业单位支付的现金净额
        oth_pay_ral_inv_act float   支付其他与投资活动有关的现金
        n_incr_pledge_loan  float   质押贷款净增加额
        stot_out_inv_act    float   投资活动现金流出小计
        n_cashflow_inv_act  float   投资活动产生的现金流量净额
        c_recp_borrow   float   取得借款收到的现金
        proc_issue_bonds    float   发行债券收到的现金
        oth_cash_recp_ral_fnc_act   float   收到其他与筹资活动有关的现金
        stot_cash_in_fnc_act    float   筹资活动现金流入小计
        free_cashflow   float   企业自由现金流量
        c_prepay_amt_borr   float   偿还债务支付的现金
        c_pay_dist_dpcp_int_exp float   分配股利、利润或偿付利息支付的现金
        incl_dvd_profit_paid_sc_ms  float   其中:子公司支付给少数股东的股利、利润
        oth_cashpay_ral_fnc_act float   支付其他与筹资活动有关的现金
        stot_cashout_fnc_act    float   筹资活动现金流出小计
        n_cash_flows_fnc_act    float   筹资活动产生的现金流量净额
        eff_fx_flu_cash float   汇率变动对现金的影响
        n_incr_cash_cash_equ    float   现金及现金等价物净增加额
        c_cash_equ_beg_period   float   期初现金及现金等价物余额
        c_cash_equ_end_period   float   期末现金及现金等价物余额
        c_recp_cap_contrib  float   吸收投资收到的现金
        incl_cash_rec_saims float   其中:子公司吸收少数股东投资收到的现金
        uncon_invest_loss   float   未确认投资损失
        prov_depr_assets    float   加:资产减值准备
        depr_fa_coga_dpba   float   固定资产折旧、油气资产折耗、生产性生物资产折旧
        amort_intang_assets float   无形资产摊销
        lt_amort_deferred_exp   float   长期待摊费用摊销
        decr_deferred_exp   float   待摊费用减少
        incr_acc_exp    float   预提费用增加
        loss_disp_fiolta    float   处置固定、无形资产和其他长期资产的损失
        loss_scr_fa float   固定资产报废损失
        loss_fv_chg float   公允价值变动损失
        invest_loss float   投资损失
        decr_def_inc_tax_assets float   递延所得税资产减少
        incr_def_inc_tax_liab   float   递延所得税负债增加
        decr_inventories    float   存货的减少
        decr_oper_payable   float   经营性应收项目的减少
        incr_oper_payable   float   经营性应付项目的增加
        others  float   其他
        im_net_cashflow_oper_act    float   经营活动产生的现金流量净额(间接法)
        conv_debt_into_cap  float   债务转为资本
        conv_copbonds_due_within_1y float   一年内到期的可转换公司债券
        fa_fnc_leases   float   融资租入固定资产
        end_bal_cash    float   现金的期末余额
        beg_bal_cash    float   减:现金的期初余额
        end_bal_cash_equ    float   加:现金等价物的期末余额
        beg_bal_cash_equ    float   减:现金等价物的期初余额
        im_n_incr_cash_equ  float   现金及现金等价物净增加额(间接法)
        """
        if not ts_code:
            return
        # fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
        
        df = self.pro.query('cashflow', ts_code=ts_code,ann_date=ann_date,start_date=start_date,end_date=end_date,period=period,report_type=report_type,comp_type=comp_type)
        self.save_data(df=df, collection=self.conn, filter_fields=['ts_code','f_ann_date','end_date','report_type'])

    def crawl_all(self):
        codes = get_all_codes_pro()
        for code in codes:
            self.crawl(ts_code=code)


if __name__ == '__main__':
    stock = CashFlow()
    stock.crawl('000001.SZ')
