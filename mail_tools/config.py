import os
from functools import reduce

mail_host = ''
mail_pass = ''
mail_user = ''
sender = ''
receiver = ''
db_engine = 'sqlite:///mail.db'
# gbk/gb2312/gb23012
encode_type = 'gbk'

""" 字段描述 """
# 虚拟净值描述
nvaf_set = {'虚拟净值', '虚拟净值提取后单位净值', '虚拟单位净值', '虚拟后净值','虚拟计提净值', '虚拟提取后净值', '计提后单位净值', '试算单位净值（扣除业绩报酬后）', '试算后单位净值','份额虚拟净值'}

# 累计单位净值描述
acc_nv_set = {'产品累计净值', '基金份额累计净值', '基金累计单位净值', '实际累计净值', '累计净值', '累计单位净值', '虚拟净值提取前单位净值', '虚拟净值提取前累计单位净值', '计提前累计净值',
              '试算前累计单位净值','试算前累计净值', '资产份额累计净值(元)', '基金份额累计净值(元)'}

# 单位净值描述
nvbf_set = {'产品单位净值', '今日单位净值', '净值', '单位净值', '基金份额净值', '基金单位净值', '实际净值', '虚拟净值提取前单位净值', '计提前单位净值', '试算前单位净值',
            '资产份额净值(元)', '基金份额净值(元)','最新单位净值'}

# fof名称描述
fof_set = {'客户名称', '投资者名称', 'TA账号名称', '客户姓名'}

# 基金名称描述
fund_set = {'基金名称','产品名称','资产名称'}

# 代码描述
code_set = {'基金代码','产品代码','产品ID','资产代码'}

# 管理人描述
mgr_set = {'管理人'}

# 净值日期描述
date_set = {'日期', '净值日期', '业务日期', '估值基准日', '基金净值日期','最新单位净值日期'}


# 份额日期描述
share_date_set = {'份额日期'}

# 持有份额描述
share_set = {'发生份额', '参与计提份额', '客户资产份额', '份额余额', '持仓份额', '计提份额', '投资者资产份额', '产品总份额', '资产份额(份)', '基金资产份额',
             '资产份额', '基金份额','持有份额'} # 持有份额放在产品总份额后面

# 业绩报酬描述
carry_set = {'试算业绩报酬', '虚拟业绩报酬', '虚拟计提金额'}

# fof产品名称
fof_name_set = {}



''' 读取的excel和导出的excel的格式 '''
input_col_origin=('FOF产品名称', '产品代码','估值日期', '私募代码', '私募名称','收市价','份额')
input_col_trans=('fof','fof_code','date','code','fund_name','nv','share')

def input_col():
    return dict(zip(input_col_origin, input_col_trans))

def output_col():
    return dict(zip(input_col_trans, input_col_origin))


""" 过滤器 """

# 无记录筛选过滤器
global_filter = {
                'XX123X':[{'col':'src_file','val':'XY2439'}],
                 }

# 多条记录筛选过滤器
cus_filter ={
             'XXX123':[{'col':'src_file','val':'虚拟计提业绩报'}],
             }

# 通用过滤器
common_filter = [{'col':'src_file','val':'TA虚拟净值'}]

# 聚合函数过滤器
agg_func = {
            'XX123X':{'nvbf':lambda x:list(x)[0],'share':lambda x:reduce(lambda y,z:str(float(y)+float(z)),x),'nvaf':lambda x:list(x)[0]},
            'XXX123':{'nvbf':lambda x:max(x),'nvaf':lambda x:max(x),'share':lambda x:max(x)},
            }