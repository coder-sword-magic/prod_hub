import os
import sys

import numpy as np
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from atch import open_handler

from tools.db_tools import DBT


def gen_open_freq_des(cal):
    fmt_l = '%Y.%-m.%-d'
    fmt_s = '%-m.%-d'    

    if type(cal['sub_period_start']) == float:
        print('sub_period_start',type(cal['sub_period_start']),cal['sub_period_start'])
        return cal['open_freq_des']

    open_freq_des = ''
    secondary_des = ''
    sub_avbl = []
    red_avbl = []
    bth_avlb = []
    
    op_st = min(cal['red_period_start'], cal['sub_period_start'])
    op_ed = max(cal['red_period_end'], cal['sub_period_end'])
    op_rng = pd.date_range(op_st, op_ed)
    sub_rng = pd.date_range(cal['sub_period_start'], cal['sub_period_end'])
    red_rng = pd.date_range(cal['red_period_start'], cal['red_period_end'])
    open_freq_des += op_st.strftime(fmt_l) + '-' + op_ed.strftime(fmt_s)

    for d in op_rng:
        if d in sub_rng and d in red_rng:
            bth_avlb.append(d)
        elif d in sub_rng and d not in red_rng:
            sub_avbl.append(d)
        elif d not in sub_rng and d in red_rng:
            red_avbl.append(d)

    if len(bth_avlb) == 1:
        secondary_des += bth_avlb[0].strftime(fmt_s) + '可申赎,'
    elif len(bth_avlb) > 1:
        secondary_des += bth_avlb[0].strftime(fmt_s) + '-' + bth_avlb[-1].strftime(fmt_s) + '可申赎,'
    if len(sub_avbl) == 1:
        secondary_des += sub_avbl[0].strftime(fmt_s) + '仅申购，'
    elif len(sub_avbl) > 1:
        secondary_des += sub_avbl[0].strftime(fmt_s) + '-' + sub_avbl[-1].strftime(fmt_s) + '仅申购，'
    if len(red_avbl) == 1:
        secondary_des += red_avbl[0].strftime(fmt_s) + '仅赎回，'
    elif len(red_avbl) > 1:
        secondary_des += red_avbl[0].strftime(fmt_s) + '-' + red_avbl[-1].strftime(fmt_s) + '仅赎回，'

    open_freq_des += "(" + secondary_des[:-1] + ")"
    
    return open_freq_des


def process_sn(df: pd.DataFrame):
    # 排序 [固收，权益，FOF]
    df['privilage'] = np.inf
    for i in df.index:
        if df.loc[i, 'pro_type'] == '固收':
            df.loc[i, 'privilage'] = 1
        elif df.loc[i, 'pro_type'] == '权益':
            df.loc[i, 'privilage'] = 2
        elif df.loc[i, 'pro_type'] == 'FOF':
            df.loc[i, 'privilage'] = 3
        else:
            df.loc[i, 'privilage'] = 4
    df = df.sort_values('privilage', ascending=True)
    # 加序号
    df['sn'] = 1
    df['sn'] = df['sn'].cumsum()

    return df


def gen_title(dbt: DBT,
              req_box: dict) -> str:
    """
    生成标题
    """
    box_date = parse(req_box['begin_time'])
    return f'资产管理总部{box_date.year}年{box_date.month}'


def gen_items(dbt: DBT,
              req_box: dict,
              ) -> dict:
    # 读取所有产品要素
    products = dbt.fetch_all_products()
    cal = pair_calendar(dbt, req_box, products)
    items = prepare_items(products, cal)
    return items


def prepare_items(products: pd.DataFrame,
                  cal: pd.DataFrame) -> list:
    # 产品要素与开放日历信息合并
    merged = pd.merge(products, cal, left_on='full_name', right_on='full_name', how='left')
    # 周度开放产品特殊处理
    week_open = merged[merged['open_freq'].str.contains('周')].copy()
    # 删除期限内没有开放的产品
    prepared = merged[~merged['sub_period_start'].isna()].copy()
    # 合并
    prepared = pd.concat([prepared, week_open], axis=0)
    # 处理序号
    prepared = process_sn(prepared)
    # 标记是否为同资产类别的第一行
    prepared['is_1st_line'] = np.where(prepared['pro_type'] != prepared['pro_type'].shift(1), True, False)
    # 计算个资产类别有多少开放的产品
    t = prepared.groupby('pro_type').aggregate({'short_name': np.count_nonzero}).reset_index().rename(
        columns={'short_name': 'rowspan'})
    # 讲资产类别的数目添加在表中
    prepared = pd.merge(prepared, t, left_on='pro_type', right_on='pro_type', how='left')
    # 添加开放期的描述
    prepared['next_open_des'] = prepared.apply(lambda x: gen_open_freq_des(x), axis=1)
    # 整型
    prepared = prepared.fillna(0)
    prepared['init_contri'] = prepared['init_contri'].astype(int)
    prepared['add_contri'] = prepared['add_contri'].astype(int)

    items = list(prepared.T.to_dict().values())
    return items


def pair_calendar(dbt: DBT,
                  req_box: dict,
                  df: pd.DataFrame,
                  ) -> pd.DataFrame:
    cal = pd.DataFrame([])
    for i in df.index:
        # 添加格式化的日期
        tmp = dbt.fetch_next_open(full_name=df.loc[i, 'full_name'],
                                  after_date=req_box['begin_time'],
                                  before_date=req_box['end_time'])
        if tmp is None:
            continue

        cal = pd.concat([cal, tmp], axis=0)

    return cal


def gen_file(tpl: str,
             title: str,
             items: list,
             file_name: str,
             path: str = '.'
             ) -> bool:
    # 生成模板
    try:
        tpl_env = Environment(loader=FileSystemLoader(path))
        template = tpl_env.get_template(tpl)
        file_path = os.path.join(path, file_name)
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(template.render(title=title, items=items))
    except Exception as e:
        raise Exception(e)


def main(req_id: str)->str:
    # 初始化数据库工具
    dbt = DBT()
    # 获得参数
    req_box: dict = dbt.fetch_box(req_id=req_id)

    from_date = parse(req_box['begin_time'])
    # 生成模板标题
    title = gen_title(dbt, req_box)
    # 生成参数
    items = gen_items(dbt, req_box)

    # 配置输出参数
    path = 'box'
    fn = title + '.xlsx'
    fn_path = os.path.join(path, fn)
    # 生成文件
    
    gen_file(tpl='product_preview.htm', title=title, items=items, file_name=fn, path=path)

    with open(fn_path, 'rb') as f:
        file_data = f.read()

    req_box = pd.DataFrame([req_box])
    req_box['file_name'] = fn
    req_box['file_data'] = file_data
    req_box['status'] = 'SUCCESS'
    dbt.upload_to_db(req_box, tb_name=dbt.box_tb, replace_by='id')

    # 生成附件
    for item in items:
        full_name:str=item['full_name']
        try:
            if isinstance(full_name,str) and full_name.endswith('资产管理计划'):
                print('开始生成附件:',full_name,'after date',from_date,type(from_date))
                open_handler.main(full_name=item['full_name'],after_date=from_date)
        except:
            continue

    return 'ok'


if __name__ == '__main__':
    if len(sys.argv) == 1:
        dbt = DBT()
        box = dbt.load_from_db(sql=f'''select * from {dbt.box_tb}''')
        print(box)
    else:
        req_id = sys.argv[1]
        # 读取参数范围
        main(req_id)
