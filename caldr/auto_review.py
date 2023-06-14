import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td
from tools.db_tools import DBT
from atch import open_handler
import sys
import secrets

def trans_freq(open_freq):
    if open_freq == '月':
        return 'M'
    elif open_freq == '周':
        return 'W'
    elif open_freq == '年':
        return 'Y'
    else:
        freq = trans_freq(open_freq[-1])
        freq_num = open_freq[:-1].replace('个','')
        return freq_num+freq
    

def gen_calendar(product,dbt,trading_cal,lagged_days=600)->pd.DataFrame:

    # 不生成周度开放的产品
    open_freq=product['open_freq']
    if '周' in open_freq:
        return None

    def tr_day(date,interval=1):
        "不能用dt.now(),因为对比小时的时间戳"
        return trading_cal[date:].iloc[interval-1].name.date()
    
    full_name = product['full_name']
    # 将年月周调整为Y，M，W
    freq = trans_freq(open_freq)
    cal=dbt.fetch_all_calendar(full_name)

    # todo:不支持申购赎回周期不同的产品
    last_start = max(cal.iloc[-1]['red_period_start'],cal.iloc[-1]['sub_period_start'])
    sub_date=int(product['sub_date'])
    sub_days=int(product['sub_days'])
    red_date=int(product['red_date'])
    red_days=int(product['red_days'])
    range_end=dt.now().date()+td(days=lagged_days) # 默认：以今天为起始计算一年后
    rng=pd.date_range(last_start,range_end,freq=freq)[1:]
    
    # 默认记录已经长达一年
    if rng.empty:
        return None

    prepared = rng.to_frame(name='st').reset_index(drop=True)
    prepared['month']=prepared['st'].dt.month
    prepared['year']=prepared['st'].dt.year
    prepared['sub_period_start']=prepared.apply(lambda x:str(tr_day(dt(x['year'],x['month'],sub_date).date())).replace('-','/'),axis=1)
    prepared['sub_period_end']=prepared['sub_period_start'].apply(lambda x:str(tr_day(x,sub_days)).replace('-','/'))
    prepared['red_period_start']=prepared.apply(lambda x:str(tr_day(dt(x['year'],x['month'],red_date).date())).replace('-','/'),axis=1)
    prepared['red_period_end']=prepared['red_period_start'].apply(lambda x:str(tr_day(x,red_days)).replace('-','/'))
    prepared['id']=prepared.apply(lambda x:secrets.token_hex(16),axis=1)
    prepared['doc_date']=str(dt.now().date()).replace('-','/')
    prepared['full_name']=full_name
    prepared['publiser_name']='自动生成'
    prepared['publisher_mail']=''
    prepared['type']='开放公告'

    return prepared[cal.columns]

def main():
    trading_cal = pd.read_csv('./caldr/trading_calendar.csv',
                              index_col=0, parse_dates=[0])

    dbt=DBT()
    all_product=dbt.fetch_all_products()
    
    # 测试阶段用
    if all_product.empty:
        sys.exit()
    
    # 
    for i,product in all_product.iterrows():
        
        # 生成产品日历
        updated = gen_calendar(product,dbt,trading_cal)
        if updated is not None:
            # 插入数据库
            dbt.update_cal_db(updated)


if __name__ == '__main__':
    # 读取交易日历
    main()