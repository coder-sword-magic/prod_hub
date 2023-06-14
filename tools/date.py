from dateutil import parser,relativedelta
from datetime import datetime, timedelta

from typing import Union
import re
import pandas as pd


def parse_date(date_str: Union[str, datetime]):
    if isinstance(date_str, str):
        date_str = date_str.strip()
        date_str = re.sub(r'[年月日]', '', date_str)
        return parser.parse(date_str).date()
    elif isinstance(date_str,datetime):
        return date_str.date()
    elif isinstance(date_str,pd.Timestamp):
        return date_str.date()
    else:
        return date_str


def tr_date_range(from_date:Union[str,datetime], 
                  to_date:Union[str,datetime]):
    """
    返回交易日区间
    """
    from_date,to_date = parse_date(from_date),parse_date(to_date)
    df = pd.read_csv('./caldr/trading_calendar.csv',
                     index_col=0, parse_dates=[0])
    return df[from_date: to_date].index

def count_days_by_month(from_date:Union[str,datetime], 
                        month:int):
    """
    返回从from_date到to_date的月数
    """
    from_date = parse_date(from_date)
    to_date = (from_date + relativedelta.relativedelta(months=month))
    return (to_date - from_date).days

def date_after_month(from_date:Union[str,datetime], 
                     month:int):
    """
    返回from_date后的月数
    """
    from_date = parse_date(from_date)
    return (from_date + relativedelta.relativedelta(months=month)).date()

def next_tr_date(from_date:Union[str,datetime], # 起始日期
                 inter_days:int=1               # 后面的天数
                 ):
    """
    返回下一个交易日
    """
    df = pd.read_csv('./caldr/trading_calendar.csv',
                     index_col=0, parse_dates=[0])
    return df[from_date:].index[inter_days]

def prev_tr_date(from_date:Union[str,datetime], # 过去日期
                 inter_days:int=1               # 后面的天数
                 ):
    """
    返回上一个交易日
    """
    df = pd.read_csv('./caldr/trading_calendar.csv',
                     index_col=0, parse_dates=[0])
    return df[:from_date].index[-inter_days-1]