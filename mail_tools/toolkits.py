import re
from dateutil import parser
from decimal import Decimal


def strQ2B(ustring):
    """
    全角转半角
    """
    if not isinstance(ustring, str):
        return

    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:  # 全角空格直接转换
            inside_code = 32
        elif 65281 <= inside_code <= 65374:  # 全角字符（除空格）根据关系转化
            inside_code -= 65248
        rstring += chr(inside_code)
    return rstring


def extract_date(x):
    """
    提取时间
    """
    has_date = re.search(r'20\w{2}[-_年]?[01]{1}[0-9]{1}[-_月]?[0123]{1}[0-9]{1}[日]?', x)

    if has_date:
        date = re.sub(r'[年|月|日|-|_]', '', has_date.group())
        return parser.parse(date)
    return None


def to_decimal(x):
    x = re.sub(r',|，|nan', '', str(x).lower())
    return str(float(x))

def is_decimal(x):
    if x is None:
        return False
    try:
        x = re.sub(r',|，|nan', '', str(x).lower())
        float(x)
        return True
    except ValueError:
        return False

def is_non_zero_decimal(x):
    if x is None:
        return False
    elif isinstance(x, int) and x==0:
        return False
    elif isinstance(x,float) and x==0:
        return False
    elif isinstance(x,str) and x=='0':
        return False
    try:
        x = re.sub(r',|，|nan', '', str(x).lower())
        float(x)
        return True
    except ValueError:
        return False
