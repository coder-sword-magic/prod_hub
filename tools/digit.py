def to_chinese_numeral(num):
    chinese_numerals = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九']
    chinese_unit = ['','十', '百', '千', '万', '亿']
    result = ''
    num_str = str(num)
    num_len = len(num_str)

    for i in range(num_len):
        j=int(num_str[i])
        if j!=0:
            result +=chinese_numerals[j]+chinese_unit[num_len-i-1]
        else:
            if num_len-i-1 == 4 or (i ==num_len-1 and result[-1]!=chinese_numerals[0]):
                result += chinese_unit[num_len-i-1]

    if result.startswith('一十'):
        result = result[1:]
    return result


def chinese_to_arabic(chinese_num: str) -> int:
    chinese_num_dict = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
    chinese_unit_dict = {'十': 10, '百': 100, '千': 1000, '万': 10000, '亿': 100000000}
    arabic_num = 0
    unit = 1
    for char in chinese_num[::-1]:
        if char in chinese_unit_dict:
            unit = chinese_unit_dict[char]
            if unit >= 10000:
                arabic_num += unit
                unit = 1
        else:
            digit = chinese_num_dict[char]
            arabic_num += digit * unit
    return arabic_num