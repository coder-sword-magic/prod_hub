import numpy as np
import pandas as pd
import re
from typing import Optional
from .config import nvaf_set, nvbf_set, fof_set, share_set, date_set, carry_set, acc_nv_set,fund_set,code_set,share_date_set
from .toolkits import strQ2B, is_non_zero_decimal, to_decimal,is_decimal
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

class Base(DeclarativeBase):
    pass

class FOF_tb(Base):
    __tablename__ = 'fof_tb'
    file_name:Mapped[str] = mapped_column(primary_key=True) 
    subject:Mapped[str] = mapped_column(nullable=True)
    fund_name:Mapped[str] = mapped_column(nullable=True)
    fof_name:Mapped[str] = mapped_column(nullable=True)
    fof_code:Mapped[str] = mapped_column(nullable=True)
    nvbf:Mapped[str] = mapped_column(nullable=True)
    nvaf:Mapped[str] = mapped_column(nullable=True)
    acc_nv:Mapped[str] = mapped_column(nullable=True)
    share:Mapped[str] = mapped_column(nullable=True)
    carry:Mapped[str] = mapped_column(nullable=True)
    date:Mapped[datetime] = mapped_column(nullable=True)
    share_date:Mapped[datetime] = mapped_column(nullable=True)


class ExcelReader:

    def __init__(self,
                file_name:str,
                subject:str,
                raw:bytes,
                 ):
        self.file_name = strQ2B(file_name)
        self.subject = strQ2B(subject)
        self.raw:pd.DataFrame = pd.read_excel(raw)

        # 表中的字段描述
        self.fund_desc:str = ''   # 基金名称描述
        self.code_desc:str = ''   # 基金代码描述
        self.mgr_desc:str = ''    # 管理人描述
        self.nvbf_desc:str = ''   # 单位净值描述 
        self.nvaf_desc:str = ''   # 虚拟净值描述 
        self.acc_nv_desc:str = '' # 累计净值表述
        self.share_desc:str = ''  # 持有份额描述
        self.fof_desc:str = ''    # fof名称描述
        self.fof_code_desc:str = '' # fof代码描述
        self.carry_desc:str = ''  # 业绩报酬描述
        self.date_desc:str = ''   # 净值日期描述 
        self.share_date_desc:str = '' # 持有份数日期描述
        
        # patterns
        self.code_pattern = r'[a-zA-Z]{2}[\w\d][\w\d][\d][\w\d]' # 前面两个是字母，倒数第5个是数字
        # self.fund_pattern = r'基金|私募证券投资基金|单一资产管理计划|集合资金管理计划|（|）|A类|B类|份额|-|改名|'
        self.fund_pattern = r'\w*(投资基金|私募证券投资基金|单一资产管理计划|集合资金管理计划)[A-Z]?类?'
        self.fof_pattern = r'.*资产管理计划'
        self.date_pattern = r'\d{4}-\d{2}-\d{2}|20\d{2}(01|02|03|04|05|06|07|08|09|10|11|12)[0123]\d{1}'
        self.col_pattern = r'\s|:|：|\r|\n'

        # 主题和文件名可能包含的字段
        self.fund_name = ''
        self.code = ''
        self.fof_name = ''
        self.fof_code = ''
        self.date = ''

        # 整理excel
        self.df = self.reset_df_col(self.raw)
        self.parse_str(self.subject)
        self.parse_str(self.file_name)
    
    def parse_str(self,text):
        text = strQ2B(text) # 转为半角
        text = re.sub(r'[\[\]【】\(\)（）]','',text) # 删除括号
        text = re.sub(r'年|月|日','',text) # 删除年月日
        if '_' in text:
            parts = text.split('_')
            for part in parts:
                if re.search(self.code_pattern,part):
                    self.code = re.search(self.code_pattern,part).group() if self.code == '' else self.code
                elif re.search(self.fof_pattern,part): 
                    self.fof_name = re.search(self.fof_pattern,part).group() if self.fof_name == '' else self.fof_name
                # todo 可能会将fof名称误判
                elif re.search(self.fund_pattern,part): 
                    self.fund_name = re.search(self.fund_pattern,part).group() if self.fund_name == '' else self.fund_name
                elif re.search(self.date_pattern,part):
                    self.date = pd.to_datetime(re.search(self.date_pattern,part).group()) if self.date == '' else self.date
        else:
            if re.search(self.code_pattern,text):
                self.code = re.search(self.code_pattern,text).group() if self.code == '' else self.code
            if re.search(self.fof_pattern,text):
                self.fof_name = re.search(self.fof_pattern,text).group() if self.fof_name == '' else self.fof_name
            if re.search(self.fund_pattern,text):
                fund_name = re.search(self.fund_pattern,text).group()
                if fund_name != self.fof_name:
                    self.fund_name = fund_name if self.fund_name == '' else self.fund_name
            if re.search(self.date_pattern,text):
                self.date = pd.to_datetime(re.search(self.date_pattern,text).group()) if self.date == '' else self.date

    def reset_desc(self):
        self.fund_desc:str = ''   # 基金名称描述
        self.code_desc:str = ''   # 基金代码描述
        self.mgr_desc:str = ''    # 管理人描述
        self.nvbf_desc:str = ''   # 单位净值描述 
        self.nvaf_desc:str = ''   # 虚拟净值描述 
        self.acc_nv_desc:str = '' # 累计净值表述
        self.share_desc:str = ''  # 持有份额描述
        self.fof_desc:str = ''    # fof名称描述
        self.fof_code_desc:str = '' # fof代码描述
        self.carry_desc:str = ''  # 业绩报酬描述
        self.date_desc:str = ''   # 净值日期描述 
        self.share_date_desc:str = '' # 持有份数日期描述

    @property
    def basic_info(self)->dict:

        return {
            'fund_name':self.fund_name,
            'code':self.code,
            'fof':self.fof_name,
            'fof_code':self.fof_code,
            'date':self.date,
            'src_email':self.subject,
            'src_file':self.file_name,
        }
    
    @property
    def db_cols(self)->dict:
        cols= {
            self.fund_desc:'fund_name',
            self.code_desc:'code',
            self.mgr_desc:'mgr',
            self.nvbf_desc:'nvbf',
            self.nvaf_desc:'nvaf',
            self.acc_nv_desc:'acc_nv',
            self.share_desc:'share',
            self.fof_desc:'fof',
            self.fof_code_desc:'fof_code',
            self.carry_desc:'carry',
            self.date_desc:'date',
            self.share_date_desc:'share_date',
        }
        cols.pop('') #删除未匹配的字段
        return cols

    @property
    def trans_num(self):
        return list(filter(lambda x: x!='',[self.nvbf_desc,self.nvaf_desc,self.acc_nv_desc,self.share_desc,self.carry_desc]))
    
    @property
    def trans_str(self):
        return list(filter(lambda x: x!='',[self.fund_desc,self.code_desc,self.mgr_desc,self.fof_desc]))
    
    @property
    def trans_date(self):
        return list(filter(lambda x: x!='',[self.date_desc,self.share_date_desc]))

    @property
    def desc(self):
        return list(filter(lambda x: x!='',
                [self.fund_desc,
                self.code_desc,
                self.nvbf_desc,
                self.nvaf_desc,
                self.acc_nv_desc,
                self.share_desc,
                self.fof_desc,
                self.carry_desc,
                self.date_desc]))

    def _is_qualified_df(self, df):
        """
        判断表格中是否有需要采集的字段，以及字段的描述
        """

        # 单位净值和累计净值在一行则视为合格的表格
        # 以单位净值开始，累计净值结束
        # 如果不在同一行则将表格方向转置
        qualified_count = 0

        for nvbf_desc in nvbf_set:
            if nvbf_desc in df.columns:
                self.nvbf_desc = nvbf_desc
                qualified_count+=1

        for fund_desc in fund_set:
            if fund_desc in df.columns:
                self.fund_desc = fund_desc
                qualified_count+=1

        for code_desc in code_set:
            if code_desc in df.columns:
                self.code_desc = code_desc
                qualified_count+=1

        for date_desc in date_set:
            if date_desc in df.columns:
                self.date_desc = date_desc

        for nvaf_desc in nvaf_set:
            if nvaf_desc in df.columns:
                self.nvaf_desc = nvaf_desc
                qualified_count+=1

        for fof_desc in fof_set:
            if fof_desc in df.columns:
                self.fof_desc = fof_desc
                qualified_count+=1

        for carry_desc in carry_set:
            if carry_desc in df.columns:
                self.carry_desc = carry_desc
                qualified_count+=1

        for acc_nv_desc in acc_nv_set:
            if acc_nv_desc in df.columns:
                self.acc_nv_desc = acc_nv_desc
                qualified_count+=1

        for share_desc in share_set:
            if share_desc in df.columns:
                self.share_desc = share_desc
                qualified_count+=1
        
        for share_date_desc in share_date_set:
            if share_date_desc in df.columns:
                self.share_date_desc = share_date_desc
                qualified_count+=1

        if qualified_count >=2:
            return True
 
        self.reset_desc()
        return False

    def _reset_col_by_row(self, row=0):
        """将指定列作为列名"""
        df = self.raw.rename(columns=self.raw.iloc[row].to_dict())
        df.columns = list(map(lambda x: re.sub(self.col_pattern, '', str(x)), df.columns))  # 调整列名
        return df

    def _reset_col_by_col(self, col=0):
        """将指定行作为列名"""
        df = self.raw.T.reset_index().rename(columns=self.raw.T.reset_index().iloc[col].to_dict())
        df.columns = list(map(lambda x: re.sub(self.col_pattern, '', str(x)), df.columns))  # 调整列名
        return df

    def reset_df_col(self, raw):
        """
        判断表格读取方向（横向 / 纵向）

        """
        df = raw.copy()
        for row in range(self.raw.shape[0]):
            if self._is_qualified_df(df):
                return df
            else:
                df = self._reset_col_by_row(row)

        df = raw.copy()
        for col in range(self.raw.shape[1]):
            if self._is_qualified_df(df):
                return df
            else:
                df = self._reset_col_by_col(col)

    def insert_db(self,data):
        payload = FOF_tb(**data)
        with self.Session() as session:
            # 检查库中数据是否重复
            session.add(payload)
            session.commit()

    def fetch_values(self)->list:
        """获取表格中的数字"""
        data_lst = []
        df=self.df
        for n in df.index:  # 逐行扫描有效数据
            # 有虚拟净值数据
            if self.nvaf_desc!='':
                nvaf = df.loc[n][self.nvaf_desc]  # 取値：单位净倍
                condition = is_non_zero_decimal(nvaf)
                if condition:
                    data:pd.Series = df.loc[n,self.desc].copy()
                    row=self.merge_dict(self.basic_info,self.extract_values(data))
                    data_lst.append(row)

            # 没有虚拟净值，判断是否有单位净值
            elif self.nvbf_desc!='':
                nvbf = df.loc[n][self.nvbf_desc]  # 取值：单位净值
                condition = is_non_zero_decimal(nvbf)
                if condition:  
                    data:pd.Series = df.loc[n,self.desc].copy()
                    row=self.merge_dict(self.basic_info,self.extract_values(data))
                    data_lst.append(row)

            # 没有净值数据，只有份额数据
            elif self.share_desc!='':
                share = df.loc[n][self.share_desc]  # 取值：份额
                condition = is_non_zero_decimal(share)
                if condition:
                    data:pd.Series = df.loc[n,self.desc].copy()
                    row=self.merge_dict(self.basic_info,self.extract_values(data))
                    data_lst.append(row)
        
        return data_lst
    
    def merge_dict(self,basic,supplement):
        for key,value in supplement.items():
            # 没有key
            if key not in basic:
                basic[key] = value
            # 有key保留非空值
            else:
                if value is not None and value != '':
                    basic[key] = value
        return basic
    

    def extract_values(self,data:pd.Series)->dict:
        for desc in self.desc:
            # 调整数字格式
            if desc in self.trans_num:
                if not is_decimal(data[desc]):
                    continue
                data[desc] = to_decimal(data[desc])
            # 调整文本格式
            elif desc in self.trans_str:
                if not isinstance(data[desc],str):
                    continue
                text = strQ2B(data[desc])
                if '-' in text:
                    text_lst = text.split('-')
                    for part in text_lst:
                        if re.search(self.code_pattern,part):
                            self.code_desc = '产品代码' if self.code_desc=='' else self.code_desc
                            data[self.code_desc] = part
                        elif re.search(self.fund_pattern,part):
                            self.fund_desc = '产品名称' if self.fund_desc=='' else self.fund_desc
                            data[self.fund_desc] = part
                else:
                    data[desc] = text
            # 调整日期格式
            elif desc in self.trans_date:
                if data[desc] is None:
                    continue
                if isinstance(data[desc],float):
                    data[desc]=int(data[desc])
                data[desc] = pd.to_datetime(re.sub(r'年|月|日','',str(data[desc])))

        return data.rename(index=self.db_cols).to_dict()
