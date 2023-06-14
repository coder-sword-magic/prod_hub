from mail_tools.mail import Hmail
from mail_tools.excel_reader import ExcelReader
from mail_tools import config as cfg
from typing import Optional
from datetime import datetime,timedelta
import pandas as pd
import re
from tqdm import tqdm
import pandas_market_calendars as mcal
import warnings
warnings.filterwarnings('ignore')


class DataHub:
    
    def __init__(self,
                 lagged_days:int=3, # 向前追溯的天数
                 input_file_path:str='产品汇总.xls'
                 ) -> None:
        
        self.cli=Hmail(pop3_server=cfg.mail_host,
                        user=cfg.mail_user,
                        password=cfg.mail_pass,
                        db_engine=cfg.db_engine)
        
        # 获取上交所的日历
        sse = mcal.get_calendar('XSHG')
        self.now_date = datetime.now().date()
        self.schedule = sse.schedule(start_date=str(datetime(self.now_date.year-1,12,1)), end_date=str(self.now_date))
        self.from_date = self.schedule.index[-lagged_days]
        self.db:Optional[pd.DataFrame] = None
        self.input_df = pd.read_excel(input_file_path,index_col=0).rename(columns=cfg.input_col())
        self.input_df['code']=self.input_df['code'].str.replace('OTC','').str.strip()
        self.input_df['nv']=None
        self.input_df['share']=None
        self.input_df['date']=None
        self.db = None
    
    def parse_atchs(self)->pd.DataFrame:
        """
        解析原始邮件，偏底层
        """
        data=self.cli.load_from_db()
        data['date']=pd.to_datetime(data['date'])
        self.mail_db = data
        bct = data[data['date']>=self.from_date]['byte_content']
        data_list = []
        # 解析每一份邮件的excel附件
        print('提取附件要素')
        for n in tqdm(bct.index):
            # 读取字节码为mail.Message对象
            msg = self.cli.parse_byte_to_msg(bct[n])
            byte_content_dict = {}
            for i in msg.walk():
                main_type = i.get_content_maintype()
                # 附件
                if main_type == 'application':
                    file_name = i.get_filename()
                    # todo 是否要判断文件名为空的情况
                    if file_name is None:
                        continue
                    # 解析二进制文件名
                    file_name = self.cli.parse_header(file_name)
                    # 跳过不是合法的文件名
                    if not re.search(r'xlsx|xls',file_name):
                        continue
                    raw = i.get_payload(decode=True)
                    byte_content_dict[file_name] = raw

            for file_name,raw in byte_content_dict.items():
                    excel=ExcelReader(file_name=file_name,subject=data.loc[n,'subject'],raw=raw)
                    if excel.df is None:
                        continue
                    data_list.extend(excel.fetch_values())

        return pd.DataFrame(data_list).fillna('')
    
    def extract_atch(self,byte_content):
        msg = self.cli.parse_byte_to_msg(byte_content)
        byte_content_dict = {}
        for i in msg.walk():
            main_type = i.get_content_maintype()
            # 附件
            if main_type == 'application':
                file_name = i.get_filename()
                # todo 是否要判断文件名为空的情况
                if file_name is None:
                    continue
                # 解析二进制文件名
                file_name = self.cli.parse_header(file_name)
                # 跳过不是合法的文件名
                if not re.search(r'xlsx|xls',file_name):
                    continue
                raw = i.get_payload(decode=True)
                byte_content_dict[file_name] = raw
        return byte_content_dict


    def gen_output(self):
        for i,d in self.input_df.iterrows():
            """
            sel为二进制文件解析后的记录
            """
            fof,fof_code,code,fund_name,nv,share,date = d.values

            ''' 数据关联 '''
            sel = self.db
            # 根据自定义的逻辑寻找
            if code in cfg.global_filter:
                fils= cfg.global_filter.get(code)
                for f in fils:
                    sel = sel[sel[f['col']].str.contains(f['val'])]
            # 根据代码或者名称寻找
            else:
                sel = sel[(sel['fund_name'].str.contains(fund_name))|(sel['code'].str.contains(code))]

            if sel.empty:
                print('无数据',fof,fof_code,code,fund_name)
                continue

            # 如有fof名称，进一步匹配
            if not (sel.fof == '').all():
                sel = sel[sel['fof'].str.contains(fof)]
            
            if sel.empty:
                print('有数据无匹配fof管理人',fof,fof_code,code,fund_name)
                continue

            ''' 对多条关联记录进行筛选 '''
            # 自定义过滤器
            if code in cfg.cus_filter:
                fils= cfg.cus_filter.get(code)
                for f in fils:
                    sel = sel[sel[f['col']].str.contains(f['val'])]

            '''对记录整型'''
            # 按日期降序排列
            sel = sel.sort_values('date',ascending=False)
            sel = sel.drop_duplicates(ignore_index=True,subset=['nvbf','share','nvaf'])
            # 按日期合并
            sel_by_date = sel.groupby('date')
            # 最新日期
            last_date = max(sel['date']) 
            # 自定义聚合时间函数进行整型
            if code in cfg.agg_func:
                sel = sel_by_date.aggregate(cfg.agg_func.get(code)).reset_index()
                sel_by_date = sel.groupby('date')

            # 没有fof的名称
            # todo 只有单条记录,算法待优化
            if (sel_by_date.count().loc[last_date] == 1).all():
                nvbf,nvaf,share = sel[sel['date'] == last_date][['nvbf','nvaf','share']].values[0]
                # todo 数据错误
                self.input_df.loc[i,'nv'] = round(float(nvaf),4) if nvaf!='' else round(float(nvbf),4)
                self.input_df.loc[i,'share'] = share
                self.input_df.loc[i,'date'] = last_date
            else:
                print('多条记录未选择',fof,fof_code,code,fund_name)
                break

        output = self.input_df[['fof','fof_code','date','code','fund_name','nv','share']]
        output['date']=output['date'].apply(lambda x: x.strftime('%Y%m%d') if x is not None else x)

        return output
    
    def main(self):
        self.cli.fetch_email(stop_before_date=self.from_date.date())
        self.db = self.parse_atchs()
        self.output = self.gen_output().rename(columns=cfg.output_col())
        self.output.to_csv('FBHQ.csv')
        print('生成文件:FBHQ.csv')
        now = datetime.now()
        self.cli.send_mail(
            subject=f'fof_持仓汇总_{now.date()}',
            sender=cfg.sender,
            receiver=cfg.receiver,
            mail_text='',
            atch_path_lst=['FBHQ.csv'],
            encoding=cfg.encode_type
        )
        print('邮件发送成功')


def main():
    dh = DataHub(lagged_days=4)
    dh.main()


if __name__=='__main__':
    main()