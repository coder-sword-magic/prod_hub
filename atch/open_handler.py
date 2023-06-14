from typing import Union
from atch.doc_handler import GenDoc
from datetime import timedelta
from datetime import datetime as dt
import pandas as pd
from tools.date import parse_date
import os
from tools.digit import to_chinese_numeral
from tools.db_tools import DBT


class GenOpenDoc(GenDoc):

    """
    开放日表格
    """
    def __init__(self,**kwargs,):
        super().__init__(**kwargs)


        # 路径参数
        self.base_path = os.path.join('atch/files',self.short_name)
        self.export_folder = os.path.join(self.base_path,f'{self.short_name}_开放日附件_第{self.open_ancm_count}次')
        self.template_folder = os.path.join(self.base_path,'template')

        # 文件日期
        if self.content.get('doc_date',None) is None:
            # 文件日期为开放日的前30个自然日
            self.doc_date = self.rng[0]-timedelta(days=30)
        else:
            self.doc_date = parse_date(self.content.get('doc_date'))


        '''runtime 参数'''
        # 文件名
        self.attachment_names={'ntf':f'{self.full_name}业务通知单.docx',
                               'ancm':f'{self.full_name}第{self.open_ancm_count}次开放公告.docx',
                               'sms': f'{self.full_name}短信.docx'}

        # 文件路径
        for k,name in self.attachment_names.items():
            self.attachment_paths[k]=os.path.join(self.export_folder,name)


        # 模板路径
        self.template_paths={'ntf':os.path.join(self.template_folder,'ntf.docx'),
                             'ancm':os.path.join(self.template_folder,'ancm.docx'),
                             'sms':os.path.join(self.template_folder,'sms.docx')}
        

        # 生成模板的映射
        self.gen_funcs = {'ntf':self.gen_ntf,
                          'ancm':self.gen_ancm,
                          'sms':self.gen_sms}
        
        
        # print('ancm',self.ancm_count)
        # print('doc_date',self.doc_date_fmt)
        # print('open_period',self.open_period_fmt)
        # print('sub_period', self.sub_period_fmt)
        # print('red_period', self.red_period_fmt)
        # print('next_open_day',self.next_open_fmt)
        # print('soft_lock_period',self.soft_lock_period_fmt)

        # 更新模板数据
        self.content.update({
            'ancm_count':self.open_ancm_count,
            'doc_date':self.doc_date_fmt,
            'open_period':self.open_period_fmt,
            'sub_period': self.sub_period_fmt,
            'red_period': self.red_period_fmt,
            'next_open_day':self.next_open_fmt,
            'soft_lock_period':self.soft_lock_period_fmt
        })

            
    
    def gen_sms(self):
        """从数据库中提取短信模板"""
        template_path = self.get_template_path('sms')
        return self.render(template_path,self.content)
    
        
    def gen_ancm(self):
        '''从数据库调取基础要素'''
        template_path = self.get_template_path('ancm')
        return self.render(template_path,self.content)
          
                
    def gen_ntf(self):
        """从数据库中提取通知单模板"""
        template_path = self.get_template_path('ntf')

        cont = {'full_name':self.full_name,
                'code':self.code,
                'A1':'','A2':'','A3':'','A4':'',
                'B1':'','B2':'','B3':'','B4':'',
                'C1':'','C2':'','C3':'','C4':'',
                'D1':'','D2':'','D3':'','D4':'',}

        r = ('A','B','C','D')
        i = 0
        status = (False,False)

        for date in self.rng:
            if date in self.sub_rng and date in self.red_rng:
                if status != (True,True):
                    cont[f'{r[i]}1'] = '开'
                    cont[f'{r[i]}2'] = '开'
                    cont[f'{r[i]}4'] = dt.strftime(date,self.fmt)
                    status = (True,True)
                    i+=1
            elif date not in self.sub_rng and date in self.red_rng:
                if status != (False,True):
                    cont[f'{r[i]}1'] = '不开'
                    cont[f'{r[i]}2'] = '开'
                    cont[f'{r[i]}4'] = dt.strftime(date,self.fmt)
                    status = (False,True)
                    i+=1
            elif date not in self.red_rng and date in self.sub_rng:
                if status != (True,False):
                    cont[f'{r[i]}1'] = '开'
                    cont[f'{r[i]}2'] = '不开'
                    cont[f'{r[i]}4'] = dt.strftime(date,self.fmt)
                    status = (True,False)
                    i+=1

        cont[f'{r[i]}1'] = '不开'
        cont[f'{r[i]}2'] = '不开'
        cont[f'{r[i]}4'] = dt.strftime(self.rng[-1] + timedelta(days=1),self.fmt)

        return self.render(template_path,cont)
    
def main(req_id:Union[str,None]=None,
         full_name:Union[str,None]=None,
         after_date:Union[dt,None]=None,
         ):
    # 非自动
    if req_id is not None:
        doc = GenOpenDoc(req_id=req_id)
        doc.gen_attachments()
        doc.upload_data(tb_name=doc.dbt.atch_tb)
    # 自动
    elif full_name is not None:
        doc = GenOpenDoc(full_name=full_name,after_date=after_date)
        print('开始生成附件')
        doc.gen_attachments()
        print('成功生产附件:',full_name)
        doc.upload_data(tb_name=doc.dbt.atch_tb,doc_type='开放公告')

    return doc.content

if __name__ == '__main__':
    pass