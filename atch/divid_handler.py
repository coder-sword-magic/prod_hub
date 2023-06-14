from atch.doc_handler import GenDoc
from datetime import timedelta
from dateutil import parser
from tools.digit import to_chinese_numeral
from typing import Union
import os
from cfg.alias import *
from tools.db_tools import *
from tools.date import parse_date,next_tr_date


class GenDividendDoc(GenDoc):
    
 
    def __init__(self,
                **kwargs
            ):
        super().__init__(**kwargs)

        # 路径参数
        # /输出/上海证券xxxx资产管理计划/xxxx_开放日附件_1
        self.base_path = os.path.join('atch/files',self.short_name)
        self.export_folder = os.path.join(self.base_path,f'{self.short_name}_分红附件_第{self.ancm_count}次')
        self.template_folder = os.path.join(self.base_path,'template')

        '''表格参数'''
        # 权益登记 = 分红日 = 除权日 = 分红业绩报酬
        # 红利划出日 = T + 2
        # 红利到账日 = T + 3
        # 分红再投资 = T + 1

        # 分配批次
        self.dividend_count= to_chinese_numeral(self.content.get('dividend_count',0))

        # 权益登记日
        self.record_date=parse_date(self.content.get(RECORD_DATE,None)) 
        if self.record_date is None:
            raise ValueError('红利表格缺少基准日')
        
        # 除权除息日
        self.ex_dividend_date=parse_date(self.content.get(EX_DIVIDEND_DATE,self.record_date))

        # 红利划出日
        self.dividend_payment=parse_date(self.content.get(DIVIDEND_PAYMENT,next_tr_date(self.record_date,inter_days=2)))

        # 红利到账日
        self.settlement_date=parse_date(self.content.get(SETTLEMENT_DATE,next_tr_date(self.record_date,inter_days=3)))

        # 利润分配基准日
        self.clearing_date=parser.parse(self.content.get(CLEARING_DATE,self.record_date)) 
        
        self.reinvest_date = parse_date(self.content.get(REINVEST_DATE,next_tr_date(self.record_date,inter_days=1))) # 分红再投资日
        self.dividend_unit=self.content.get(DIVIDEND_UNIT,0) # 分红单位
        self.dividend_amt=self.content.get(DIVIDEND_AMT,0) # 单位分红金额
        self.custodian = self.content.get(CUSTODIAN,None) # 托管行

        # 文件日期
        if self.content.get('doc_date',None) is None:
            # 文件日期为开放日的前30个自然日
            self.doc_date = self.rng[0]-timedelta(days=30)
        else:
            self.doc_date = parse_date(self.content.get('doc_date'))

        '''runtime 参数'''
        # 文件名
        self.attachment_names={'ntf':f'{self.full_name}分红通知单.docx',
                               'ancm':f'{self.full_name}第{self.ancm_count}次分红公告.docx',
                               'sms': f'{self.full_name}分红短信.docx'}

        # 文件路径
        for k,name in self.attachment_names.items():
            self.attachment_paths[k]=os.path.join(self.export_folder,name)

        # 模板路径
        self.template_paths={'ntf':os.path.join(self.template_folder,'d_ntf.docx'),
                             'ancm':os.path.join(self.template_folder,'d_ancm.docx'),
                             'sms':os.path.join(self.template_folder,'d_sms.docx')}

        # 生成模板的映射
        self.gen_funcs = {'ntf':self.gen_ntf,
                          'ancm':self.gen_ancm,
                          'sms':self.gen_sms}


    def gen_ntf(self):
        template_path = self.get_template_path('ntf')

        fmt='%Y%m%d'
        context = {
                    'full_name':self.full_name,
                    'code':self.code,
                    'record_date':dt.strftime(self.record_date,fmt),
                    'reinvest_date':dt.strftime(self.reinvest_date,fmt),
                    'dividend_unit':int(int(self.dividend_unit)/int(self.dividend_unit)),
                    'dividend_amt':round(float(self.dividend_amt)/int(self.dividend_unit),4),
                    }

        return self.render(template_path=template_path,context=context)

    def gen_ancm(self):

        template_path = self.get_template_path('ancm')

        context = {
            'full_name':self.full_name,
            'code':self.code,
            'dividend_count':self.dividend_count, # 批次
            'nv_date':self.clearing_date, # 利润分配基准日
            'record_date':dt.strftime(self.record_date,self.fmt), # 权益登记日
            'ex_dividend_date':dt.strftime(self.ex_dividend_date,self.fmt), # 除权除息日
            'dividend_payment':dt.strftime(self.dividend_payment,self.fmt), # 红利划出日
            'settlement_date':dt.strftime(self.settlement_date,self.fmt), # 红利到账日
            'dividend_unit':self.dividend_unit, # 
            'dividend_amt':self.dividend_amt, #
            'doc_date':dt.strftime(self.doc_date,self.fmt),
            'custodian':self.custodian,
        }

        self.render(template_path=template_path,context=context)


def main(req_id:Union[str,None]=None,
        full_name:Union[str,None]=None):
    if req_id is not None:
        doc = GenDividendDoc(req_id=req_id)
        doc.gen_attachments()
        doc.upload_data(tb_name=doc.dbt.atch_tb)
    elif full_name is not None:
        doc = GenDividendDoc(full_name=full_name)
        doc.gen_attachments()
        # todo 名称待定
        doc.upload_data(tb_name=doc.dbt.atch_tb,doc_type='红利公告')

    return doc.content


if __name__ == '__main__':
    doc = GenDividendDoc.make()
    doc.export_attachments()
    doc.localize_all()
    doc.export_all(doc_type='divd_ancm')