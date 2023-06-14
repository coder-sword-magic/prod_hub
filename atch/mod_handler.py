from atch.doc_handler import GenDoc
from tools.db_tools import *
from tools.date import parse_date
import os

class GenModDoc(GenDoc):

    """
    参数变更表格
    """
    def __init__(self,**kwargs,):
        super().__init__(**kwargs)

        # 路径参数
        # /输出/上海证券xxxx资产管理计划/xxxx_开放日附件_1
        self.base_path = os.path.join('atch/files',self.short_name)
        self.export_folder = os.path.join(self.base_path,f'{self.short_name}_系统参数设置通知单')
        self.template_folder = os.path.join(self.base_path,'template')

        # 参数表格
        self.parameter_des = self.content.get('parameter_des',None)
        self.parameter_type = self.content.get('parameter_type',None)
        self.apply_department = self.content.get('apply_department',None)
        self.system = self.content.get('system',None)
        self.parameter_name = self.content.get('parameter_name').split(';')
        self.setting_date = parse_date(self.content.get('setting_date',dt.now().date()))
        self.doc_date = parse_date(self.content.get('doc_date',self.setting_date))

        # 文件名
        self.attachment_names={'mod':f'{self.full_name}系统参数设置通知单.docx'}

        # 文件路径
        for k,name in self.attachment_names.items():
            self.attachment_paths[k]=os.path.join(self.export_folder,name)

        # 模板路径
        self.template_paths={'mod':os.path.join(self.template_folder,'mod.docx')}

        # 生成模板的映射
        self.gen_funcs = {'mod':self.gen_mod}


    def gen_mod(self):     
        template_path = self.get_template_path('mod')
           
        context={
            'full_name':self.full_name,
            'parameter_des':self.parameter_des,
            'parameter_type':self.parameter_type,
            'apply_department':self.apply_department,
            'system':self.system,
            'setting_date':dt.strftime(self.setting_date,self.fmt),
            'doc_date':dt.strftime(self.doc_date,self.fmt),
        }

        for i,pn in enumerate(['parameter_name_1','parameter_name_2','parameter_name_3']):
            context[pn] = self.parameter_name[i] if i<len(self.parameter_name) else '' 
        
        return self.render(template_path=template_path,context=context)
    
def main(req_id:Union[str,None]=None,
         full_name:Union[str,None]=None):
    if req_id is not None:
        doc = GenModDoc(req_id=req_id)
        doc.gen_attachments()
        doc.upload_data(tb_name=doc.dbt.atch_tb)
    elif full_name is not None:
        doc = GenModDoc(full_name=full_name)
        doc.gen_attachments()
        # todo 名称待定
        doc.upload_data(tb_name=doc.dbt.atch_tb,doc_type='参数修改')

    return doc.content