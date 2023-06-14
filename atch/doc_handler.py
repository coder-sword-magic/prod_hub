from datetime import datetime as dt
from datetime import timedelta as td
from docxtpl import DocxTemplate
from functools import cached_property
from tools.file import zip_folder
from typing import Union
import os
import re
import io
from tools.db_tools import DBT
import pandas as pd
from tools import date
from tools.digit import to_chinese_numeral
from tools.date import parse_date
import secrets
from sqlalchemy import text

def main(req_id):
    '''
    生戯各业务文档
    '''
    gen_doc = GenDoc(req_id)
    return gen_doc.content

class GenDoc:

    def __init__(self,
                 req_id:Union[str,None]=None,
                 full_name:Union[str,None]=None,
                 after_date:Union[dt,str,None]=None):
        """
        读取数据库参数
        初始化变量
        """

        '''用户提交的入参'''
        self.dbt=DBT()
        self.content = dict()
        self.auto = False
        open_period_start,open_period_end,sub_period_start,sub_period_end,red_period_start,red_period_end=None,None,None,None,None,None

        # 手动生成
        if req_id is not None:
            # 从数据库中找到请求记录
            self.req_raw:pd.DataFrame = self.dbt.fetch_req_raw(id=req_id,tb=self.dbt.atch_tb)
            if self.req_raw.empty:
                raise ValueError('id访问的不存在:',req_id)
            # 解析请求参数
            self.req_params:list = eval(self.req_raw.iloc[0]['params'])
            for v in self.req_params:
                self.content[v.get('name')]=v.get('value')

            # 提取必填项信息
            open_period = self.content.get('open_period',None)
            if open_period is not None:
                open_period_start,open_period_end = open_period.split(' ')
            # 提取选填项信息
            sub_period = self.content.get('sub_period',None)
            if sub_period is not None:
                sub_period_start,sub_period_end = sub_period.split(' ')
            red_period = self.content.get('red_period',None)
            if red_period is not None:
                red_period_start,red_period_end = red_period.split(' ')

            # 提取高频字段
            self.full_name = self.content.get('full_name')
            self.req_id = req_id
            # 从赎回日首日开始计算
            self.after_date = parse_date(open_period_start)
        
        # 自动生成
        elif full_name is not None:
            self.full_name = full_name
            self.auto = True
            self.req_raw = None
            self.after_date = parse_date(after_date)
        else:
            print('需要至少提供 req_id 或者 full_name')
            raise Exception('需要至少提供 req_id 或者 full_name')
        

        print('after date:',self.after_date)

        '''产品要素与开放日历'''
        # 更新产品要素
        prod=self.dbt.fetch_product(full_name=self.full_name)
        self.content.update(prod)  

        # 提取开放日日历
        self.calendar=self.dbt.fetch_next_open(full_name=self.full_name,
                                 # todo 需要改为最近开放日
                                after_date=self.after_date,
                                # todo 需要判断1年是否足够
                                before_date=self.after_date+td(days=365))
        
        # 无开放日历不生成文件
        if self.calendar.empty:
            print('无据库无产品信息')
            raise ValueError(f'{self.full_name}无开放日数据')
        

        this_open = self.calendar.iloc[0].T.to_dict()
                # 更新开放日历
        self.content.update(this_open)

        self.open_ancm_count = '未知'
        # todo 临时处理，等第二次修改的时候字段落表
        from sqlalchemy import create_engine,text
        engine = create_engine('sqlite:///open_ancm_count.db')        
        # 提取输入的ancm_count
        ancm_count = self.content.get('ancm_count','')
        # 如果输入为空
        if ancm_count == '':
            # 读取本地sqlite
            tmp = pd.read_sql(sql=f'''select * from open_ancm where id='{this_open['id']}';''',con=engine)
            # 读取数据库开放次数
            tmp_ancm_count = tmp['ancm_count'].values[0]
            # 如果没有数据就返回未知
            if tmp_ancm_count == 0:
                 self.open_ancm_count = '未知'
            # 有数据就返回数据
            else:
                self.open_ancm_count = to_chinese_numeral(tmp_ancm_count)
        # 如果输入不为空
        elif ancm_count.isdigit():
            ancm_count = int(ancm_count)
            print('ancm_count in content HHHH:',ancm_count)
            self.open_ancm_count = to_chinese_numeral(ancm_count)
            print('YYYYYYYYYYYYYYYYYYYYYYYYYY',self.open_ancm_count,ancm_count)
            # 提取本地所有产品的次数
            tmp = pd.read_sql(sql=f'''select * from open_ancm where full_name='{self.full_name}';''',con=engine)
            # 升序排列
            tmp=tmp.sort_values('sub_period_start',ascending=True)
            print('tmp')
            print(tmp)
            # 提取id的index
            idx = tmp[tmp['id']==this_open['id']].index[0]
            tmp['tmp']=1
            # 
            tmp.loc[idx:,'ancm_count']=ancm_count + tmp.loc[idx:,'tmp'].cumsum() - 1
            # 前面不变
            tmp = tmp.drop('tmp',axis=1)
            print(tmp)
            # 
            conn = engine.connect()
            conn.execute(text(f'''delete from open_ancm where full_name='{self.full_name}';'''))
            conn.close()
            tmp.to_sql(name='open_ancm',index=False,con=engine,if_exists='append')



        print('&&&&&&&&&&&&&&&&&&&&&&&&&&&',self.open_ancm_count)
        
        # 更新选填项
        if sub_period_start is not None:
            self.content['sub_period_start']=sub_period_start
        if sub_period_end is not None:
            self.content['sub_period_end']=sub_period_end
        if red_period_start is not None:
            self.content['red_period_start'] = red_period_start
        if red_period_end is not None:
            self.content['red_period_end']=red_period_end

        '''常用变量'''
        self.fmt = '%Y年%m月%d日'
        self.code = self.content.get('code')
        self.short_name = self.content.get('short_name')

        # 生成附件的名称
        self.attachment_names = dict()
        # 存放附件的路径 
        self.attachment_paths = dict()
        # 存放模板的路径
        self.template_paths = dict()
        # 生成附件的函数
        self.gen_funcs = dict()
        # 
        self.common_path = os.path.join('files','common_templates')
        self.general_templates={'ntf':os.path.join(self.common_path,'ntf.docx'),
                                'ancm':os.path.join(self.common_path,'ancm.docx'),
                                'sms':os.path.join(self.common_path,'sms.docx'), # todo divid写的不对
                                'd_ntf':os.path.join(self.common_path,'d_ntf.docx'),
                                'd_ancm':os.path.join(self.common_path,'d_ancm.docx'),
                                'd_sms':os.path.join(self.common_path,'d_sms.docx'),
                                'mod':os.path.join(self.common_path,'mod.docx')}
        
    @cached_property
    def sub_st(self):
        return parse_date(self.content.get('sub_period_start'))
    
    @cached_property
    def sub_ed(self):
        return parse_date(self.content.get('sub_period_end'))
    
    @cached_property
    def red_st(self):
        return parse_date(self.content.get('red_period_start'))
    
    @cached_property
    def red_ed(self):
        return parse_date(self.content.get('red_period_end'))
        
    @cached_property
    def next_open(self):
        return parse_date(self.content.get('next_open'))

    @cached_property
    def sub_rng(self):
        return date.tr_date_range(self.sub_st,self.sub_ed)

    @cached_property
    def red_rng(self):
        return date.tr_date_range(self.red_st,self.red_ed)
    
    @cached_property
    def rng(self):
        return date.tr_date_range(min(self.sub_st,self.red_st),max(self.sub_ed,self.red_ed) )
    
    @cached_property
    def _after_date(self):
        return min(self.sub_st,self.red_st).date()

    @property
    def soft_lock_period_fmt(self):
        # todo 需要调整为工作日,并判断有无期限
        try:
            from_date = date.parse_date(date.next_tr_date(self.sub_st))
            
            v = int(self.content.get('soft_lock'))
            if v is None:
                return None
            u = self.content.get('soft_lock_u')

            # todo 
            if u == '天':
                to_date = from_date+td(days=v)
            elif u == '周':
                to_date = from_date+td(weeks=v)
            elif u == '个月' or u == '月':
                if from_date.month+v>12:
                    add_year = int((from_date.month+v)/12)
                    month=(from_date.month+v)%12
                    to_date = dt(from_date.year+add_year,month,from_date.day)
                else:
                    to_date = dt(from_date.year,from_date.month+v,from_date.day)
            elif u == '年':
                to_date = dt(from_date.year+v,from_date.month,from_date.day)
            else:
                print('锁定期单位错误: soft_lock_unit =',u)
                return None

            return '-'.join([dt.strftime(from_date,self.fmt),dt.strftime(to_date,self.fmt)])
        
        except:
            return ''
        
    @property
    def sub_st_fmt(self):
        return dt.strftime(self.sub_st,self.fmt)

    @property
    def sub_ed_fmt(self):
        return dt.strftime(self.sub_ed,self.fmt)

    @property
    def red_st_fmt(self):
        return dt.strftime(self.red_st,self.fmt)
    
    @property
    def red_ed_fmt(self):
        return dt.strftime(self.red_ed,self.fmt)
    
    @property
    def doc_date_fmt(self):
        return dt.strftime(self.sub_st - td(days=30),self.fmt)
    
    @property
    def sub_period_fmt(self):
        return '-'.join([self.sub_st_fmt,self.sub_ed_fmt]) if self.sub_st!=self.sub_ed else self.sub_st_fmt
    
    @property
    def red_period_fmt(self):
        return '-'.join([self.red_st_fmt,self.red_ed_fmt]) if self.red_st!=self.red_ed else self.red_st_fmt
    
    @property
    def open_period_fmt(self):
        """最长期限"""
        return '-'.join([dt.strftime(min(self.sub_st,self.red_st),self.fmt),dt.strftime(max(self.sub_ed,self.red_ed),self.fmt)])
    
    @property
    def doc_date_fmt(self):
        return dt.strftime(self.doc_date,self.fmt) 

    @property
    def next_open_fmt(self):
        if self.next_open is None:
            return ''
        return dt.strftime(self.next_open,self.fmt)
    
    def get_template_path(self,template_name:str):
        """
        返回可用的模板,优先选择模板
        """
        if not os.path.exists(self.template_paths[template_name]):
            return self.general_templates[template_name]
        else:
            return self.template_paths[template_name]
    
    def gen_attachments(self):
        """
        将io buffer 写入文件
        """
        if os.path.exists(self.export_folder):
            remove_folder(self.export_folder)
        os.makedirs(self.export_folder)
        for t,path in self.attachment_paths.items():
            with open(path,'wb') as f:
                f.write(self.gen_funcs[t]())
    
    def render(self,template_path:str,context:dict)->bytes:
        """
        渲染模板 写入 io buffer
        """
        buffer = io.BytesIO()
        template = DocxTemplate(template_path)
        template.render(context)
        template.save(buffer)
        return buffer.getvalue()

    def express_folder(self)->str:
        """
        压缩文件夹
        """
        zip_path = f'{self.export_folder}.zip'
        zip_folder(folder_path=self.export_folder,output_path=zip_path)
        return zip_path
    
    def update_req_params(self,
                          params:Union[str,None]=None):
        if isinstance(params,str):
            # 自动生成附件，没有request id的情况
            params_lst = eval(params)
        elif params is None:
            # 包含request id的情况
            params_lst = self.req_params

        for i in range(len(params_lst)):
            name = params_lst[i]['name']
            value = self.content.get(name)    
            value = self.gen_front_end(name,value)
            params_lst[i]['value'] = value    

        self.req_params = params_lst
        return str(params_lst).replace('\'','\"') # 将单引号改为双引号
    
    def gen_front_end(self,name,value):
        if re.match(r'\d{4}年\d{2}月\d{2}日-\d{4}年\d{2}月\d{2}日',value):
            value = re.sub(r'日','',value)
            value = re.sub(r'-',' ',value)
            value = re.sub(r'[年月]','-',value)
            return value
        elif value == self.red_period_fmt and name == 'red_period':
            value = re.sub(r'日','',value)
            value = re.sub(r'[年月]','-',value)
            value = value+' '+value
            return value
        elif value == self.doc_date_fmt and name == 'doc_date':
            value = re.sub(r'日','',value)
            value = re.sub(r'[年月]','-',value)
            return value
        else:
            return value

    
    def get_req_param(raw:Union[list,str],
                        key:str)->Union[str,None]:
        """
        用以获取request id
        """
        if isinstance(raw,str):
            raw = eval(raw)
        elif isinstance(raw,list):
            for item in raw:
                if item['name'] == key:
                    return item['value']
        return None

    def gen_req_raw(self,
                    doc_type:str,
                    tb_name:str):
        """
        用于自动生成附件
        """
        # todo doc_date 未处理
        sql = f'''select * from {tb_name} where type='{doc_type}' limit 1;'''
        df = self.dbt.load_from_db(sql)
        print(df)
        if df.empty:
            raise Exception(f'表{tb_name}中暂无{doc_type}类型的数据')
        now_time = str(dt.now().time())
        df['id'] = secrets.token_hex(16)
        df['full_name'] = self.full_name
        df['create_time'] = now_time
        df['create_user'] = 'auto'
        df['update_user'] = 'auto'
        df['update_time'] = now_time
        df['name'] = self.full_name
        df['file_name'] = None
        df['file_data'] = None
        df['params'] = self.update_req_params(params=df['params'].iloc[0])
        self.req_raw = df.copy()
        return self.req_raw
    
    def upload_data(self,
                    tb_name:str,
                    doc_type:Union[str,None]=None,
                    ):
        print('auto:',self.auto)

        if self.auto is True:
            self.req_raw = self.gen_req_raw(doc_type=doc_type,tb_name=tb_name)
        else:
            self.req_raw['params'] = self.update_req_params()

        print('req_raw',self.req_raw)
        
        file_path = self.express_folder()
        with open(file_path,'rb') as f:
            self.req_raw['file_data'] = f.read()

        self.req_raw['file_name'] = file_path.split('/')[-1]
        self.req_raw['status'] = 'SUCCESS'

        conn = self.dbt.engine.connect()
        
        if self.auto is False:
            sql = f'''delete from {self.dbt.atch_tb} where id='{self.req_id}';'''
            conn.execute(text(sql))
        
        elif self.auto is True:
            print('添加时间')
            now_time = dt.now()
            self.req_raw['create_time'] = now_time
            self.req_raw['update_time'] = now_time

        self.req_raw.to_sql(name=self.dbt.atch_tb,con=conn,if_exists='append',index=False)
        conn.close()
        print(self.full_name,'附件成功')
        
        # todo 删除文件
        print('删除压缩文件')
        os.remove(file_path)
        print('删除文件夹')
        remove_folder(self.export_folder)
        print('成功')

def remove_folder(path):
    if os.path.exists(path):
        for fn in os.listdir(path):
            fp = os.path.join(path,fn)
            if os.path.isfile(fp):
                os.remove(fp)
            else:
                remove_folder(fp)
        os.rmdir(path)