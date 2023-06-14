from typing import Union
import pandas as pd
from datetime import datetime as dt
from tools.date import parse_date
import psycopg2
import secrets
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import text
from configparser import ConfigParser

def convert_to_dt(df: pd.DataFrame,
                  params: list):
    for v in params:
        df[v] = df[v].apply(lambda x: parse_date(x))

    return df


class DBT(HSQL):

    def __init__(self) -> None:
        super().__init__()
        self.date_params = ['sub_period_start','sub_period_end','red_period_start','red_period_end']

    def load_config(self):
        super().load_config()
        self.box_tb = self.config.get('db','box_tb')
        self.calendar_tb = self.config.get('db','calendar_tb')
        self.product_tb = self.config.get('db','product_tb')
        self.atch_tb = self.config.get('db','atch_tb')

    def update_cal_db(self,df:pd.DataFrame):
        """
        更新calendar表
        """
        self.upload_to_db(df,tb_name=self.calendar_tb)


    def fetch_atch_param(self,req_id)->dict:
        """
        获取公告附件生成表格的入参的json部分
        """
        sql=f'''select * from {self.atch_tb} where id='{req_id}';'''
        raw = self.load_from_db(sql=sql,tb=self.atch_tb)
        if raw.empty:
            return raw
        return eval(raw.params[0])
    
    def fetch_req_raw(self,
                      id:str,
                      tb:str):
        """
        获取生戽数据表样本的入参的json郫分
        """
        sql=f'''select * from {tb} where id='{id}' limit 1;'''
        raw = self.load_from_db(sql=sql)
        return raw

    
    def fetch_box(self,req_id)->dict:
        """
        获取消息盒子
        """
        sql = f"select * from {self.box_tb} where id='{req_id}' limit 1;"
        raw = self.load_from_db(sql=sql)
        if raw.empty:
            return {}
        return raw.iloc[0].to_dict()
    
    def fetch_product(self,code=None,full_name=None)->dict:
        """
        获取数据产品要素
        return : 便于渲染，返回字典
        """
        sql=f"select * from {self.product_tb} where code='{code}' or full_name='{full_name}';"
        raw = self.load_from_db(sql=sql)
        if raw.empty:
            return {}
        return raw.sort_values('update_time',ascending=False).iloc[0].to_dict()
 
    def fetch_next_open(self,
                        after_date:Union[str,dt],
                        before_date:Union[str,dt],
                        full_name:str=None,
                        from_redeem_date=True)->pd.DataFrame:
        """
        获取after date之后最近的一起开放日, 默认以开放赎回日的首日为准
        """

        # 默认按赎回日首日起算，否则按申购日首日
        date = 'red_period_start' if from_redeem_date is True else 'sub_period_start'

        '''参数检查'''
        after_date,before_date = parse_date(after_date),parse_date(before_date)

        '''读取现有日历数据'''
        sql = f'''select * from {self.calendar_tb} where full_name='{full_name}';'''
        raw = self.load_from_db(sql=sql)
        if raw.empty:
            print('数据库无记录')
            print(raw,type(raw),raw.empty)
            return raw

        '''增加 next_open 字段'''
        raw = convert_to_dt(raw,params=self.date_params)
        raw = raw.sort_values(date,ascending=True)
        raw['next_open']=raw[date].shift(-1)

        '''筛选时间范围'''
        raw=raw[(raw[date]>=after_date) & (raw[date]<=before_date)].copy()

        return raw[['id','full_name','sub_period_start','sub_period_end','red_period_start','red_period_end','next_open']]
     
    def fetch_all_calendar(self,full_name:str)->pd.DataFrame:
        """
        获取所有日历
        """
        sql = f'''select * from {self.calendar_tb} where full_name='{full_name}';'''
        raw = self.load_from_db(sql=sql)
        if raw.empty:
            return raw
        
        raw = convert_to_dt(raw,params=self.date_params)

        return raw.sort_values(['sub_period_start','red_period_start'],ascending=True)
    
    def fetch_all_products(self)->pd.DataFrame:
        """
        获取所有数据仓位
        """
        sql=f"select * from {self.product_tb};"
        raw = self.load_from_db(sql=sql)

        if raw.empty:
            return raw
        try:
            # 根据产品代码返回最后更新的数据
            return raw.sort_values('update_time',ascending=False).groupby('code').last().reset_index()
        except:
            return raw        
        
    def upload_new_binary_to_db(self,
                                full_name,
                                file_name,
                                param_tmp,
                                formwork_id,
                                binary_data,
                                file_type,
                                data_id=None
                                ):
        """二进制文件写入数据库"""
        
        # 数据整型
        self.check_env()
        now=str(dt.now())[:-3]
        file_data = psycopg2.Binary(binary_data)
        file_data = str(file_data)
        name = file_name.split('.')[0]

        with self.engine.connect() as conn:
            if data_id is None:
                data_id = secrets.token_hex(16) # 生成随机数作为id
                p = {'id':"'{}'".format(data_id),
                    'create_user':"'{}'".format('admin'),
                    'create_time':"'{}'".format(now),
                    'update_user':"'{}'".format('admin'),
                    'update_time':"'{}'".format(now),
                    'formwork_id':"'{}'".format(formwork_id),
                    'name':"'{}'".format(name),
                    'params':"'{}'".format(str(param_tmp).replace('\'','\"')), # params仅支持双引号插入
                    'file_data':file_data,
                    'file_name':"'{}'".format(file_name),
                    'status':"'{}'".format('SUCCESS'),
                    'type':"'{}'".format(file_type),
                    'full_name':"'{}'".format(full_name),
                    }
                df = pd.DataFrame([p],index=[0])
                df.to_sql(name=self.atch_tb,con=conn,if_exists='append',index=False)


class HSQL:
    def __init__(self) -> None:
        self.env = os.environ.get('ENV', 'dev').rstrip('\r')
        self.config = ConfigParser()
        self.config.read(f'./cfg/config_{self.env}.ini',encoding='utf-8')
        self.load_config()


    def check_env(self):
        env = os.environ.get('ENV', 'dev')
        if env != self.env:
            self.reset(env)

    def reset(self,env):
        self.env = os.environ.get('ENV', 'dev')
        self.config.read(f'config_{env}.ini',encoding='utf')
        self.load_config()

    def load_config(self):
        self.db = self.config.get('db','db_data')
        self.user = self.config.get('db','user')
        self.password = self.config.get('db','password')
        self.host = self.config.get('db','host')
        self.port = self.config.get('db','port')
        self.protocol = self.config.get('db','protocol')
        self.engine = create_engine(f'{self.protocol}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')

    @property
    def db_params(self):
        return dict(database=self.db,user=self.user,password=self.password,host=self.host,port=self.port)

    def reset_db(self,df,tb_name):
        """
        重新加进数据处理后的数据
        """
        with self.engine.connect() as conn:
            df.to_sql(name=tb_name,con=conn,if_exists='replace',index=False)

    def upload_to_db(self,
                     to_upload:pd.DataFrame,
                     tb_name:str,
                     replace_by:str=None):
        """
        替换数据库中的表
        有数据源
        """
        with self.engine.connect() as conn:
            # 如何需要替换，并给出替换的列名
            if replace_by is not None:
                self.delete_from_table(to_upload,tb_name,replace_by,conn)

            # todo 删除和添加分步进行，可能会造成数据丢失！！！
            to_upload.to_sql(name=tb_name,con=conn,if_exists='append',index=False)

    def delete_from_table(self,
                          df,
                          tb_name:str,
                          replace_by:str,
                          con):
        """
        删除原始记录
        """

        condition = self.trans_sql_condition(replace_by,df[replace_by].to_list())
        if condition != '':
            df = pd.read_sql(sql=f'''select * from {tb_name} {condition};''',con=con) 
            if df.empty is False:    
                sql = f'''delete from {tb_name} {condition};'''
                print(f'execute sql:{sql}')
                con.execute(text(sql))

    def load_from_db(self,
                    sql:str)->pd.DataFrame:
        """
        用以装进数据处理后的数据
        """
        with self.engine.connect() as conn:
            df = pd.read_sql(sql=sql,con=conn)
        return df
    
    def execute_sql(self,
                    sql:str):
        """
        执行sql返回结果
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
        return result

    def trans_sql_condition(self,k,v_lst:list)->str:
        if len(v_lst) == 0:
            return ''
        v_lst = self.trans_sql_list(v_lst)
        return f'''where {k} in ({v_lst})'''

    def trans_sql_list(self,lst:list)->str:
        """
        将输入的list转换为sql语句的字符串组合
        """
        lst = list(map(lambda x:"'"+str(x)+"'" if isinstance(x,str) else str(x),lst))
        lst = ','.join(lst)
        lst = lst.replace('nan',"''")
        return lst

    def trans_sql_insert(self,
                        df:pd.DataFrame,
                        sql_tb_name,
                        parition:str=None,
                        p_key:str=None,
                        p_value:str=None,
                        )->str:
        """
        将dataframe的数据转为sql的insert语句
        """
        # 表头
        th = ','.join(df.columns)
        
        td = []
        for v in df.values:
            # 如果是字符串就加引号
            data = self.trans_sql_list(v)
            data = "("+data+")"
            td.append(data)
        td = ','.join(td)

        if parition is None:
            sql = f'''insert into {sql_tb_name} ({th}) values {td}'''
        else:
            sql = f'''insert into {sql_tb_name} partition({p_key}={p_value}) ({th}) values {td}'''
        return sql


if __name__ == '__main__':
    dbt=HSQL()
    sql = f'''select * from t_data_product;'''
    # print(dbt.engine)
    with dbt.engine.connect() as conn:
        df = pd.read_sql(sql=sql,con=conn)
        print(df)