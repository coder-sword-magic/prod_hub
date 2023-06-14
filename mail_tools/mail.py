from functools import cached_property
from typing import Union,Optional
from datetime import datetime, timedelta
import poplib,smtplib
from email.parser import Parser
from email.header import decode_header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dateutil import parser
import pandas as pd
import os
from tqdm import tqdm
from sqlalchemy.orm import DeclarativeBase,Mapped,mapped_column,sessionmaker
from sqlalchemy import create_engine,func
import numpy as np
from tools.toolkits import strQ2B

# 采集邮件，存入mail.db
class Base(DeclarativeBase):
    pass

class Content(Base):
    __tablename__ = 'mail_content'

    loc:Mapped[int] = mapped_column(primary_key=True) 
    date:Mapped[datetime] = mapped_column(nullable=False)
    subject:Mapped[str] = mapped_column(nullable=False)
    fr:Mapped[str] = mapped_column(nullable=False)
    to:Mapped[str] = mapped_column(nullable=False)
    cc:Mapped[str] = mapped_column(nullable=True)
    bcc:Mapped[str] = mapped_column(nullable=True)
    reply_to:Mapped[str] = mapped_column(nullable=True)
    sender:Mapped[str] = mapped_column(nullable=True)
    atch_names:Mapped[str] = mapped_column(nullable=True)
    byte_content:Mapped[bytes] = mapped_column(nullable=True)

class Hmail:
    
    def __init__(self,
                 pop3_server:Optional[str]=None,
                 user:Optional[str]=None,
                 password:Optional[str]=None,
                 server_type:str='pop3',
                 db_engine:str='sqlite',
                 db_name:str='mail.db',
                 workers:int=3,
                 ) -> None:
        self.pop3_server = pop3_server
        self.user = user
        self.password = password
        self.server_type = server_type
        self.workers = workers
        self.msg = list()
        
        # 初始化数据库引擎
        if db_engine == 'sqlite':
            if not os.path.exists(db_name):
                os.makedirs(db_name)
            self.engine = create_engine(f'{db_engine}:///{db_name}')
        else:
            self.engine = create_engine(db_engine)
        self.Session = sessionmaker(bind=self.engine)

    def decode_email(self,byte_content:bytes)->str:
        valid_charsets=['utf-8','gbk','gb2312','gb18030']
        for charset in valid_charsets:
            try:
                decode_str =  byte_content.decode(charset)
                return decode_str
            except:
                continue

    def load_from_db(self):
        '''
        从数据库读取消息
        '''
        sql = f'''select * from {Content.__tablename__}'''
        self.mail_content = pd.read_sql(sql,self.engine)
        return self.mail_content


    @cached_property
    def overlook_downloaded(self):
        return pd.DataFrame(self.msg)

    def parse_attachment_names(self,msg)->str:
        """
        获得附件名称
        """
        filenames = list()
        
        for part in msg.walk():
            if part.get_content_maintype()=='multipart':
                continue

            filename:str = part.get_filename()

            if filename:
                filenames.append(self.parse_header(filename))
        
        return ','.join(filenames)

    def download_attachment(self,
                            i:int, # 邮件记录的位置
                            save_path:str='downloads',
                            ):
        """
        下载附件到文件夹，弃用
        """
        import os
        if os.path.exists(save_path) is False:
            os.makedirs(save_path)

        for part in self.msg_content[i].walk():
            if part.get_content_maintype()=='multipart':
                continue

            filename:str = part.get_filename()

            if filename:
                with open(os.path.join(save_path,filename),'wb') as f:
                    f.write(part.get_payload(decode=True))
                    print('保存附件:',filename)
                    
    def _login_server(self):
        """
        登录邮件服务器
        """
        if self.pop3_server is None:
            raise Exception('pop3_server is None')
        self.server = poplib.POP3(self.pop3_server)
        self.server.user(self.user)
        self.server.pass_(self.password)
        return self.server


    def parse_header(self,raw:str):
        """
        """
        value,charset = decode_header(raw)[0]
        charset = 'utf-8' if charset is None else charset
        if isinstance(value,bytes):
            value=strQ2B(str(value,encoding=charset,errors='ignore'))
        if isinstance(value,str):
            return strQ2B(value)
        else:
            return raw
    
    def parse_byte_to_msg(self,byte_content:bytes)->Parser:
        """
        将二进制字节码转为mail.Message
        """
        msg = Parser().parsestr(self.decode_email(byte_content))
        return msg

    def parse_mail(self,
                   byte_content:Union[list,bytes], # 邮箱返回的二进制字节码数组
                   )->dict:
        """
        byte_list 为python mail库retr返回的字节码数组
        """
        if isinstance(byte_content,list):
            byte_content=b'\r\n'.join(byte_content)
        
        msg = self.parse_byte_to_msg(byte_content)
        d = dict()
        d['date'] = parser.parse(msg['Date']).date() 
        d['subject'] = self.parse_header(msg['Subject'])
        d['fr'] = self.parse_header(msg['From'])
        d['to'] = msg['To']
        d['cc'] = msg['Cc']
        d['bcc'] = msg['Bcc']
        d['reply_to'] = msg['Reply-To']
        d['sender'] = msg['Sender']
        d['atch_names'] = self.parse_attachment_names(msg)
        d['byte_content'] = byte_content

        return d
    
    def insert_db(self,d:dict):
        payload = Content(**d)
        with self.Session() as session:
            session.add(payload)
            session.commit()

    def fetch_email(self,
                    stop_before_date:Union[str,datetime]=None,
                    limit:Optional[int]=None,
                    ):
        
        # 获得库中已经存在的邮件列表
        with self.Session() as session:
            record_list = session.query((Content.loc)).all()
            record_list = np.array(record_list).flatten()

        # 检查时间类型
        if isinstance(stop_before_date,str):
            stop_before_date=parser.parse(stop_before_date).date()
        
        # 连接邮件服务器
        server = self._login_server()
        # 获取服务器邮件总数
        index = len(server.list()[1])
        
        print(f"[{datetime.now().time()}]共({index})封邮件，共下载：{limit}，开始搜索邮件...")

        for i in tqdm(range(index,0,-1)):
            # 设置接收邮件的上限
            if isinstance(limit,int):
                if limit==0:
                    break
                limit -= 1

            # 数据库中已经存在记录，不下载
            if i in record_list:
                continue
            
            # 访问服务器下载邮件
            try:
                _, lines, _ = self.server.retr(i)
            except poplib.error_proto:
                print('occur error proto:',i)
                continue
            
            # 解析邮件
            try:             
                row = self.parse_mail(byte_content=lines) 
                row['loc']=i
                self.msg.append(row)
                self.insert_db(d=row)


            except Exception as e:
                print('occur error: ',e,' at ',i,'  ',lines)
            
            if stop_before_date is not None and row['date'] < stop_before_date:
                    break

        server.quit()
    

    def send_mail(self,
                  subject:str,
                  sender:str,
                  receiver:str,
                  mail_text:str,
                  atch_path_lst:list,
                  encoding:str):

        message = MIMEMultipart()

        content = MIMEText(mail_text, 'html', 'utf-8')
        message.attach(content)

        for atch_path in atch_path_lst:
            fn = atch_path.split('/')[-1]
            file = MIMEText(open(atch_path, 'rb').read(), 'base64', encoding)
            file.add_header('Content-Disposition', 'attachment', file_name=fn)
            message.attach(file)

        message['Subject'] = subject
        message['To'] = receiver
        message['From'] = sender

        smtpObj = smtplib.SMTP(self.pop3_server)
        smtpObj.connect(self.pop3_server, 25)
        smtpObj.starttls()
        smtpObj.login(self.user, self.password)
        smtpObj.sendmail(sender, receiver, message.as_string())
        smtpObj.quit()

