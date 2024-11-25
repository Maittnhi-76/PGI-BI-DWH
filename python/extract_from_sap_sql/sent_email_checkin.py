import sys
import os
import time
import pymssql
import pyodbc
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import numpy as np

#------------------------------Định nghĩa function-----------------
#Hàm để đọc sql từ path
def execute_sql_from_file(sql_file_path):
    """Đọc truy vấn từ tệp .sql"""
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        query = file.read()
    return query

# Hàm để lấy dữ liệu từ SQL Server
def get_data_from_sql_server(var_server, var_user, var_password, var_database, var_port, var_sqlinput):
    # Khai báo chuỗi truy vấn SQL
    # sql_input = var_sqlinput
    sql_input = execute_sql_from_file(var_sqlinput)

    # Kết nối đến cơ sở dữ liệu SQL Server
    conn = pymssql.connect(
        server=var_server,
        user=var_user,
        password=var_password,
        database=var_database,
        port=var_port,
    )

    # Thực hiện truy vấn và lấy dữ liệu vào DataFrame
    datafarm_outcome = pd.read_sql_query(sql_input, conn)
    datafarm_outcome['updated_time'] = datetime.now() + timedelta(hours=7)
    
    # Đóng kết nối
    conn.close()
    
    return datafarm_outcome
# Hàm để tải dữ liệu lên BigQuery
def load_data_to_bigquery(var_dataframe, var_table_id, var_write_disposition = 'WRITE_TRUNCATE'):
    # Cấu hình đường dẫn cho credentials của BigQuery
    credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    # Kết nối đến BigQuery và tải dữ liệu vào bảng
    client = bigquery.Client()
    # table_id = var_table_id

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition= var_write_disposition
    )

    job = client.load_table_from_dataframe(var_dataframe, var_table_id, job_config=job_config)

    # Đợi cho công việc tải dữ liệu hoàn thành
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()

    # In kết quả của công việc tải
    return print(job.result())

def get_data_from_bigquery(var_pathinputsql):
    # BQ credential
    credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    client = bigquery.Client()

    # lấy ngày trong tháng từ Bigquery
    inputsql = execute_sql_from_file(var_pathinputsql)
    # inputsql = var_pathinputsql
    result = client.query(inputsql).result().to_dataframe()
    return result

#----------------------Dùng function------------------------------------------------------------------------

def sent_email_checkinout ():
#--------------------------------------------------------------------------------------
    # lấy ngày trong tháng từ Bigquery
    var_pathinputsql_raw = '/python/old_modules_bi/sqlfile/checkinout_email.sql'
    df = get_data_from_bigquery(var_pathinputsql_raw)

#--------------------------------------------------------------------------------------
    # Lấy danh sách email - CSV : google sheets
    var_pathinputsql_email = '/python/old_modules_bi/sqlfile/checkinout_email_listemail.sql'
    email = get_data_from_bigquery(var_pathinputsql_email)

#--------------------------------------------------------------------------------------
    # Vòng lặp gửi mail
    for index,row in email.iterrows():
        email_from ='pgi.bi.main@gmail.com'
        password= 'dudd smoc fgxb egsx'
        email_to=  row['email']
        # email_to=  'nhi.mai@pgi.com.vn'

        date_note = (pd.Timestamp.today()- timedelta(days=1)).strftime("%Y-%m")
        df1=df[df['machamcong']==row['machamcong']]
        # df1.columns=['Tên Nhân Viên','Tên Chấm Công','Mã Chấm Công','Ngày','Thứ', 'Giờ Vào', 'Giờ ra','Thời gian làm việc']
        # df1=df1[['Ngày','Thứ', 'Giờ Vào', 'Giờ ra']]
        df1.columns=['Tên Nhân Viên','Tên Chấm Công','Mã Chấm Công','Ngày','Thứ', 'Giờ Vào', 'Giờ ra','Thời gian làm việc']
        df1=df1[['Ngày','Thứ', 'Giờ Vào', 'Giờ ra']]
        # df1["Giờ Vào"].replace('NaT', '-',inplace=True)
        df1["Giờ Vào"] = df1["Giờ Vào"].astype(object).where(df1["Giờ Vào"].notnull(),"")
        df1["Giờ ra"] = df1["Giờ ra"].astype(object).where(df1["Giờ ra"].notnull(),"")
        # df1.fillna("",inplace=True)

        #---------------------------------

        #-------------------------------------
        html_table =df1.to_html(classes='table table-stripped',index=False,float_format='{:20,.2f}'.format,justify='left')
        #---------------------------------
        #Vòng lặp format bảng, tô màu ngày chủ  nhật
        i=0
        color=""" bgcolor="#D6EEEE" """
        n=len(html_table)
        while i<n and i>-1:
            a=html_table.find("Chủ Nhật",i)
            if a>-1:
                html_table=html_table[0:a-38]+color+html_table[a-38:]
            else:
                break
            n=n+19
            i=a+27
        #------------------------------------------
        #Body gửi mail
        HTMLBody= """<!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                </head>
                <body>

                <p>Dear anh/chị """+ row['tennhanvien']  +f""",</p>

                <p>Phòng Nhân sự xin gửi thông tin chấm công tháng {date_note} như bên dưới:</p>

                </body>
                </html> """ + html_table + """

                <p>Đối với các ngày làm việc bị thiếu chấm công, Anh/Chị vui lòng làm request bổ sung chấm công, phép hoặc xin đi trễ/về sớm chậm nhất là 3 ngày làm việc kể từ khi nhận email này </p>            

                <p>Vui lòng không phản hồi email tự động này, có bất kỳ thắc mắc hoặc lỗi vui lòng email: </p>
                <p>HR: phung.le@pgi.com.vn hoặc liên hệ skype: Phung Le</p>
                <p>IT: nhi.mai@pgi.com.vn hoặc liên hệ skype: Mai Nhi</p>
                <p>Thanks & Best Regards </p>
                """ 
        email_mesage=MIMEMultipart()
        email_mesage['From']=email_from
        email_mesage['To']=email_to
        email_mesage['Subject'] = f'Thông Tin Chấm Công Trong Tháng - {date_note}'
        email_mesage.attach(MIMEText(HTMLBody,"html"))
        email_string =email_mesage.as_string()

        context =ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com',465,context=context) as server:
            server.login(email_from,password)
            server.sendmail(email_from,email_to,email_string.encode('utf-8'))
        # print('mail sent')

sent_email_checkinout()
