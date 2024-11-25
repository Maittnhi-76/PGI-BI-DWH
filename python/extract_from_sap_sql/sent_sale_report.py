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

def sent_email_sale_report ():
#--------------------------------------------------------------------------------------
    # lấy ngày trong tháng từ Bigquery
    var_pathinputsql_raw = '/python/old_modules_bi/sqlfile/sale_report_mail.sql'
    category_newallocate = get_data_from_bigquery(var_pathinputsql_raw)
    category_newallocate=category_newallocate.astype(dtype={'TARGET_MONTH': np.float64, 'TARGET_QUARTER': np.float64})

    #code gửi mail
    for index,row in category_newallocate.iterrows():

        email_from ='pgi.bi.main@gmail.com'
        password= 'dudd smoc fgxb egsx'
        email_to=  row['EMAIL']
        # email_to= 'thien.vo@pgi.com.vn'
        if email_to is None:
                email_to = 'thien.vo@pgi.com.vn'

        date_note = pd.Timestamp.today().strftime("%Y-%m-%d")
    

        df=category_newallocate.loc[category_newallocate['SALES']==row['SALES']]
        df=df[['SALES','TARGET_MONTH','ACT_MTD','MTD_ACT_PERCENT','TARGET_QUARTER','ACT_QTD','QTD_ACT_PERCENT']]
        df.columns=['SALES','TARGET MONTH','ACT MTD','%MTD ACT','TARGET QUARTER','ACT QTD','%QTD ACT']
        # df=df.style.format({'ACT_PERCENT':'{:.2%}'})
        # df = pd.DataFrame(category_newallocate,category_newallocate['SALES']==row['SALES'])

        html_table =df.to_html(classes='table table-stripped',index=False,float_format='{:20,.0f}'.format)
        HTMLBody="""<!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                </head>
                    <body>

                    <p>Dear anh/chị """ + row['SALES']+""",</p>

                    <p>Bộ phận BI gửi báo cáo doanh số MTD </p>

                </body>
                </html> """ + html_table + """
                <p>Chi tiết vui lòng bấm vào link: </p> <a href="https://lookerstudio.google.com/reporting/c3ca842e-c85f-408d-85da-3002bdf598d3">Sales Report link</a> <br>

                <p>Vui lòng không phản hồi email tự động này, có bất kỳ thắc mắc hoặc lỗi báo cáo vui lòng email thien.vo@pgi.com.vn hoặc liên hệ skype: Thiện Võ</p>
                <p>Thanks & Best Regards </p>
                """  
        email_mesage=MIMEMultipart()
        email_mesage['From']=email_from
        email_mesage['To']=email_to
        email_mesage['Subject'] = f'Sales Report - {date_note}'
        email_mesage.attach(MIMEText(HTMLBody,"html"))
        email_string =email_mesage.as_string()

        context =ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com',465,context=context) as server:
            server.login(email_from,password)
            server.sendmail(email_from,email_to,email_string.encode('utf-8'))
        print('mail sent')

sent_email_sale_report()
