import sys
import os
import time
import pymssql
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery


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

def get_and_load_data():
    var_sqlinput='/python/old_modules_bi/sqlfile/dataloginoutusersap.sql'
    var_server='192.168.60.252'
    var_user='BA'
    var_password='PGI@123'
    var_database='PGI_UAT'
    var_port=1433
    
    var_dataframe=get_data_from_sql_server(var_server, var_user, var_password, var_database, var_port, var_sqlinput)
    var_table_id='pgi-dwh.sales.dataloginoutusersap'
    load_data_to_bigquery(var_dataframe, var_table_id, var_write_disposition = 'WRITE_TRUNCATE')

get_and_load_data()