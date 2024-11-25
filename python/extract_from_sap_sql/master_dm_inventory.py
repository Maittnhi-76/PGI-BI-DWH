from airflow import DAG
from datetime import datetime
from datetime import timedelta
import pymssql
import pandas as pd
import psycopg2
import datetime
from datetime import datetime
from sqlalchemy import create_engine
import sys
import time
from google.cloud import bigquery
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook




# from sqlserver import SQLserver



# default_args= {
#     'owner': 'phuongbi',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2)
# }


# dag =   DAG(
#     dag_id='upload_dm_inventory',
#     default_args=default_args,
#     description='load data inventory from server to bigquery',
#     start_date=datetime(2023, 6, 27, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():
from datetime import datetime
# my_path_var = os.getenv('PATHVAR', '')
date_note = (datetime.now()+ timedelta(days=-10)).date()

# hook=MsSqlHook(mssql_conn_id="pgi_server")
# Fetching the data from the selected table using SQL query
sql= """ select 
    DocDate	,
    ItemCode ,
    LocCode as WhsCode ,
    InQty	,
    OutQty	,
    Price	
    FROM [PGI_UAT].[dbo].[OIVL] 
    """
    # where DocDate>= '%s'""" %(date_note)

whs = """ select 
    WhsCode	,
    WhsName
    FROM [PGI_UAT].[dbo].[OWHS] 
"""
conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)
RawData = pd.read_sql_query(sql,conn)
RawData1 = pd.read_sql_query(whs,conn)

RawData['updated_time'] = datetime.now()+ timedelta(hours=7)
RawData['DocDate']=RawData.DocDate.dt.date

RawData=pd.merge(RawData, RawData1, how="left", on=["WhsCode", "WhsCode"])

#xóa data 10 ngày gần nhất & cập nhật lại
credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_inventory'

###xóa data 10 ngày & nối data mới nhất
# query = """DELETE FROM `%s`"""  %(table_id)+ """ WHERE docdate>= '%s'"""%(date_note)

# query_job = client.query(query)
# rows = query_job.result()  # Waits for query to finish
# for row in rows:
#     print(row.name)

job_config= bigquery.LoadJobConfig(
autodetect= True,
# write_disposition ='WRITE_APPEND'
write_disposition ='WRITE_TRUNCATE'

)

job = client.load_table_from_dataframe(RawData,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())

    # # Tạo bảng mới truncate tổng tồn ngày hiện tại

    # RawData2 = RawData.groupby(['ItemCode'])['InQty','OutQty'].sum().reset_index()
    # RawData2['quanntity']=RawData2['InQty'] - RawData2['InQty']
    # table_id2 = 'pgi-dwh.sales.tb_current_inventory'
    # job_config2= bigquery.LoadJobConfig(
    # autodetect= True,
    # write_disposition ='WRITE_TRUNCATE'
    # )
    # job = client.load_table_from_dataframe(RawData2,table_id2,job_config=job_config2)

    # while job.state!='DONE':
    #     time.sleep(2)
    #     job.reload()
    # print(job.result())



# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)