from airflow import DAG
from datetime import datetime
# from airflow.operators.bash import BashOperator
from datetime import timedelta
import pyodbc
import pandas as pd
import psycopg2
import datetime
from datetime import datetime
from sqlalchemy import create_engine
import numpy as np
import sys
# from sqlalchemy.engine import URL
import time
# import pyodbc as odbc
# import pymssql as odbc
from google.cloud import bigquery
import os
# import pyarrow
# from airflow.operators.bash import BashOperator
# import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
# from airflow.contrib.hooks.bigquery_hook  import BigQueryHook
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.providers.odbc.hooks.odbc import OdbcHook 
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from customoperator.custom_PostgresToGCSOperator import  custom_PostgresToGCSOperator
# from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator




# default_args= {
#     'owner': 'phuongbi',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2)
# }


# dag =   DAG(
#     dag_id='upload_dm_warehouses_serial',
#     default_args=default_args,
#     description='load data warehouses from server to bigquery',
#     start_date=datetime(2023, 12, 18, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():


#     hook=MsSqlHook(mssql_conn_id="pgi_server")
# Fetching the data from the selected table using SQL query
# my_path_var = os.getenv('PATHVAR', '')
sql= """ with base as
(	
    select t0.itemcode
    ,distnumber
    ,t0.createdate
    ,Loccode
    ,t0.quantity
    ,coalesce(indate,t0.createdate) indate
    ,'quan ly serial' as manage_type
    from dbo.OSRN t0
    inner join (select itemcode,sysnumber,sum(quantity) quantity_sum,max(logentry) LogEntry from dbo.[ITL1] group by itemcode,sysnumber) t1  on t1.ItemCode = t0.ItemCode  and  t1.[SysNumber] = t0.[SysNumber]   
    inner join dbo.[OITL] t2  on t2.[LogEntry] = t1.[LogEntry] 
    where t0.Quantity=1 and t0.QuantOut=0

    union all

    select t0.itemcode,
    null as distnumber,
    t0.createdate
    ,Whscode as Loccode
    ,t0.onhand as quantity
    ,coalesce(t1.LastPurDat,t0.createdate) as indate
    ,'khong quan ly serial' as manage_type
    from dbo.OITW t0
    inner join dbo.OITM t1 on t0.itemcode=t1.itemcode
    where t1.InvntItem = 'Y'
    and t1.ManSerNum= 'N'
    and t1.ManBtchNum= 'N'
    and t0.onhand>0
    )

select 
--t2.distnumber
t2.itemcode
,t2.Loccode
,t3.whsname
,t2.manage_type
,t2.InDate
,t5.ItmsGrpNam
,t4.U_ItmGrp1
,t6.Firmname
,sum(t2.quantity) quantity
from base t2
inner join dbo.OWHS t3 on t2.loccode =t3.Whscode
inner join dbo.OITM t4 on t2.itemcode = t4.itemcode
inner join dbo.OITB t5  on t4.[ItmsGrpCod] = t5.[ItmsGrpCod]
inner join dbo.OMRC t6 on t4.Firmcode=T6.Firmcode
group by     
--t2.distnumber
t2.itemcode
,t2.Loccode
,t3.whsname
,t2.manage_type
,t2.InDate
,t5.ItmsGrpNam
,t4.U_ItmGrp1
,t6.Firmname
order by itemcode"""

import pymssql
conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)



RawData = pd.read_sql_query(sql,conn)
RawData['updated_time'] = datetime.now()+ timedelta(hours=7)
# from datetime import datetime
RawData['timerangetonow'] =RawData['updated_time']-RawData['InDate']
RawData['timerangetonow'] =round(RawData['timerangetonow']/np.timedelta64(1,'D'),0)
RawData['InDate']=RawData.InDate.dt.date
credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
# table_id = 'pgi-dwh.sales.tb_dm_warehouses_serial_distnumber'
table_id = 'pgi-dwh.sales.tb_dm_warehouses_serial'

job_config= bigquery.LoadJobConfig(
autodetect= True,
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(RawData,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())



# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)