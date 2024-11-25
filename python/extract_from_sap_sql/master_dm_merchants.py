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
# import numpy as np
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
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

# from airflow.models.connection import Connection

# from airflow.hooks.dbapi import DbApiHook



# from sqlserver import SQLserver



# default_args= {
#     'owner': 'phuongbi',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2)
# }


# dag =   DAG(
#     dag_id='upload_dm_merchants',
#     default_args=default_args,
#     description='load data merchants from server to bigquery',
#     start_date=datetime(2023, 6, 27, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():


#     hook=MsSqlHook(mssql_conn_id="pgi_server")
# my_path_var = os.getenv('PATHVAR', '')
# Fetching the data from the selected table using SQL query
sql1= """ select
    CardCode	,
    CardName	,
    CardType	,
    GroupCode	,
    CmpPrivate	,
    Address	,
    ZipCode	,
    MailAddres	,
    MailZipCod	,
    Phone1	,
    Phone2	,
    Fax	,
    DebtLine	,
    LicTradNum	,
    SlpCode	,
    E_Mail	,
    CardFName	,
    CreateDate	,
    UpdateDate	,
    DebPayAcct	,
    ShipToDef	,
    BillToDef	,
    CreateTS	,
    UpdateTS	,
    U_CostAct1	,
    U_Capital	,
    U_TerrDesc	,
    U_TerrIDLocation	,
    U_Email,
    ChannlBP ,
    U_TerrID
    from [PGI_UAT].[dbo].[OCRD] 
    
    """

sql2= """ select
    CardCode,
    Address,
    Street,
    AdresType 
    from [PGI_UAT].[dbo].[CRD1] 
    
    """

sql3 = """ select *

    from [PGI_UAT].[dbo].[S1_Region]
    """
import pymssql
conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)

RawData = pd.read_sql_query(sql1,conn)
RawData2 = pd.read_sql_query(sql2,conn)
RawData3 = pd.read_sql_query(sql3,conn)

Raw1=pd.merge(RawData, RawData2, how="inner", on=["CardCode", "CardCode"])
Raw=pd.merge(Raw1, RawData3, how="left", left_on=["U_TerrID"],right_on=["Territory"])
# RawData=pd.read_csv('plugins/kho2.6.csv')
Raw['updated_time'] = datetime.now()+ timedelta(hours=7)

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_merchants'

job_config= bigquery.LoadJobConfig(
autodetect= True,
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(Raw,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())


# get_server_dag = get_server()

# get_server_dag = DummyOperator(task_id="get_server_dag", dag=dag)

# get_server_dag


# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)