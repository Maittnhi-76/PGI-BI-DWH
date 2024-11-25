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
#     dag_id='upload_dm_products',
#     default_args=default_args,
#     description='load data products from server to bigquery',
#     start_date=datetime(2023, 6, 27, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():


    # hook=MsSqlHook(mssql_conn_id="pgi_server")
# my_path_var = os.getenv('PATHVAR', '')
# Fetching the data from the selected table using SQL query
sql_OITM= """ select
    ItemCode	,
    ItmsGrpCod,
    ItemName	,
    FrgnName	,
    OnHand	,
    BuyUnitMsr	,
    LastPurPrc	,
    LastPurCur	,
    LastPurDat	,
    SWeight1	,
    BHeight1	,
    BWidth1	,
    BLength1	,
    BVolUnit	,
    CreateDate	,
    UpdateDate	,
    AvgPrice	,
    Deleted	,
    DocEntry	,
    FirmCode,
    U_CostAct1	,
    U_CostAct2	,
    U_OldItemCode	,
    U_ItmGrp1	,
    U_ItmGrp2	,
    U_Specific	,
    U_ItmCodWb	,
    U_ItmNamWb	,
    U_ItmNamAcct	
    FROM [PGI_UAT].[dbo].[OITM]"""

sql_OITB= """ select
    ItmsGrpCod	,
    ItmsGrpNam	,
    U_CostAct1	,
    U_ReportGroup	

    FROM [PGI_UAT].[dbo].[OITB]"""

sql_OMRC= """ select
    FirmCode,
    FirmName	
    FROM [PGI_UAT].[dbo].[OMRC]"""

bq_category_newallocate = """ select department as department1,newgroup,ItmsGrpNam
    From pgi-dwh.sales.category_newallocate_manual"""

bq_category_review = """ select ItemCode,department ,groupnew
From pgi-dwh.sales.category_review_manual"""

#lấy data từ server

import pymssql
conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)


df_OITM = pd.read_sql_query(sql_OITM,conn)
df_OITB = pd.read_sql_query(sql_OITB,conn)
df_OMRC = pd.read_sql_query(sql_OMRC,conn)

#BQ credential
credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()

#Lấy data BQ

category_newallocate = client.query(bq_category_newallocate).result().to_dataframe()
category_review = client.query(bq_category_review).result().to_dataframe()

#merge data lại

Raw1=pd.merge(df_OITM, df_OITB, how="left", on=["ItmsGrpCod", "ItmsGrpCod"])
Raw2=pd.merge(Raw1, df_OMRC, how="left", on=["FirmCode", "FirmCode"])
Raw3=pd.merge(Raw2, category_review, how="left", on=["ItemCode", "ItemCode"])
RawData=pd.merge(Raw3, category_newallocate, how="left", on=["ItmsGrpNam", "ItmsGrpNam"])
RawData['department'] = RawData.department.fillna(RawData.department1)
RawData['groupnew'] = RawData.groupnew.fillna(RawData.newgroup)
RawData.drop(['department1','newgroup'],axis='columns',inplace=True)


RawData['updated_time'] = datetime.now()+ timedelta(hours=7)


table_id = 'pgi-dwh.sales.tb_dm_product'

job_config= bigquery.LoadJobConfig(
autodetect= True,
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(RawData,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())


# get_server_dag = get_server()

# get_server_dag = DummyOperator(task_id="get_server_dag", dag=dag)

# get_server_dag


# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)