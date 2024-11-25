from airflow import DAG
from datetime import datetime
# from airflow.operators.bash import BashOperator
from datetime import timedelta
import pymssql
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
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
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
#     dag_id='upload_dm_order_adjust_time',
#     default_args=default_args,
#     description='load data adjust time sales',
#     start_date=datetime(2023, 9, 26, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():
# my_path_var = os.getenv('PATHVAR', '')

RawData=pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vSJS-RfhsMKujS7G-2A7O6bm0L_HrM9WcYOGmqROFmkoextOSC2yl81F9MYvlrbatIYtWie1ubbAcs9/pub?gid=0&single=true&output=csv')
RawData['updated_time'] = datetime.now()+ timedelta(hours=7)

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_sales_adjusttime'

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
