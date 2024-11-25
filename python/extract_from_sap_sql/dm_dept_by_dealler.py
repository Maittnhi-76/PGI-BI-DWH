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
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook



# from sqlserver import SQLserver



# default_args= {
#     'owner': 'phuongbi',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2)
# }


# dag =   DAG(
#     dag_id='upload_dept',
#     default_args=default_args,
#     description='load data dept',
#     start_date=datetime(2024, 3, 17, 9),
#     schedule_interval= '@daily'
# ) 


conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)
sql= """ Select CardCode,CardName,Balance,BalanceFC,BalanceSys from [PGI_UAT].[dbo].[OCRD]  where CardType='C'""" 


RawData = pd.read_sql_query(sql,conn)
RawData['updated_time'] = datetime.now()+ timedelta(hours=7)

credentials_path = '/home/stackops/airflow29/schedule_cron/bqprivatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.Accounting.tb_dm_dept'


job_config= bigquery.LoadJobConfig(
autodetect= True,
write_disposition ='WRITE_TRUNCATE'
# write_disposition ='WRITE_APPEND'
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