from datetime import datetime
from datetime import timedelta
import pyodbc
import pandas as pd
import datetime
from datetime import datetime
import sys
import time
from google.cloud import bigquery
import os
from datetime import datetime, timedelta

# my_path_var = os.getenv('PATHVAR', '')


RawData=pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vT2Q_XjpN5Ur0RHVA5OWyHM4B1OC5R9CR0PEMfxmj8hMl1nKGTl9jlAo1-5jv34KV1o_TUo_7cEiL9e/pub?gid=0&single=true&output=csv')
RawData['updated_time'] = datetime.now()+ timedelta(hours=7)

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_staffs_mail'
# table_id = 'pgi-dwh.sales.tb_dm_staffs_mail_thienvo'

job_config= bigquery.LoadJobConfig(
autodetect= True,
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(RawData,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())
