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
from hdbcli import dbapi

#-----------------------General Information----------------------------------
#BQ credential
#Set var in local and cloud is difference
credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

#---------------------------------Fetching the data from the selected table using SQL query or Path file--------------------
# Từ dòng DocType là thông tin thêm
sql= """ SELECT
  "DocEntry",
  "DocNum",
  "CardCode",
  "CardName",
  "DocDate",
  "U_RebateDate",
  "U_SONo",
  "NumAtCard",
  "U_InvDate",
  "U_DelDate",
  "U_TransType",
  "U_SOType",
  "Address2",
  "SlpCode",
  "U_SONo_Org",
  "DocType",
  "CANCELED",
  "DocStatus",
  "InvntSttus",
  "Transfered",
  "ObjType",
  "DocCur",
  "Comments",
  "UpdateDate",
  "CreateDate",
  "TaxDate",
  "CancelDate",
  "ExtraMonth",
  "ExtraDays",
  "U_DocNo",
  CURRENT_TIMESTAMP AS "etl_updated"
FROM
  "PGI"."ORIN"
WHERE 1 = 1
  AND "DocDate" >= ADD_MONTHS(CURRENT_DATE, -48)
"""

#---------------------------------Sync to Table------------------------------------------------------------------------------
table_des = 'pgi-dwh.a200_staging_last30days.st_hana_pgi_orin'

#------------------------------- Query delete if have ----------------------------------------
# sql_delete_2 = """DELETE FROM pgi-dwh.b_dw_sap.test_oter_2 WHERE 1 = 1 """
sql_delete = ''

#---------------------------------Schema table destination ------------------------------------------
schema_des = [
    bigquery.SchemaField("DocEntry", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DocNum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CardCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CardName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("U_RebateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("U_SONo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("NumAtCard", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_InvDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("U_DelDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("U_TransType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_SOType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Address2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("SlpCode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("U_SONo_Org", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CANCELED", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocStatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("InvntSttus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Transfered", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ObjType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocCur", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Comments", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("UpdateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("CreateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("TaxDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("CancelDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("ExtraMonth", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ExtraDays", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("U_DocNo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("etl_updated", "TIMESTAMP", mode="NULLABLE"),
    # Thêm các cột khác tương ứng
]
#-----------------------------Connect to DB ------------------------------------------------------------
#lấy data từ server
conn = dbapi.connect(
    address='172.16.18.5',
    port=30015,
    user='USRPGI_BI',
    password='PGI@2024#'
)
# Kiểm tra kết nối
if conn.isconnected():
    print("Kết nối thành công!")
else:
    print("Kết nối thất bại!")

cursor = conn.cursor()

#Processing Query in DataBase_from
cursor.execute(sql)
rows = cursor.fetchall() # Get data
df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description]) # Convert Data to DataFrame for next step

job_config= bigquery.LoadJobConfig(
    schema = schema_des,
    write_disposition = 'WRITE_TRUNCATE'
)

# Begin load data to bigquery
job = client.load_table_from_dataframe(df, table_des, job_config = job_config)

while job.state != 'DONE':
    time.sleep(2)
    job.reload()
print(job.result())

# Close Connect to DataBase
cursor.close()
conn.close()
