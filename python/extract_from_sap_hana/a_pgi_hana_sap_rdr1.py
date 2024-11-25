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
  "ItemCode",
  "Dscription",
  "unitMsr",
  "U_PMType",
  "Quantity",
  "Price",
  "LineTotal",
  "U_BaseItem",
  "U_DiscountPercent",
  "VatSum",
  "StockPrice",
  "StockValue",
  "U_PMCode",
  "WhsCode",
  "DocEntry",
  "VatGroup",
  "LineNum",
  "TargetType",
  "LineStatus",
  "Currency",
  "Rate",
  "SerialNum",
  "DocDate",
  "BaseCard",
  "InvntSttus",
  "ObjType",
  "ActDelDate",
  "U_RevCheck",
  CURRENT_TIMESTAMP AS "etl_updated"
FROM
  "PGI"."RDR1"
WHERE 1 = 1
  AND "DocDate" >= ADD_MONTHS(CURRENT_DATE, -48)
"""

#---------------------------------Sync to Table------------------------------------------------------------------------------
table_des = 'pgi-dwh.a200_staging_last30days.st_hana_pgi_rdr1'

#------------------------------- Query delete if have ----------------------------------------
# sql_delete_2 = """DELETE FROM pgi-dwh.b_dw_sap.test_oter_2 WHERE 1 = 1 """
sql_delete = ''

#---------------------------------Schema table destination ------------------------------------------
schema_des = [
    bigquery.SchemaField("ItemCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Dscription", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unitMsr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_PMType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Quantity", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("Price", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("LineTotal", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("U_BaseItem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_DiscountPercent", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("VatSum", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("StockPrice", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("StockValue", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("U_PMCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("WhsCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocEntry", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("VatGroup", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("LineNum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("TargetType", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("LineStatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Currency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Rate", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SerialNum", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("BaseCard", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("InvntSttus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ObjType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ActDelDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("U_RevCheck", "STRING", mode="NULLABLE"),
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
df['TargetType'] = df['TargetType'].astype(float) #Trường hợp đặc biệt có float số âm
# print(df['TargetType'].head())
# print(df['TargetType'].unique())

job_config= bigquery.LoadJobConfig(
    schema = schema_des,
    # autodetect= True,
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
