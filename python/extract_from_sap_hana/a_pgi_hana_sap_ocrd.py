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
  "CardCode",
  "CardName",
  "CardType",
  "Address",
  "ZipCode",
  "CntctPrsn",
  "Balance",
  "GroupNum",
  "CreditLine",
  "DebtLine",
  "SlpCode",
  "Currency",
  "City",
  "County",
  "Country",
  "E_Mail",
  "FatherCard",
  "CardFName",
  "FatherType",
  "CreateDate",
  "UpdateDate",
  "Priority",
  "sEmployed",
  "chainStore",
  "ObjType",
  "Block",
  "ChannlBP",
  "StreetNo",
  "U_CusGrp02",
  "U_CusGrp03",
  "U_CusGrp04",
  "U_BPMkt",
  "U_BPOldCode",
  "U_EmpDept",
  "U_StreetNo",
  "U_Wards",
  "U_District",
  "U_Province",
  "U_Area",
  "U_Region",
  "U_Country",
  CURRENT_TIMESTAMP AS "etl_updated"
FROM
  "PGI"."OCRD"
WHERE 1 = 1
"""

#---------------------------------Sync to Table------------------------------------------------------------------------------
table_des = 'pgi-dwh.a200_staging_last30days.st_hana_pgi_ocrd'

#------------------------------- Query delete if have ----------------------------------------
# sql_delete_2 = """DELETE FROM pgi-dwh.b_dw_sap.test_oter_2 WHERE 1 = 1 """
sql_delete = ''

#---------------------------------Schema table destination ------------------------------------------
schema_des = [
    bigquery.SchemaField("CardCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CardName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CardType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ZipCode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CntctPrsn", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Balance", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("GroupNum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CreditLine", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("DebtLine", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SlpCode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Currency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("City", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("County", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("E_Mail", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("FatherCard", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CardFName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("FatherType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CreateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("UpdateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("Priority", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("sEmployed", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("chainStore", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ObjType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Block", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ChannlBP", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("StreetNo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_CusGrp02", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_CusGrp03", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_CusGrp04", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_BPMkt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_BPOldCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_EmpDept", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_StreetNo", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("U_Wards", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_District", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_Province", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_Area", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_Region", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_Country", "STRING", mode="NULLABLE"),
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
