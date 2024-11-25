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
  "WhsCode",
  "WhsName",
  "Street",
  "Block",
  "ZipCode",
  "City",
  "County",
  "Country",
  "Location",
  "DropShip",
  "objType",
  "createDate",
  "updateDate",
  "StreetNo",
  "U_Branch",
  CURRENT_TIMESTAMP AS "etl_updated"
FROM
  "PGI"."OWHS"
WHERE 1 = 1
"""

#---------------------------------Sync to Table------------------------------------------------------------------------------
table_des = 'pgi-dwh.a200_staging_last30days.st_hana_pgi_owhs'

#------------------------------- Query delete if have ----------------------------------------
# sql_delete_2 = """DELETE FROM pgi-dwh.b_dw_sap.test_oter_2 WHERE 1 = 1 """
sql_delete = ''

#---------------------------------Schema table destination ------------------------------------------
schema_des = [
    bigquery.SchemaField("WhsCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("WhsName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Street", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Block", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ZipCode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("City", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("County", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Country", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Location", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("DropShip", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("objType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("updateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("StreetNo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_Branch", "STRING", mode="NULLABLE"),
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
