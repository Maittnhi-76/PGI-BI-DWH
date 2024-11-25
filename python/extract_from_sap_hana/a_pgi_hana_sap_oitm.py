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
  "ItemName",
  "FrgnName",
  "ItmsGrpCod",
  "CstGrpCode",
  "VatGourpSa",
  "CodeBars",
  "PrchseItem",
  "SellItem",
  "InvntItem",
  "OnHand",
  "DfltWH",
  "BuyUnitMsr",
  "NumInBuy",
  "SalUnitMsr",
  "NumInSale",
  "SerialNum",
  "LastPurPrc",
  "LastPurCur",
  "LastPurDat",
  "SHeight1",
  "SHght1Unit",
  "SWidth1",
  "SWdth1Unit",
  "SLength1",
  "SLen1Unit",
  "SVolume",
  "SVolUnit",
  "SWeight1",
  "SWght1Unit",
  "BHeight1",
  "BHght1Unit",
  "FirmCode",
  "LstSalDate",
  "QryGroup1",
  "QryGroup2",
  "QryGroup3",
  "QryGroup4",
  "CreateDate",
  "UpdateDate",
  "SalFactor1",
  "SalFactor2",
  "SalFactor3",
  "SalFactor4",
  "PurFactor1",
  "PurFactor2",
  "PurFactor3",
  "PurFactor4",
  "VatGroupPu",
  "DocEntry",
  "ByWh",
  "ItemType",
  "BaseUnit",
  "StockValue",
  "InvntryUom",
  "IUoMEntry",
  "AssetClass",
  "CapDate",
  "AcqDate",
  "U_ItemGrp02",
  "U_ItemGrp03",
  "U_ItemGrp04",
  "U_ItemBrand",
  "U_ItemColor",
  "U_ItemModel",
  "U_ItemOrigin",
  "U_ItemProducer",
  "U_ItemSpec",
  "U_ItemHSCode",
  "U_ItemInvName",
  "U_FACCCode",
  "U_ItemProCod",
  "U_ItemProCodHarman",
  CURRENT_TIMESTAMP AS "etl_updated"
FROM
  "PGI"."OITM"
WHERE 1 = 1
"""

#---------------------------------Sync to Table------------------------------------------------------------------------------
table_des = 'pgi-dwh.a200_staging_last30days.st_hana_pgi_oitm'

#------------------------------- Query delete if have ----------------------------------------
# sql_delete_2 = """DELETE FROM pgi-dwh.b_dw_sap.test_oter_2 WHERE 1 = 1 """
sql_delete = ''

#---------------------------------Schema table destination ------------------------------------------
schema_des = [
    bigquery.SchemaField("ItemCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ItemName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("FrgnName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ItmsGrpCod", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CstGrpCode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("VatGourpSa", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CodeBars", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("PrchseItem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("SellItem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("InvntItem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("OnHand", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("DfltWH", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("BuyUnitMsr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("NumInBuy", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SalUnitMsr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("NumInSale", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SerialNum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("LastPurPrc", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("LastPurCur", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("LastPurDat", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("SHeight1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SHght1Unit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("SWidth1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SWdth1Unit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("SLength1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SLen1Unit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("SVolume", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SVolUnit", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("SWeight1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SWght1Unit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("BHeight1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("BHght1Unit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("FirmCode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("LstSalDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("QryGroup1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("QryGroup2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("QryGroup3", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("QryGroup4", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CreateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("UpdateDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("SalFactor1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SalFactor2", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SalFactor3", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("SalFactor4", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("PurFactor1", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("PurFactor2", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("PurFactor3", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("PurFactor4", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("VatGroupPu", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DocEntry", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ByWh", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ItemType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("BaseUnit", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("StockValue", "NUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("InvntryUom", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("IUoMEntry", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("AssetClass", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CapDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("AcqDate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemGrp02", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemGrp03", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemGrp04", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemBrand", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemColor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemModel", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemOrigin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemProducer", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemSpec", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemHSCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemInvName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_FACCCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemProCod", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("U_ItemProCodHarman", "STRING", mode="NULLABLE"),
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

print(os.name)
print('hello')