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
#     dag_id='dm_purchase_detail',
#     default_args=default_args,
#     description='load data purchase order detail from server to bigquery',
#     start_date=datetime(2023, 6, 27, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():

   
# hook=MsSqlHook(mssql_conn_id="pgi_server")
# Fetching the data from the selected table using SQL query
from datetime import datetime
# my_path_var = os.getenv('PATHVAR', '')
date_note = (datetime.now()+ timedelta(days=-30)).date()


# đơn mua hàng detail - lấy từ invoice
sql_PCH1= """ select 
    DocEntry	,
    LineNum	,
    TargetType	,
    TrgetEntry	,
    BaseType	,
    LineStatus	,
    InvntSttus,
    ItemCode	,
    Dscription	,
    Quantity	,
    ShipDate	,
    Price	,
    rate,
    Currency	,
    LineTotal	,
    DocDate	,
    GrssProfit	,
    unitMsr	,
    ShipToCode	,
    ShipToDesc	,
    OcrCode3	,
    CogsOcrCo2	,
    U_S1_BaseNotes	,
    U_OrigiPrice	,
    U_DiscAmt	,
    U_DiscPrcnt	,
    U_PriceDisc	,
    WhsCode	,
    BaseCard	,
    TaxCode	,
    U_VoucherCode,
    VendorNum,
    GTotal

    from [PGI_UAT].[dbo].[PCH1] 
    """

    # where DocDate>= '%s'""" %(date_note)



    #đơn hàng tổng
sql_OPCH= """ select 
    DocEntry	,
    DocNum	,
    DocType	,
    CANCELED	,
    DocStatus	,
    DocDueDate	,
    DocTotal	,
    GrosProfit	,
    JrnlMemo	,
    DocTotalSy	,
    PaidSys	,
    GrosProfSy	,
    UpdateDate	,
    CreateDate	,
    TaxDate	,
    ReqDate	,
    ExtraDays	,
    SlpCode	,
    U_S1No   	,
    U_CardCode	,
    U_CardName	,
    U_NoteForAll,
    U_NoteForAcc	,
    U_NoteForWhs	,
    U_NoteForStatus	,
    U_Store	,
    U_TotalQty	,
    U_NoteForLogistic	,
    Comments,
    U_VoucherTypeID

    from [PGI_UAT].[dbo].[OPCH] 
    where CANCELED='N'

    """
    # where DocDate>= '%s'""" %(date_note)


    
    #danh sách warehouse
sql_OWHS= """ select 
    WhsCode	,
    WhsName	
    from [PGI_UAT].[dbo].[OWHS] 
    """

    # danh sách nhân viên
sql_OSLP= """ select 
    SlpCode	,
    SlpName	
    from [PGI_UAT].[dbo].[OSLP] 
    """

    #danh sách khách hàng chính
sql_OCRD= """ select 
    CardCode,
    CardName as Main_CardName,
    U_TerrDesc
    from [PGI_UAT].[dbo].[OCRD] 
    """

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

sql_OITM= """ select
        ItemCode	,
        ItmsGrpCod	,
        FirmCode,
        U_ItmGrp1,
        U_ItmGrp2

        FROM [PGI_UAT].[dbo].[OITM]"""

sql_VOUCHERTYPE= """ select
        Code	,
        Name	
        FROM [PGI_UAT].[dbo].[@VOUCHERTYPE]"""


conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)

df_PCH1 = pd.read_sql_query(sql_PCH1,conn)
df_OPCH = pd.read_sql_query(sql_OPCH,conn)
df_OWHS = pd.read_sql_query(sql_OWHS,conn)
df_OSLP = pd.read_sql_query(sql_OSLP,conn)
df_OCRD = pd.read_sql_query(sql_OCRD,conn)
df_OITB = pd.read_sql_query(sql_OITB,conn)
df_OMRC = pd.read_sql_query(sql_OMRC,conn)
df_OITM = pd.read_sql_query(sql_OITM,conn)
df_VCTYPE = pd.read_sql_query(sql_VOUCHERTYPE,conn)


Raw0=pd.merge(df_PCH1, df_OPCH, how="inner", on=["DocEntry", "DocEntry"])
Raw1=pd.merge(Raw0, df_OWHS, how="left", on=["WhsCode", "WhsCode"])
Raw2=pd.merge(Raw1, df_OSLP, how="left", left_on=["SlpCode"],right_on=["SlpCode"])
Raw3=pd.merge(Raw2, df_OCRD, how="left", left_on=["U_CardCode"],right_on=["CardCode"])
Raw4=pd.merge(Raw3, df_OITM, how="left", left_on=["ItemCode"],right_on=["ItemCode"])
Raw5=pd.merge(Raw4, df_OITB, how="left", left_on=["ItmsGrpCod"],right_on=["ItmsGrpCod"])
Raw6=pd.merge(Raw5, df_OMRC, how="left", left_on=["FirmCode"],right_on=["FirmCode"])
Rawdata=pd.merge(Raw6, df_VCTYPE, how="left", left_on=["U_VoucherTypeID"],right_on=["Code"])

from datetime import datetime

Rawdata['updated_time'] = datetime.now()+ timedelta(hours=7)
Rawdata['TaxDate']=Rawdata.TaxDate.dt.date
Rawdata.columns = Rawdata.columns.str.lower()

# print some columns by position
# print(Rawdata.iloc[:, 15:18])
# print(Rawdata.iloc[:, 1:4])

#Cast type data
Rawdata['docentry'] = Rawdata['docentry'].astype(int)
Rawdata['linenum'] = Rawdata['linenum'].astype(int)
Rawdata['targettype'] = Rawdata['targettype'].astype(float)
Rawdata['trgetentry'] = Rawdata['trgetentry'].astype(float)
Rawdata['basetype'] = Rawdata['basetype'].astype(object)
Rawdata['linestatus'] = Rawdata['linestatus'].astype(object)
Rawdata['invntsttus'] = Rawdata['invntsttus'].astype(object)
Rawdata['itemcode'] = Rawdata['itemcode'].astype(object)
Rawdata['dscription'] = Rawdata['dscription'].astype(object).str.encode('utf-8')
Rawdata['quantity'] = Rawdata['quantity'].astype(float)
# Rawdata['shipdate'] = pd.to_datetime(Rawdata['shipdate'])
Rawdata['shipdate'] = Rawdata['shipdate'].astype('datetime64[us]')
Rawdata['price'] = Rawdata['price'].astype(float)
Rawdata['rate'] = Rawdata['rate'].astype(float)
Rawdata['currency'] = Rawdata['currency'].astype(object)
Rawdata['linetotal'] = Rawdata['linetotal'].astype(float)
# Rawdata['docdate'] = pd.to_datetime(Rawdata['docdate'])
Rawdata['docdate'] = Rawdata['docdate'].astype('datetime64[us]')
Rawdata['grssprofit'] = Rawdata['grssprofit'].astype(float)
Rawdata['unitmsr'] = Rawdata['unitmsr'].astype(object)
Rawdata['shiptocode'] = Rawdata['shiptocode'].astype(object)
Rawdata['shiptodesc'] = Rawdata['shiptodesc'].astype(object).str.encode('utf-8')
Rawdata['ocrcode3'] = Rawdata['ocrcode3'].astype(object)
Rawdata['cogsocrco2'] = Rawdata['cogsocrco2'].astype(object)  #test
Rawdata['u_s1_basenotes'] = Rawdata['u_s1_basenotes'].astype(object)
Rawdata['u_origiprice'] = Rawdata['u_origiprice'].astype(float)
Rawdata['u_discamt'] = Rawdata['u_discamt'].astype(float)
Rawdata['u_discprcnt'] = Rawdata['u_discprcnt'].astype(float)
Rawdata['u_pricedisc'] = Rawdata['u_pricedisc'].astype(float)
Rawdata['whscode'] = Rawdata['whscode'].astype(object)
Rawdata['basecard'] = Rawdata['basecard'].astype(object)
Rawdata['taxcode'] = Rawdata['taxcode'].astype(object)
Rawdata['u_vouchercode'] = Rawdata['u_vouchercode'].astype(object)
Rawdata['vendornum'] = Rawdata['vendornum'].astype(object)
Rawdata['gtotal'] = Rawdata['gtotal'].astype(float)
Rawdata['docnum'] = Rawdata['docnum'].astype(int)
Rawdata['doctype'] = Rawdata['doctype'].astype(object)
Rawdata['canceled'] = Rawdata['canceled'].astype(object)
Rawdata['docstatus'] = Rawdata['docstatus'].astype(object)
# Rawdata['docduedate'] = pd.to_datetime(Rawdata['docduedate'])
Rawdata['docduedate'] = Rawdata['docduedate'].astype('datetime64[us]')
Rawdata['doctotal'] = Rawdata['doctotal'].astype(float)
Rawdata['grosprofit'] = Rawdata['grosprofit'].astype(float)
Rawdata['jrnlmemo'] = Rawdata['jrnlmemo'].astype(object)
Rawdata['doctotalsy'] = Rawdata['doctotalsy'].astype(float)
Rawdata['paidsys'] = Rawdata['paidsys'].astype(float)
Rawdata['grosprofsy'] = Rawdata['grosprofsy'].astype(float)
# Rawdata['updatedate'] = pd.to_datetime(Rawdata['updatedate'])
Rawdata['updatedate'] = Rawdata['updatedate'].astype('datetime64[us]')
# Rawdata['createdate'] = pd.to_datetime(Rawdata['createdate'])
Rawdata['createdate'] = Rawdata['createdate'].astype('datetime64[us]')
# Rawdata['taxdate'] = pd.to_datetime(Rawdata['taxdate'])
Rawdata['taxdate'] = Rawdata['taxdate'].astype('datetime64[us]')
# Rawdata['reqdate'] = pd.to_datetime(Rawdata['reqdate'])
Rawdata['reqdate'] = Rawdata['reqdate'].astype('datetime64[us]')
# Rawdata['extradays'] = pd.to_datetime(Rawdata['extradays'])
Rawdata['extradays'] = Rawdata['extradays'].astype('datetime64[us]')
Rawdata['slpcode'] = Rawdata['slpcode'].astype(object)
Rawdata['u_s1no'] = Rawdata['u_s1no'].astype(object)
Rawdata['u_cardcode'] = Rawdata['u_cardcode'].astype(object)
Rawdata['u_cardname'] = Rawdata['u_cardname'].astype(object).str.encode('utf-8')
Rawdata['u_noteforall'] = Rawdata['u_noteforall'].astype(object).str.encode('utf-8')
Rawdata['u_noteforacc'] = Rawdata['u_noteforacc'].astype(object).str.encode('utf-8')
Rawdata['u_noteforwhs'] = Rawdata['u_noteforwhs'].astype(object).str.encode('utf-8')
Rawdata['u_noteforstatus'] = Rawdata['u_noteforstatus'].astype(object)
Rawdata['u_store'] = Rawdata['u_store'].astype(object)
Rawdata['u_totalqty'] = Rawdata['u_totalqty'].astype(float)
Rawdata['u_noteforlogistic'] = Rawdata['u_noteforlogistic'].astype(object)
Rawdata['comments'] = Rawdata['comments'].astype(object).str.encode('utf-8')
Rawdata['u_vouchertypeid'] = Rawdata['u_vouchertypeid'].astype(object)
Rawdata['whsname'] = Rawdata['whsname'].astype(object)
Rawdata['slpname'] = Rawdata['slpname'].astype(object).str.encode('utf-8')
Rawdata['cardcode'] = Rawdata['cardcode'].astype(object)
Rawdata['main_cardname'] = Rawdata['main_cardname'].astype(object).str.encode('utf-8')
Rawdata['u_terrdesc'] = Rawdata['u_terrdesc'].astype(object)
Rawdata['itmsgrpcod'] = Rawdata['itmsgrpcod'].astype(object)
Rawdata['firmcode'] = Rawdata['firmcode'].astype(object)
Rawdata['u_itmgrp1'] = Rawdata['u_itmgrp1'].astype(object)
Rawdata['u_itmgrp2'] = Rawdata['u_itmgrp2'].astype(object)
Rawdata['itmsgrpnam'] = Rawdata['itmsgrpnam'].astype(object)
Rawdata['u_costact1'] = Rawdata['u_costact1'].astype(object)
Rawdata['u_reportgroup'] = Rawdata['u_reportgroup'].astype(object)
Rawdata['firmname'] = Rawdata['firmname'].astype(object)
Rawdata['code'] = Rawdata['code'].astype(object)
Rawdata['name'] = Rawdata['name'].astype(object).str.encode('utf-8')
# Rawdata['updated_time'] = pd.to_datetime(Rawdata['updated_time'])
Rawdata['updated_time'] = pd.to_datetime(Rawdata['updated_time']).astype('datetime64[us]')

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_purchase_detail'

# print(Rawdata.iloc[:, 15:18])
# print(Rawdata.iloc[:, 1:4])
# print(Rawdata.dtypes)

# Define Schema for table purchase details
schema = [
    bigquery.SchemaField("docentry", "INTEGER", mode="NULLABLE"),  # This allows null values,
    bigquery.SchemaField("linenum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("targettype", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("trgetentry", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("basetype", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("linestatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("invntsttus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("itemcode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dscription", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("quantity", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("shipdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("linetotal", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("docdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("grssprofit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unitmsr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("shiptocode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("shiptodesc", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("ocrcode3", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cogsocrco2", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("u_s1_basenotes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_origiprice", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_discamt", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_discprcnt", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_pricedisc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("whscode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("basecard", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("taxcode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("u_vouchercode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("vendornum", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gtotal", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("docnum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("doctype", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("canceled", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("docstatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("docduedate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("doctotal", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("grosprofit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("jrnlmemo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("doctotalsy", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("paidsys", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("grosprofsy", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("updatedate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("createdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("taxdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("reqdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("extradays", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("slpcode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("u_s1no", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_cardcode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_cardname", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforall", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforacc", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforwhs", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforstatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_store", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_totalqty", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforlogistic", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("comments", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("u_vouchertypeid", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("whsname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("slpname", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("cardcode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("main_cardname", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("u_terrdesc", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("itmsgrpcod", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("firmcode", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_itmgrp1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_itmgrp2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("itmsgrpnam", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_costact1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_reportgroup", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("firmname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("updated_time", "TIMESTAMP", mode="NULLABLE"),
    # Thêm các cột khác tương ứng
]


query = """DELETE FROM `%s`"""  %(table_id)+ """ WHERE docdate>= '%s'"""%(date_note)

query_job = client.query(query)
rows = query_job.result()  # Waits for query to finish
for row in rows:
    print(row.name)

job_config= bigquery.LoadJobConfig(
# autodetect= True,
schema=schema,
# write_disposition ='WRITE_APPEND'
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(Rawdata,table_id,job_config=job_config,)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())


# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)