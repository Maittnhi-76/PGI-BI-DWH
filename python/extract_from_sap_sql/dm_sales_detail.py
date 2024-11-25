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
import numpy as np
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
#     dag_id='dm_sales_detail',
#     default_args=default_args,
#     description='load data sales detail from server to bigquery',
#     start_date=datetime(2023, 6, 27, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():

   
#     hook=MsSqlHook(mssql_conn_id="pgi_server")
# Fetching the data from the selected table using SQL query
from datetime import datetime
# my_path_var = os.getenv('PATHVAR', '')
date_note = (datetime.now()+ timedelta(days=-360)).date()

# đơn hàng Pending - lấy từ order

sql_RDR1= """ select 
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
    GTotal,
    StockPrice,
    StockValue,
    'Pending_kho' as voucher_type		
    from [PGI_UAT].[dbo].[RDR1] tb1
    where LineStatus = N'O' 
    and DocDate>= '%s'""" %(date_note)



    #đơn hàng tổng
sql_ORDR= """ select 
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
    CreateTs,
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
    U_VoucherTypeID,
    cast(ObjType as int) as ObjType,
    cast(U_StatusID as int) as U_StatusID

    from [PGI_UAT].[dbo].[ORDR] 
    where CardCode not in ('PGI_LOCK','PGI')
    and DocDate>= '%s'""" %(date_note)



# Đơn hàng pending được tạo & kế toán chưa duyệt

sql_DRF1= """ select 
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
    GTotal,
    StockPrice,
    StockValue,
    'Pending_ketoan' as voucher_type		
    from [PGI_UAT].[dbo].[DRF1] tb1
    where  isnull(tb1.basetype,-1) <> '17'

    and DocDate>= '%s'""" %(date_note)

sql_ODRF= """ select 
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
    CreateTs ,
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
    U_VoucherTypeID,
    cast(ObjType as int) as ObjType,
    cast(U_StatusID as int) as U_StatusID

    from [PGI_UAT].[dbo].[ODRF] 
    
    where CardCode not in ('PGI_LOCK','PGI','PRO_MKT')
    and DocStatus = N'O' and isnull(ObjType,'') <> '15'
    and DocDate>= '%s'""" %(date_note)




# đơn hàng mua trực tiếp tại showroom &  đơn dịch vụ bảo hành => lấy từ Invoice vouchercode in (1007,1002)

sql_INV1= """ select 
    tb1.DocEntry	,
    tb1.LineNum	,
    tb1.TargetType	,
    tb1.TrgetEntry	,
    tb1.BaseType	,
    coalesce(tb2.LineStatus,tb1.LineStatus) LineStatus	,
    tb1.InvntSttus,
    tb1.ItemCode	,
    tb1.Dscription	,
    tb1.Quantity	,
    tb1.ShipDate	,
    tb1.Price	,
    tb1.Currency	,
    tb1.LineTotal	,
    tb1.DocDate	,
    tb1.GrssProfit	,
    tb1.unitMsr	,
    tb1.ShipToCode	,
    tb1.ShipToDesc	,
    tb1.OcrCode3	,
    tb1.CogsOcrCo2	,
    tb1.U_S1_BaseNotes	,
    tb1.U_OrigiPrice	,
    tb1.U_DiscAmt	,
    tb1.U_DiscPrcnt	,
    tb1.U_PriceDisc	,
    tb1.WhsCode	,
    tb1.BaseCard	,
    tb1.TaxCode	,
    tb1.U_VoucherCode,
    tb1.VendorNum,
    tb1.GTotal,
    tb1.StockPrice,
    tb1.StockValue,
    'Sale Order' as voucher_type		
    from [PGI_UAT].[dbo].[INV1] tb1
    left join [PGI_UAT].[dbo].[RDR1] tb2 on tb1.BaseEntry = tb2.DocEntry and tb1.linenum=tb2.linenum

    where tb1.DocDate>= '%s'""" %(date_note)



    #đơn hàng tổng
sql_OINV= """ select 
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
    CreateTs ,
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
    U_VoucherTypeID,
    cast(ObjType as int) as ObjType,
    cast(U_StatusID as int) as U_StatusID

    from [PGI_UAT].[dbo].[OINV] 

    where DocDate>= '%s'""" %(date_note)







#đơn hàng trả lại detail
sql_RDN1= """ select 
    DocEntry	,
    LineNum	,
    TargetType	,
    TrgetEntry	,
    BaseType	,
    LineStatus	,
    InvntSttus,
    ItemCode	,
    Dscription	,
    - Quantity as Quantity	,
    ShipDate	,
    Price	,
    Currency	,
    -LineTotal as LineTotal 	,
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
    GTotal,
    StockPrice,
    StockValue,
    'Sale Return' as voucher_type	
    from [PGI_UAT].[dbo].[RDN1] 

    where DocDate>= '%s'""" %(date_note)



    #đơn hàng trả lại tổng
sql_ORDN= """ select 
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
    CreateTs ,
    TaxDate	,
    ReqDate	,
    ExtraDays	,
    SlpCode	,
    U_S1No	,
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
    U_VoucherTypeID,
    cast(ObjType as int) as ObjType,
    cast(U_StatusID as int) as U_StatusID

    from [PGI_UAT].[dbo].[ORDN] 

    where DocDate>= '%s'""" %(date_note)

    ############################################

    #đơn hàng trả lại detail -- Nhập trả lại ko tham chiếu & đã ghi nhận hóa đơn

sql_RIN1= """ select 
    DocEntry	,
    LineNum	,
    TargetType	,
    TrgetEntry	,
    BaseType	,
    LineStatus	,
    InvntSttus,
    ItemCode	,
    Dscription	,
    -Quantity	as Quantity,
    ShipDate	,
    Price	,
    Currency	,
    -LineTotal as LineTotal	,
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
    GTotal,
    StockPrice,
    StockValue,
    'Sale Order' as voucher_type		
    from [PGI_UAT].[dbo].[RIN1] 

    where DocDate>= '%s'""" %(date_note)



    #đơn hàng tổng
sql_ORIN= """ select 
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
    CreateTs ,
    TaxDate	,
    ReqDate	,
    ExtraDays	,
    SlpCode	,
    U_S1No	,
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
    U_VoucherTypeID,
    cast(ObjType as int) as ObjType,
    cast(U_StatusID as int) as U_StatusID


    from [PGI_UAT].[dbo].[ORIN] 

        where DocDate>= '%s'""" %(date_note)




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
    U_TerrDesc,
    ChannlBP	 ,
    U_TerrID
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
        Code as vouchercode	,
        Name	
        FROM [PGI_UAT].[dbo].[@VOUCHERTYPE]"""

sql_S1_Region = """ select *

    from [PGI_UAT].[dbo].[S1_Region]
    """

sql_S1_ObjType =""" select cast(Code as int) as Code,[Name] as objname
    from [PGI_UAT].[dbo].[S1_ObjType]"""

sql_S1_DocStatus =""" select cast(Code as int) as Code_doc,[Name] as StatusName
    from [PGI_UAT].[dbo].[@DocStatus]"""

conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)

df_RDR1 = pd.read_sql_query(sql_RDR1,conn)
df_ORDR = pd.read_sql_query(sql_ORDR,conn)
df_DRF1 = pd.read_sql_query(sql_DRF1,conn)
df_ODRF = pd.read_sql_query(sql_ODRF,conn)
df_INV1 = pd.read_sql_query(sql_INV1,conn)
df_OINV = pd.read_sql_query(sql_OINV,conn)
df_RDN1 = pd.read_sql_query(sql_RDN1,conn)
df_ORDN = pd.read_sql_query(sql_ORDN,conn)
df_RIN1 = pd.read_sql_query(sql_RIN1,conn)
df_ORIN = pd.read_sql_query(sql_ORIN,conn)
df_OWHS = pd.read_sql_query(sql_OWHS,conn)
df_OSLP = pd.read_sql_query(sql_OSLP,conn)
df_OCRD = pd.read_sql_query(sql_OCRD,conn)
df_OITB = pd.read_sql_query(sql_OITB,conn)
df_OMRC = pd.read_sql_query(sql_OMRC,conn)
df_OITM = pd.read_sql_query(sql_OITM,conn)
df_S1_Region = pd.read_sql_query(sql_S1_Region,conn)
df_VCTYPE = pd.read_sql_query(sql_VOUCHERTYPE,conn)
df_S1_ObjType = pd.read_sql_query(sql_S1_ObjType,conn)
df_s1_DocStatus = pd.read_sql_query(sql_S1_DocStatus,conn)





Raw_order=pd.merge(df_RDR1, df_ORDR, how="inner", on=["DocEntry", "DocEntry"])
Raw_invoice=pd.merge(df_INV1, df_OINV, how="inner", on=["DocEntry", "DocEntry"])
Raw_return=pd.merge(df_RDN1, df_ORDN, how="inner", on=["DocEntry", "DocEntry"])
Raw_return2=pd.merge(df_RIN1, df_ORIN, how="inner", on=["DocEntry", "DocEntry"])
Raw_pending=pd.merge(df_DRF1, df_ODRF, how="inner", on=["DocEntry", "DocEntry"])
Raw0=pd.concat([Raw_order,Raw_invoice, Raw_return,Raw_return2,Raw_pending])
# Raw0=pd.concat([Raw, Raw_return2])
Raw1=pd.merge(Raw0, df_OWHS, how="left", on=["WhsCode", "WhsCode"])
Raw2=pd.merge(Raw1, df_OSLP, how="left", left_on=["SlpCode"],right_on=["SlpCode"])
Raw3=pd.merge(Raw2, df_OCRD, how="left", left_on=["U_CardCode"],right_on=["CardCode"])
Raw4=pd.merge(Raw3, df_OITM, how="left", left_on=["ItemCode"],right_on=["ItemCode"])
Raw5=pd.merge(Raw4, df_OITB, how="left", left_on=["ItmsGrpCod"],right_on=["ItmsGrpCod"])
Raw6=pd.merge(Raw5, df_OMRC, how="left", left_on=["FirmCode"],right_on=["FirmCode"])
Raw7=pd.merge(Raw6, df_S1_Region, how="left", left_on=["U_TerrID"],right_on=["Territory"])
Raw8=pd.merge(Raw7, df_S1_ObjType, how="inner", left_on=["ObjType"],right_on=["Code"])
Raw9=pd.merge(Raw8, df_s1_DocStatus, how="left", left_on=["U_StatusID"],right_on=["Code_doc"])
Rawdata=pd.merge(Raw9, df_VCTYPE, how="left", left_on=["U_VoucherTypeID"],right_on=["vouchercode"])


from datetime import datetime

Rawdata['updated_time'] = datetime.now()+ timedelta(hours=7)
Rawdata['TaxDate']=Rawdata.TaxDate.dt.date
Rawdata.columns = Rawdata.columns.str.lower()

#----------------------------------Điều kiện cho đơn hàng của Bảo từ tháng 6/24 -> hiện tại -------------------------------------
#Link Base (https://request.base.vn/requests?request=1328953)
specific_date = pd.to_datetime('2024-06-01').date()
# Điều kiện
conditions = [
    (Rawdata['taxdate'] >= specific_date) & (Rawdata['slpcode'] == 132) & (Rawdata['cardcode'] == 'SHOPEE_RETAIL'),
    (Rawdata['taxdate'] >= specific_date) & (Rawdata['slpcode'] == 132) & (Rawdata['cardcode'] == 'KVLONLINE'),
    (Rawdata['taxdate'] >= specific_date) & (Rawdata['slpcode'] == 132) & (Rawdata['cardcode'] == 'SHOPEE')
]

# Giá trị tương ứng cho cột mới D và E
values_code = [171, 181, 181]
values_name = ['Trần Hoàng Liên Thảo', 'Đoàn Thị Minh Trang', 'Đoàn Thị Minh Trang']

# Sử dụng numpy.select để áp dụng nhiều điều kiện
Rawdata['slpcode'] = np.select(conditions, values_code, default=Rawdata['slpcode'])
Rawdata['slpname'] = np.select(conditions, values_name, default=Rawdata['slpname'])

#-------------------------END phần điều kiện đơn hàng của Bảo-------------------------------------

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_sales_detail'

query = """DELETE FROM `%s`"""  %(table_id)+ """ WHERE docdate>= '%s'"""%(date_note)

query_job = client.query(query)
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)
job_config= bigquery.LoadJobConfig(
autodetect= True,
write_disposition ='WRITE_APPEND'
# write_disposition ='WRITE_TRUNCATE' 

)

job = client.load_table_from_dataframe(Rawdata,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())


# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)