from datetime import datetime
from datetime import timedelta
import pymssql
import pandas as pd
import psycopg2
import datetime
from datetime import datetime
# import numpy as np
import sys
import time
from google.cloud import bigquery
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

date_note = (datetime.now()+ timedelta(days=-30)).date()
# hook=MsSqlHook(mssql_conn_id="pgi_server")
# Fetching the data from the selected table using SQL query
sql= """ with 
    base as(
    select t0.U_VoucherNo as VoucherNo, t0.DocDate as DocDate, t0.CardCode, t0.CardName,
            t3.Name as VoucherType, t4.[Name], 
            Case when t0.StockQty > 0 then 'Nhap' else 'Xuat' end as In_Out, 
            --isnull(t2.LotNumber, '''') U_BatchNo, 
            t0.ItemCode,  (t1.Quantity) as Quantity, t2.DistNumber,
        t0.U_S1No 
            from OITL t0 with(nolock)
            inner join ITL1 t1 with(nolock) on t0.LogEntry = t1.LogEntry
            inner join OSRN t2 with(nolock) on t1.MdAbsEntry = t2.AbsEntry and t0.ManagedBy = t2.ObjType 
            left join [@VoucherType] t3 with(nolock) on t0.U_VoucherTypeID = t3.Code
            left join s1_objtype t4 with(nolock) on t4.Code = t0.DocType	
            where t0.StockEff = 1  and  t4.[Name] not in ('Inventory Transfers','Goods Issue') 
            )

    --- Tạo bảng giá nhập theo serial
    ,
    primce_cost as(
    select t0.DistNumber,
        t0.itemcode,
            t3.Price	,
                t3.rate,
                t3.Currency	

        from base t0
        left join (select t1.ItemCode	,
                t1.Dscription	,
                t1.Quantity	,
                t1.ShipDate	,
                t1.Price	,
                t1.rate,
                t1.Currency	,
                t1.LineTotal	,
                t1.DocDate	,
                t1.GrssProfit	,
                t1.unitMsr	,
                t2.U_S1No   	
        from [PGI_UAT].[dbo].[PCH1] t1 
        inner join [PGI_UAT].[dbo].[OPCH] t2 on t1.DocEntry=t2.DocEntry
        where t2.CANCELED='N'
        ) t3 on t0.U_S1No=t3.U_S1No and t0.ItemCode=t3.itemcode
        where t0.Vouchertype='Mua hàng (Purchase Order)' and t3.Currency='USD'
    )
    -- Tao bang gia trung binh theo itemcode - cac san pham khong co gia nhap theo serial lay gia nhap trung binh
    ,
    avg_price as
    (
    select t1.itemcode, 
    sum((case when t1.Currency ='VND' then t1.Price/23500 else t1.Price end)*t1.Quantity )/sum(t1.Quantity) as avg_price
    from [PGI_UAT].[dbo].[PCH1] t1 
    inner join [PGI_UAT].[dbo].[OPCH] t2 on t1.DocEntry=t2.DocEntry

    where t2.CANCELED='N' and t1.quantity>0
    group by t1.itemcode
    )

    select t1.*, coalesce(Price,t3.avg_price) as primecost,rate,Currency,t5.Firmname
    from base t1
    left join primce_cost t2 on t1.DistNumber=t2.DistNumber and t1.itemcode=t2.itemcode
    left join avg_price t3 on t1.itemcode=t3.ItemCode
    left join dbo.OITM t4 on t1.itemcode=t4.itemcode
    left join dbo.OMRC t5 on t4.Firmcode=t5.Firmcode
    where DocDate>='%s'""" %(date_note)

conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)

RawData = pd.read_sql_query(sql,conn)
# RawData=pd.read_csv('plugins/kho2.6.csv')
RawData['updated_time'] = datetime.now()+ timedelta(hours=7)
# RawData['Docdate']=RawData.DocDate.dt.date

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.inventory.tb_dm_in_out_detail_serial'

query = """DELETE FROM `%s`"""  %(table_id)+ """ WHERE docdate>= '%s'"""%(date_note)
query_job = client.query(query)
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)

job_config= bigquery.LoadJobConfig(
autodetect= True,
# write_disposition ='WRITE_TRUNCATE'
write_disposition ='WRITE_APPEND'
)

job = client.load_table_from_dataframe(RawData,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())

# print('from airflow import DAG')