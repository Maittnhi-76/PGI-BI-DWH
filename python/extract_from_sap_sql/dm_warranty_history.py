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
# from airflow.utils.dates import days_ago
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook




# default_args= {
#     'owner': 'phuongbi',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=2)
# }


# dag =   DAG(
#     dag_id='upload_dm_warranty',
#     default_args=default_args,
#     description='load data warranty from server to bigquery',
#     start_date=datetime(2023, 6, 27, 9),
#     schedule_interval= '@daily'
# ) 


# def get_server ():
from datetime import datetime
# my_path_var = os.getenv('PATHVAR', '')
date_note = (datetime.now()+timedelta(days=-60)).date()

# hook=MsSqlHook(mssql_conn_id="pgi_server")
# Fetching the data from the selected table using SQL query
sql= """ 
    with base as
    (
    SELECT 
    t0.U_WhsCode,
    --cast(0 as bit) as 'isSelected',
    t0.U_VoucherNo,
    t0.U_Date, 
    t0.StatusID,
    t0.Status, 
    t0.TypeName,
    t0.callType, 
    t0.CallTypeName, 
    t0.PriorityName, 
    t0.Subject, 
    t0.ItemCode,
    t0.ItemName, 
    t0.U_SerialNo, 
    t0.customer, 
    t0.custmrName,
    t0.U_CardAddress,
    t0.U_Phone, 
    t0.U_WarNo, 
    t0.U_WarStart,
    t0.U_WarEnd,
    t0.descrption, 
    t0.U_TotalAmt, 
    t0.U_PayDate,
    t0.U_StartDate, 
    t0.U_EndDate, 
    t0.U_FinishDate,
    t0.U_HomeDate,
    t0.U_UserID,    
    --cast(t0.callID as nvarchar(20)) as callID, 
    t0.objType, 
    t0.priority, 
    t0.problemTyp,
    t0.createTime, 
    t0.EmployeeName, 
    t0.EmployeeNameUpdate,
    t0.ReportTitle,
    t0.BPPerson,
    t0.OnSite,
    t0.U_Evaluate1, 
    t0.U_Evaluate2, 
    t0.U_Evaluate3,
    --t0.U_WhsCode -- không query được cột này
    t0.WhsName,
    t0.BranchID, 
    t0.Branch,
    t0.U_Vender,
    t0.U_VenderName,
    t0.U_DateReceipt, 
    t0.isTransfer, 
    t0.isReceipt,
    t0.U_DeliveryDate, 
    t0.U_DeviceStatus, 
    t0.U_TransferNo,
    t0.SStore, 
    t0.U_ErrorExt,
    t0.SStoreName,
    t0.U_StatusID,
    t0.StatusName,
    '6001' as U_VoucherTypeID, 
    t0.technician, isnull(t2.firstName,'') + ' '+ isnull(t2.middleName,'')+ ' '+ isnull(t2.lastName,'') as technicianName, 
    case when t0.U_ReturnDate='yyyy-MM-dd HH:mm' then '' else t0.U_ReturnDate end as U_ReturnDate,
    --T3.U_InvNo as PurchInvoiceNo, T3.DocDate as PurchInvoiceDate, T3.U_VoucherNo as PurchVoucherNo, T3.U_S1No as PurchS1No, T3.Comments as PurchComments,
    T0.U_ActiveDate, 
    T0.U_ActivePhone,
    T0.U_SalesDate, 
    T0.U_WarTime, 
    T0.CEC_InsID, 
    t3.U_Amount as U_QuotationActivity,
    t3.U_Solution as resolution, 
    t0.U_ItemChange,
    t0.U_SerialChange,
    t0.SaleWhsCode,   
    t1.ClgID,
    t3.U_SolutionID,
    t4.Name as Solution, 
    t3.U_StatusID as U_StatusIDdetail,
    t5.Name as StatusSolution, 
    t6.Name as Technician1, 
    t3.U_FinishDate as RepaidFinishDate, 
    case t0.priority when 'H' then '#FFB822' when 'M' then '#05992D' else '' end as S1FlexGirdRowColor, 
    t3.U_KPI, 
    t3.U_SolutionSubID, 
    t7.Name as SolutionSub, 
    t3.Details,
    t8.ItemName as Description,
    t8.FrgnName,
    t8.U_ItmNamWb,
    t8.U_ItmNamAcct,
    t9.ItmsGrpNam,
    t8.U_ItmGrp1,
    t8.U_ItmGrp2,
    t10.FirmName,
    --case when (t4.Name in ('Đã Kiểm tra','Sửa xong','Bảo hành xong','Đã gọi k.hàng','Từ chối bảo hành','Báo giá','Thay thế','Sửa chữa') and t5.Name ='Hoàn tất') or t4.Name in ('Đổi mới','Không sửa được') then 'done KT'
    --     when (t4.Name in ('Cấn trừ','Hoàn tiền','Bù phí','Giao máy') and t5.Name ='Hoàn tất') or t4.Name in ('Khách không đến nhận máy','Chuyển hàng (trả máy)') then 'done CSKH(trả)'
    --	 when t4.Name in ('Tiếp nhận','Điều chuyển','Nhận hàng') then 'done CSKH(nhận)' end as bh_type,

    case when (t3.U_SolutionID in (1013,1006,1015)/*'Đã Kiểm tra','Sửa xong','Bảo hành xong',*/ 
                and t3.U_StatusID =2 /*'Hoàn tất'*/ ) 
            or (t3.U_SolutionID in (1012) /*'Đổi mới','Không sửa được'*/ 
                and t3.U_StatusID =1  /*'Đang xử lý'*/ ) 
            or (t3.U_SolutionID in (1011) ) then 'done KT'
        when (t3.U_SolutionID in (1016,1017,1018,1010,1012)/*('Cấn trừ','Hoàn tiền','Bù phí','Giao máy')*/ 
                    and t3.U_StatusID =2 /*'Hoàn tất'*/)
            or (t3.U_SolutionID  in (1008,1019)) /*'Chuyển hàng (Trả máy)','Khách không đến nhận máy'*/  then 'done CS(tra)'
        when t3.U_SolutionID in (1001,1003,1002,1007) and t0.EmployeeName = t6.Name then 'done CS(nhan)'
        else 'not done' end as bh_status1,

    case when (t3.U_SolutionID in (1002,1003,1007) ) or(t3.U_SolutionID in (1001) and  t0.EmployeeName = t6.Name)  then 'CS(nhan)'
        when ((t3.U_SolutionID in (1009,1004,1014,1005,1013,1006,1015,1020,1011)) or (t3.U_SolutionID in (1012) and t3.U_StatusID =1))
            or (t0.technician = t3.U_Technician) then 'KT'
        when t3.U_SolutionID in (1008,1016,1017,1018,1010,1012,1019) then 'CS(tra)'
        end as bh_staff1,
    row_number() over (partition by U_VoucherNo order by ClgID asc ) row_1
            
    FROM [PGI_UAT].[dbo].[S1_ServiceDocs] t0
    left join [PGI_UAT].[dbo].SCL5 t1  on t1.SrvcCallId = t0.callID
    left join [PGI_UAT].[dbo].OCLG t3  on t3.ClgCode = t1.ClgID
    left join [PGI_UAT].[dbo].OHEM T2  on t0.technician= t2.empID
    LEFT JOIN [PGI_UAT].[dbo].[@SERVICESOLUTION] t4  on t4.Code = t3.U_SolutionID
    LEFT JOIN [PGI_UAT].[dbo].[@ACTIVITYSTATUS] t5  on t5.Code = t3.U_StatusID
    LEFT JOIN [PGI_UAT].[dbo].[@OTECH] t6  on t6.Code = t3.U_Technician
    LEFT JOIN [PGI_UAT].[dbo].[@SERVICESOLUTIONSUB] t7 on t7.Code = t3.U_SolutionSubID
    inner join [PGI_UAT].[dbo].OITM t8  on t0.ItemCode=t8.ItemCode
    inner join [PGI_UAT].[dbo].OITB t9  on t8.ItmsGrpCod=t9.ItmsGrpCod
    inner join [PGI_UAT].[dbo].OMRC t10  on t8.FirmCode=t10.FirmCode
    
    

    )

    select *, case when row_1=1 and U_SolutionID=1001 then 'CS(nhan)' 
                    when row_1>1 and U_SolutionID=1001 and U_StatusIDdetail=2 then 'KT' 
                else bh_staff1 end as bh_staff
    , case when row_1=1 and U_SolutionID=1001 and U_StatusIDdetail=2 then 'done CS(nhan)' else bh_status1 end as bh_status
    from base
    """
#---where t3.U_FinishDate>= '%s'"""%(date_note) +"""
conn = pymssql.connect(
server='192.168.60.252',
user='BA',
password='PGI@123',
database='PGI_UAT',
port=1433,
)

RawData = pd.read_sql_query(sql,conn)
RawData=RawData.drop(['bh_status1','bh_staff1'],axis=1)
RawData['updated_time'] = datetime.now()+ timedelta(hours=7)
RawData['U_Date']=RawData.U_Date.dt.date
credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_warranty'

query = """DELETE FROM `%s`"""  %(table_id)+ """ WHERE RepaidFinishDate>= '%s'"""%(date_note)

query_job = client.query(query)
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)

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



# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# python_task = PythonOperator(task_id='python_task', python_callable=get_server, dag=dag)