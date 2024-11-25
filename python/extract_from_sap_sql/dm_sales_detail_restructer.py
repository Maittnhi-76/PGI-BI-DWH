import pymssql
import pandas as pd
import datetime
from datetime import datetime
import sys
import time
from google.cloud import bigquery
import os
from datetime import datetime, timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Var Of Bigquery
# my_path_var = os.getenv('PATHVAR', '')
#------------------------------------------------------------------------#
# Define_function 
#Hàm để đọc sql từ path
def execute_sql_from_file(sql_file_path):
    """Đọc truy vấn từ tệp .sql"""
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        query = file.read()
    return query

# Hàm để lấy dữ liệu từ SQL Server
def get_data_from_sql_server(var_server, var_user, var_password, var_database, var_port, var_sqlinput):
    # Khai báo chuỗi truy vấn SQL
    # sql_input = var_sqlinput
    sql_input = execute_sql_from_file(var_sqlinput)

    # Kết nối đến cơ sở dữ liệu SQL Server
    conn = pymssql.connect(
        server=var_server,
        user=var_user,
        password=var_password,
        database=var_database,
        port=var_port,
    )

    # Thực hiện truy vấn và lấy dữ liệu vào DataFrame
    datafarm_outcome = pd.read_sql_query(sql_input, conn)
    datafarm_outcome['updated_time'] = datetime.now() + timedelta(hours=7)
    
    # Đóng kết nối
    conn.close()
    
    return datafarm_outcome
def load_data_to_bigquery(var_dataframe, var_table_id, var_schema, var_write_disposition = 'WRITE_TRUNCATE'):
    # Cấu hình đường dẫn cho credentials của BigQuery
    credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    # Kết nối đến BigQuery và tải dữ liệu vào bảng
    client = bigquery.Client()
    # table_id = var_table_id

    job_config = bigquery.LoadJobConfig(
        # autodetect=True,
        schema=var_schema,
        write_disposition= var_write_disposition
    )

    job = client.load_table_from_dataframe(var_dataframe, var_table_id, job_config=job_config)

    # Đợi cho công việc tải dữ liệu hoàn thành
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()

    # In kết quả của công việc tải
    return print(job.result())

#------------------------------------------------------------------------#
# Today interval last 12 month
first_day_of_current_month = datetime.today().replace(day=1)
date_note = (first_day_of_current_month - relativedelta(months=12)).date()
print(date_note)

#------------------------------------------------------------------------#
# Deleted old record

credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =credentials_path
client = bigquery.Client()
table_id = 'pgi-dwh.sales.tb_dm_sales_detail_thienvo'

query = """DELETE FROM `%s`"""  %(table_id)+ """ WHERE docdate>= '%s'"""%(date_note)

query_job = client.query(query)
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)

#------------------------------------------------------------------------#
#Schema to append Data
schema = [
    bigquery.SchemaField("docentry", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("linenum", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("targettype", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("trgetentry", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("basetype", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("linestatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("invntsttus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("itemcode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dscription", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("quantity", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("shipdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("linetotal", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("docdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("grssprofit", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unitmsr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("shiptocode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("shiptodesc", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ocrcode3", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cogsocrco2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_s1_basenotes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_origiprice", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_discamt", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_discprcnt", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_pricedisc", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("whscode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("basecard", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("taxcode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("u_vouchercode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vendornum", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gtotal", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("stockprice", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("stockvalue", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("voucher_type", "STRING", mode="NULLABLE"),
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
    bigquery.SchemaField("createts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("taxdate", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("reqdate", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("extradays", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("slpcode", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("u_s1no", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_cardcode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_cardname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforall", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforacc", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforwhs", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforstatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_store", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_totalqty", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_noteforlogistic", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("comments", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_vouchertypeid", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("objtype", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("u_statusid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("whsname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("slpname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cardcode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("main_cardname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_terrdesc", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("channlbp", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_terrid", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("itmsgrpcod", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("firmcode", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("u_itmgrp1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_itmgrp2", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("itmsgrpnam", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_costact1", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("u_reportgroup", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("firmname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("territory", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("province", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("area_code", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("area", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("region_code", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("code", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("objname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("code_doc", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("statusname", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vouchercode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("updated_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("__index_level_0__", "INTEGER", mode="NULLABLE")
]

# Append_new_record
def get_and_load_data():
    var_sqlinput='/python/old_modules_bi/sqlfile/dm_sales_detail_restructer.sql'
    var_server='192.168.60.252'
    var_user='BA'
    var_password='PGI@123'
    var_database='PGI_UAT'
    var_port=1433
    var_schema=schema
    
    var_dataframe=get_data_from_sql_server(var_server, var_user, var_password, var_database, var_port, var_sqlinput)
    var_table_id='pgi-dwh.sales.tb_dm_sales_detail_thienvo'
    load_data_to_bigquery(var_dataframe, var_table_id, var_schema, var_write_disposition = 'WRITE_APPEND')
    # WRITE_APPEND WRITE_TRUNCATE
var_run_functino = get_and_load_data()