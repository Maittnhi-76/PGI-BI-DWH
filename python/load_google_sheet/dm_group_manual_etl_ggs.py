from datetime import timedelta, datetime
import pandas as pd
import sys
import time
from google.cloud import bigquery
import os
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

#-----------------------General Information----------------------------------
#BQ credential
#Set var in local and cloud is difference
credentials_path = os.getenv('DBT_PRD_SA')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

# Xác định phạm vi của quyền truy cập
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# Đường dẫn tới tệp JSON của tài khoản dịch vụ
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
# Uỷ quyền với các thông tin từ tệp JSON
client_ggs = gs.authorize(creds)
# Mở Google Sheet bằng ID
sheet = client_ggs.open_by_key('1COPCf3-WYBzafsD0swfb6ZEX6qotZulZDXc1S-V7L5g')
# Lấy trang tính đầu tiên
worksheet = sheet.worksheet('master_data_group')
# Lấy tất cả các bản ghi từ trang tính
records = worksheet.get_all_records()

df = pd.DataFrame(records)
# Ép kiểu trong pandas
# df['date_column'] = pd.to_datetime(df['date_column'])
# df['datetime_column'] = pd.to_datetime(df['datetime_column'])
# df['numeric_column'] = pd.to_numeric(df['numeric_column'])
# df['string_column'] = df['string_column'].astype(str)
# df['float_column'] = df['float_column'].astype(float)
# Ép kiểu dữ liệu str sang số nguyên
# df['str_column'] = pd.to_numeric(df['str_column'], errors='coerce').astype(pd.Int64Dtype())
# Ép kiểu dữ liệu null sang số nguyên
# df['null_column'] = df['null_column'].astype(pd.Int64Dtype())

df['CardCode'] = df['CardCode'].astype(str)
df['Group'] = df['Group'].astype(str)


table_id = 'pgi-dwh.sales.dm_group_customer_adhoc'
schema = [
    bigquery.SchemaField("CardCode", "STRING"),
    bigquery.SchemaField("Group", "STRING"),
    # Thêm các cột khác tương ứng
]

job_config= bigquery.LoadJobConfig(
# autodetect= True,
schema=schema,
write_disposition ='WRITE_TRUNCATE'
)

job = client.load_table_from_dataframe(df,table_id,job_config=job_config)

while job.state!='DONE':
    time.sleep(2)
    job.reload()
print(job.result())

