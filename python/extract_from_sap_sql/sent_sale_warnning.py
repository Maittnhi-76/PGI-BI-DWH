import sys
import os
import time
import pymssql
import pyodbc
# import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta

#------------------------------Định nghĩa function-----------------
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
# Hàm để tải dữ liệu lên BigQuery
def load_data_to_bigquery(var_dataframe, var_table_id, var_write_disposition = 'WRITE_TRUNCATE'):
    # Cấu hình đường dẫn cho credentials của BigQuery
    credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    # Kết nối đến BigQuery và tải dữ liệu vào bảng
    client = bigquery.Client()
    # table_id = var_table_id

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition= var_write_disposition
    )

    job = client.load_table_from_dataframe(var_dataframe, var_table_id, job_config=job_config)

    # Đợi cho công việc tải dữ liệu hoàn thành
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()

    # In kết quả của công việc tải
    return print(job.result())

def get_data_from_bigquery(var_pathinputsql):
    # BQ credential
    credentials_path = os.getenv('DBT_PRD_SA')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    client = bigquery.Client()

    # lấy ngày trong tháng từ Bigquery
    inputsql = execute_sql_from_file(var_pathinputsql)
    # inputsql = var_pathinputsql
    result = client.query(inputsql).result().to_dataframe()
    return result

# Hàm chuyển đổi tiền
# Function to format the large numbers
def format_large_numbers(num):
    if num >= 1e9:
        return f'{num / 1e9:.2f} B'
    elif num >= 1e6:
        return f'{num / 1e6:.2f} M'
    else:
        return f'{num}'

# Hàm vẽ biểu đồ theo từng Sales
def created_chart_rev_sale(df1, email_sale):
    # Link mã màu
    # http://ingiacong.co/bang-code-mau/

    # Biến mã màu dùng trong chart
    col_target = '#CFCFCF'
    col_act_100 = '#2F4F4F'
    col_act_60 = '#EEE8AA'
    col_act_0 = '#EE6363'
    lin_cr = '#C2C2C2'

    # Function to determine bar color based on completion rate
    def get_color(rate):
        if rate >= 100:
            return col_act_100 # Green
        elif rate > 60:
            return col_act_60  # Yellow
        else:
            return col_act_0  # Red

    df = df1

    # Set the figure size
    fig, ax1 = plt.subplots(figsize=(8, 5))

    # Plotting the KPI bar chart in gray
    bar_width = 0.4
    x = np.arange(len(df['MONTH']))

    bars1 = ax1.bar(x - bar_width/2, df['TGT_M'], width=bar_width, color=col_target, label='Mục tiêu')

    # Plotting the Actual Sales bar chart with conditional colors
    bar_colors = df['CR_M'].apply(get_color)
    bars2 = ax1.bar(x + bar_width/2, df['ACT_M'], width=bar_width, color=bar_colors, label='Thực hiện')

    # Setting labels and titles
    ax1.set_xlabel('Tháng')
    ax1.set_ylabel('Doanh số Mục tiêu | Thực hiện')
    # ax1.set_title('Doanh số mục tiêu, thực tế thực hiện & tỷ lệ hoàn thành theo tháng')
    ax1.set_xticks(x)
    ax1.set_xticklabels(df['MONTH'])

    # Adding labels to each bar
    for bar in bars1:
        height = bar.get_height()
        ax1.annotate(format_large_numbers(height), # Sử dụng hàm chuyển đổi ĐVT
                     xy=(bar.get_x() + bar.get_width() / 2, height),
                     xytext=(0, 3),  # 3 points vertical offset
                     textcoords="offset points",
                     ha='center', va='bottom')

    for bar in bars2:
        height = bar.get_height()
        ax1.annotate(format_large_numbers(height), # Sử dụng hàm chuyển đổi ĐVT
                     xy=(bar.get_x() + bar.get_width() / 2, height),
                     xytext=(0, 3),  # 3 points vertical offset
                     textcoords="offset points",
                     ha='center', va='bottom')

    # Create another y-axis for the line chart
    ax2 = ax1.twinx()
    line = ax2.plot(x, df['CR_M'], color=lin_cr, marker='o', linestyle='-', linewidth=2, label='Tỷ lệ hoàn thành(%)')

    ax2.set_ylabel('Tỷ lệ hoàn thành(%)')

    # Adding labels to the line chart points
    for i, value in enumerate(df['CR_M']):
        ax2.annotate(f'{value:.1f}%',
                     xy=(x[i], value),
                     xytext=(0, 3),  # 3 points vertical offset
                     textcoords="offset points",
                     ha='center', va='bottom')


    # Adding legends and positioning them outside the chart
    # ax1.legend(loc='upper left', bbox_to_anchor=(1.07, 1))
    # ax2.legend(loc='upper left', bbox_to_anchor=(1.07, 0.89))

    # Adjusting the layout to make space for the legends
    plt.tight_layout(rect=[0, 0, 0.85, 1])

    chart_path = '/python/old_modules_bi/save_image_email/' + email_sale + '.png' #'sales_chart.png'
    plt.savefig(chart_path)

# Chuyển ngày thành Quý
def convert_date_to_quarter(date_value):
    date_value = pd.to_datetime(date_value)
    return date_value.to_period('Q')

#----------------------Dùng function------------------------------------------------------------------------

var_pathinputsql_raw = '/python/old_modules_bi/sqlfile/sent_sale_warnning.sql'
df = get_data_from_bigquery(var_pathinputsql_raw)
unique_email = df['EMAIL'].unique()

#-------------------------------Sent emails------------------------------------------------------------------
for i in unique_email:
    filtered_df = df[df['EMAIL'] == i]
    staff_name = filtered_df['SALES'].values[0]
    created_chart_rev_sale(filtered_df, i) #Dùng hàm tạo chart cho từng nhân viên
    
    # Read the seaborn chart image
    chart_path_img = '/python/old_modules_bi/save_image_email/' + i + '.png'
    with open(chart_path_img, 'rb') as img_file:
        chart_img = MIMEImage(img_file.read())
        chart_img.add_header('Content-ID', '<chart_image>')
        chart_img.add_header('Content-Disposition', 'inline', filename=chart_path_img)
    
    # Additional image path

    filter_row = filtered_df[filtered_df['Is_Act_Row'] == 1] #Lấy dòng dữ liệu của tháng hiện tại gần nhất
    rank_check = filter_row['ACUM_CR_Q'].values[0].round()  #Lấy giá trị lũy tiến của tỷ lệ hoàn thành
    # print(rank_check)
    file_path = ''
    if rank_check >= 100:
        file_path = '/python/old_modules_bi/save_image_email/' + '1' + '.jpg' # Good
    elif rank_check > 60:
        file_path = '/python/old_modules_bi/save_image_email/' + '2' + '.jpg'  # Try
    else:
        file_path = '/python/old_modules_bi/save_image_email/' + '3' + '.jpg'  # Not Good
    # Read the additional image
    with open(file_path, 'rb') as img_file:
        file_img = MIMEImage(img_file.read())
        file_img.add_header('Content-ID', '<file_image>')
        file_img.add_header('Content-Disposition', 'inline', filename=file_path)
    
    # Email credentials and recipients
    email_from = 'pgi.bi.main@gmail.com'
    password = 'dudd smoc fgxb egsx'
    # email_to = i
    email_to = 'thien.vo@pgi.com.vn'

    # first_day_of_current_month = datetime.today().replace(day=1)
    first_day_of_current_month = datetime(2024,6,1)
    date_note = (first_day_of_current_month - relativedelta(months=1)).date()
    quarters = convert_date_to_quarter(date_note)
    var_content = ''
    if filter_row['STT_M'].values[0] == 3 and rank_check >= 100:
        var_content = f'Chúc mừng anh/chị đã hoàn thành xuất sắc nhiệm vụ Quý {quarters}!'
    elif filter_row['STT_M'].values[0] == 3 and rank_check >= 60:
        var_content = f'Anh/Chị đã gần đạt tới thành công ở Quý {quarters}, hãy cố gắng hơn ở Quý tới nhé!'
    elif filter_row['STT_M'].values[0] == 3 and rank_check < 60:
        var_content = f'Cảm ơn anh/chị đã cố gắng nổ lực trong Quý {quarters} hãy vững tin và cố gắng hơn ở Quý tới nhé!'
    elif filter_row['STT_M'].values[0] < 3 and rank_check >= 100:
        var_content = f'Anh/Chị đang có kết quả rất tốt trong Quý {quarters} tính tới tháng {date_note}, giữ vững phong độ nhé!'
    elif filter_row['STT_M'].values[0] < 3 and rank_check >= 60:
        var_content = f'Anh/Chị gần đạt được kết quả tốt trong Quý {quarters} tính tới tháng {date_note}, cố gắng thêm ở tháng sau để về đích nào!'
    elif filter_row['STT_M'].values[0] < 3 and rank_check < 60:
        var_content = f'Anh/Chị đang còn chần chờ gì mà không bức phá về đích trong Quý {quarters}'
    # HTML body of the email
    HTMLBody = f"""<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body>

    <p>Dear anh/chị {staff_name},</p>
    <br>
    <p>Bộ phận BI gửi báo cáo Quý {quarters} tính tới thời điểm tháng {date_note} như bên dưới:</p>
    <p>  Với tỷ lệ hoàn thành lũy kế Quý {quarters}: <b class="term">{rank_check}%</b></p>
    <br>
    <p>{var_content}</p>
    <img src="cid:file_image" alt="Additional Image" style="width: 250px; height: 250px;">
    <p>Sơ lượt kết quả Doanh số theo tháng:</p>
    <br>
    <img src="cid:chart_image" alt="Embedded Chart" style="width: auto; height: 400px;">
    <p>Ghi chú:<br></p>
    <p>  + Màu xám : Mục tiêu tháng</p>
    <p>  + Màu xanh : Thực hiện được và hoàn thành >= 100%</p>
    <p>  + Màu vàng : Thực hiện được và hoàn thành >= 60%</p>
    <p>  + Màu đỏ : Thực hiện được và hoàn thành < 60%</p>
    <br>
    <p>Vui lòng không phản hồi email tự động này, có bất kỳ thắc mắc hoặc lỗi vui lòng email:</p>
    <p>IT: thien.vo@pgi.com.vn hoặc liên hệ skype: Thiện Võ</p>
    <p>Thanks & Best Regards</p>

    </body>
    </html>"""

    # Create the email message
    email_message = MIMEMultipart('related')
    email_message['From'] = email_from
    email_message['To'] = email_to
    email_message['Subject'] = f'Doanh số Quý - {quarters} tính tới Tháng - {date_note}'

    # Attach the HTML body
    email_message.attach(MIMEText(HTMLBody, "html"))

    # Attach the seaborn chart image
    email_message.attach(chart_img)

    # Attach the additional image
    email_message.attach(file_img)

    # Convert the email message to string
    email_string = email_message.as_string()

    # Create SSL context
    context = ssl.create_default_context()

    # Send the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login(email_from, password)
        server.sendmail(email_from, email_to, email_string.encode('utf-8'))

    print(f'Mail have sent for {staff_name} ')
    # <p>Đối với các ngày làm việc bị thiếu chấm công, Anh/Chị vui lòng làm request bổ sung chấm công, phép hoặc xin đi trễ/về sớm chậm nhất là 3 ngày làm việc kể từ khi nhận email này.</p>   


