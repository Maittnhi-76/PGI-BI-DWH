# PGI-BI-DWH
PGI BUSINESS INTELLIGENCE PLATFORM
Short decription: Platform include all tool support for ETL in PGI

Structure:

- Images: Chứa tất cả các image được build và sử dụng trong platform
  + Airflow Apache: CÔNG CỤ QUẢN LÝ VÀ VẬN HÀNH CÁC JOB TỰ ĐỘNG CỦA TOÀN BỘ PLATFORM DỮ LIỆU
    * Version: pgi-airflow-image:2.9.0.1
    * Mục đích: Image để dựng airflow chạy core schedule cho toàn platform
    * Mô tả: Image để dựng airflow chạy core schedule cho toàn platform
  + Dbt: CÔNG CỤ QUẢN LÝ VÀ THỰC THI LUỒNG DỮ LIỆU SQL KẾT NỐI VỚI BIGQUERY
    * Version: dbtrun:1.0
    * Mục đích: Dựng lên 1 môi trường và các gói hỗ trợ dành riêng cho việc cấu hình và thực thi dbt
    * Mô tả: Image để chạy dự án trong folder "Transform"
  + Python_etl: MÔI TRƯỜNG PYTHON ĐỂ ETL DỮ LIỆU
    * Version: python_etl:1.0
    * Mục đích: Môi trường python với các gói hỗ trợ tương thích phục vụ cho việc etl dữ liệu. Và có thể điều chỉnh mà không ảnh hưởng các gói khác
    * Mô tả: Để chạy các file python ở giai đoạn ETL dữ liệu
  + Python_load_database: MÔI PYTHON ĐỂ LOAD DỮ LIỆU TỪ CÁC DATABASE OR API
    * Version: python_load_db:1.0
    * Mục đích: Tạo một môi trường python đặc thù cho việc giao tiếp với các DB như SQLSever, Hana, API, GGS....
    * Mô tả: Để chạy các file python ở giai đoạn Etract dữ liệu từ nguồn
- Airflow: Nơi cấu hình các job chạy tự động và tần suất của toàn bộ platform
  + Analytics: nơi chứa code phân tích ad-hoc
  + Credential: bảo mật
  + Dags:
    * etl_dw_a: cấu hình các file dbt ở tầng staging
    * etl_dbt_abcd: cấu hình các file tạo models ở tầng b(datawarehouse), c(common), d(mart, vmart)
    * etl_ggs: cấu hình các nguồn dữ liệu lấy từ Google Sheet
    * etl_old_bi: cấu hình các luồng dữ liệu cũ chưa theo model
      * atuo_email: cấu hình các khung giờ và tần suất thực hiện gửi mail
      * load_dw: cấu hình việc xử lý dữ liệu khi không có models dbt
  + Logs: ghi lại logs của từng sự kiện trong airflow
  + Plugins:
  + Add-in (airflow.env; docker-compose.yml, start_airflow.sh): những file cấu hình để tự động ghi nhận và sử dụng airflow
- Python: Chứa tất cả các file được xây dựng bằng Python có thể chia nhỏ theo mục tiêu sử dụng:
  + Dev_evn: Folder chứa các file thử nghiệm
  + Extract_From_Database: Cấu hình các job lấy data từ nguồn
    * Db_hana:
    * Db_sqlsever:
  + LoadGoogleSheet: Chứa cấu hình các file GGS được load lên
  + OldModuleBI: File chạy models cũ
- Transform: Chứa cấu trúc models (dims - facts) xây dựng trên nền tảng dbt-core and plusg-in dbt-bigquery 
  + Logs:
  + Macros:
  + Models:
    * a_st: Tầng staging
    * b_dw: Tầng Data warehouse
    * c_cm: Tầng Common
    * c_cm_old: Tầng dữ liệu cũ
    * d_vm: Tầng mart và vmart
  + Profiles: Cấu hình Profile
  + Target: nơi lưu thông tin câu SQL được chuyển đổi
  + Dbt_Project: Cấu hình project transform
END
