select 
    sm_department DEPT, 
    nvkinhdoanh SALES,
    companyemail EMAIL,
    --'phuong.huynh@pgi.com.vn' as EMAIL,
    sum(case when yearmonth= date_trunc(current_date,month) then Tgt_revised end) TARGET_MONTH,
    SUM(case when yearmonth= date_trunc(current_date,month) then ttientruocvat end) ACT_MTD,
    SUM(case when yearmonth= date_trunc(current_date,month) then ttientruocvat end)/sum(case when yearmonth= date_trunc(current_date,month) then Tgt_revised end)*100 MTD_ACT_PERCENT,
    sum(case when ngaygiaohang>= date_trunc(current_date,quarter) and date_trunc(yearmonth,quarter)=date_trunc(current_date,quarter) then Tgt_revised end) TARGET_QUARTER,
    SUM(case when ngaygiaohang>= date_trunc(current_date,quarter) then ttientruocvat end) ACT_QTD,
    SUM(case when ngaygiaohang>= date_trunc(current_date,quarter) then ttientruocvat end)/sum(case when ngaygiaohang>= date_trunc(current_date,quarter) and date_trunc(yearmonth,quarter)=date_trunc(current_date,quarter)  then Tgt_revised end)*100 QTD_ACT_PERCENT
    from pgi-dwh.sales.sale_detail_target_dept_nvkd_exclude_otterbox t1
    left join pgi-dwh.sales.tb_dm_staffs_mail t2 on t1.nvkinhdoanh = t2.name
    where ngaygiaohang>= date_trunc(current_date,quarter) 
    group by 1,2,3
    having Target_month>0 and SALES !='Nguyễn Thành Nam' 
    order by 1