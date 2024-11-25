with base_date as 
    (
        select t1.day,
    t1. dayofweek,
    t2.machamcong,
    t2.tenchamcong,
    t2.tennhanvien
    from pgi-dwh.categories_manual_sources.dm_day_month_year t1
    left join (select distinct machamcong,tenchamcong,tennhanvien from pgi-dwh.human_resource.dm_staffmail) t2 on 1=1
    where date_trunc(cast(t1.day as date),month) =date_trunc(current_date()-1,month) and cast(t1.day as date)< current_date() 
    -- and tenchamcong IN('thienvo')
    --and tenchamcong in ('PhuongHuynh','NguyenVu','Thu Ha','Tho Nguyen','phungle')
    order by day asc
    )


    select 
    t1.tennhanvien,
    t1.tenchamcong,
    t1.machamcong,
    t1.day,
    case when t1.dayofweek = 'Monday' then 'Hai'
    when t1.dayofweek = 'Tuesday' then 'Ba'
    when t1.dayofweek = 'Wednesday' then 'Tư'
    when t1.dayofweek = 'Thursday' then 'Năm'
    when t1.dayofweek = 'Friday' then 'Sáu'
    when t1.dayofweek = 'Saturday' then 'Bảy'
    when t1.dayofweek = 'Sunday' then 'Chủ Nhật' end as day,
    FORMAT_DATETIME("%H:%M", checkin) checkin,
    FORMAT_DATETIME("%H:%M", t2.checkout) checkout,
    round(datetime_diff(t2.checkout, t2.checkin, minute)/60 -1,1) working_hour
    FROM base_date t1
    left join pgi-dwh.human_resource.tb_dm_checkinout_detail t2 on t1.day=cast(t2.ngaycham as date) and t1.machamcong=t2.machamcong
    where  date_trunc(cast(t1.day as date),month) =date_trunc(current_date()-1,month) and cast(t1.day as date)< current_date()
    order by t1.day