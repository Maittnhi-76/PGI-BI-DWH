SELECT
    machamcong,
    tennhanvien,
    tenchamcong,
    ngaycham,
    checkin,
    checkout,
    updated_time
FROM pgi-dwh.human_resource.tb_dm_checkinout_detail_thienvo
WHERE 1 = 1
    AND ngaycham >= '2024-01-01'
LIMIT 50