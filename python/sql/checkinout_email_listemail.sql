SELECT DISTINCT
    machamcong,
    tenchamcong,
    tennhanvien,
    email
FROM `pgi-dwh.human_resource.dm_staffmail`
WHERE 1 = 1
    AND email is not null 
    -- AND tenchamcong IN ('thienvo')