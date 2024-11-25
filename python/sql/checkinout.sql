SELECT
    t1.machamcong,
    tennhanvien,
    tenchamcong,
    ngaycham,
    MIN(GIOCHAM) AS checkin,
    CASE WHEN MAX(giocham) = MIN(giocham) THEN NULL ELSE MAX(giocham) END AS checkout
FROM CHECKINOUT T1
LEFT JOIN NHANVIEN T2 ON T1.MaChamCong = T2.MaChamCong 
GROUP BY t1.machamcong, tennhanvien, tenchamcong, NGAYCHAM
ORDER BY ngaycham