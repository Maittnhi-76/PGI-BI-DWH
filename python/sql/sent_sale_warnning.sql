WITH S_Data AS (

  SELECT
    sm_department AS DEPT, 
    nvkinhdoanh AS SALES,
    companyemail AS EMAIL,
    yearmonth AS MONTH,
    CASE WHEN EXTRACT(MONTH FROM yearmonth) > 3 THEN
      CASE WHEN MOD(EXTRACT(MONTH FROM yearmonth), 3) = 0 THEN 3
        WHEN MOD(EXTRACT(MONTH FROM yearmonth), 3) = 1 THEN 1
        WHEN MOD(EXTRACT(MONTH FROM yearmonth), 3) = 2 THEN 2
      ELSE 0 END
    ELSE EXTRACT(MONTH FROM yearmonth) END AS STT_M,
    EXTRACT(QUARTER FROM yearmonth) AS STT_Q,
    EXTRACT(YEAR FROM yearmonth) AS STT_Y,
    -- DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH), INTERVAL 1 MONTH) AS CUR_M,
    DATE('2024-05-01') AS CUR_M,
    SUM(Tgt_revised) AS TGT_M,
    SUM(ttientruocvat) AS ACT_M,
    -- SAFE_DIVIDE(SUM(ttientruocvat), SUM(Tgt_revised)) AS CR_M,
    FROM `pgi-dwh.sales.sale_detail_target_dept_nvkd_exclude_otterbox` AS t1
    LEFT JOIN `pgi-dwh.sales.tb_dm_staffs_mail` AS t2
      ON t1.nvkinhdoanh = t2.name
    WHERE 1 = 1
    --   AND DATE_TRUNC(yearmonth, QUARTER) = DATE_TRUNC(DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH), INTERVAL 1 MONTH), QUARTER)
      AND yearmonth >= '2024-04-01'
      AND yearmonth <= '2024-05-31'
      AND companyemail IS NOT NULL
      AND companyemail IN('thanh.tran@pgi.com.vn', 'thao.tran@pgi.com.vn','khang.tran@pgi.com.vn')
    --   AND companyemail IN('thanh.tran@pgi.com.vn')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    HAVING Tgt_M > 0 AND SALES != 'Nguyễn Thành Nam'
),
T_Acum_By_Quarter AS (

  SELECT DISTINCT
    DEPT,
    SALES,
    EMAIL,
    MONTH,
    STT_M,
    STT_Q,
    STT_Y,
    CUR_M,
    TGT_M,
    ACT_M,
    SUM(Tgt_M) OVER (PARTITION BY DEPT, SALES, EMAIL, Stt_Y, Stt_Q ORDER BY MONTH ASC) AS ACUM_TGT_Q,
    SUM(Act_M) OVER (PARTITION BY DEPT, SALES, EMAIL, Stt_Y, Stt_Q ORDER BY MONTH ASC) AS ACUM_ACT_Q,
  FROM S_Data
),
T_Cal_CR AS (

  SELECT
    DEPT,
    SALES,
    EMAIL,
    MONTH,
    STT_M,
    STT_Q,
    STT_Y,
    CUR_M,
    TGT_M,
    ACT_M,
    ACUM_TGT_Q,
    ACUM_ACT_Q,
    SAFE_DIVIDE(ACT_M, TGT_M) AS CR_M,
    SAFE_DIVIDE(ACUM_ACT_Q, ACUM_TGT_Q) AS ACUM_CR_Q,
  FROM T_Acum_By_Quarter
)
SELECT
  DEPT,
  SALES,
  EMAIL,
  MONTH,
  STT_M,
  STT_Q,
  STT_Y,
  IF(MONTH = CUR_M, 1, 0) AS Is_Act_Row,
  COALESCE(TGT_M, 0) AS TGT_M,
  COALESCE(ACT_M, 0) AS ACT_M,
  COALESCE(ACUM_TGT_Q, 0) AS ACUM_TGT_Q,
  COALESCE(ACUM_ACT_Q, 0) AS ACUM_ACT_Q,
  COALESCE(CR_M, 0) * 100 AS CR_M,
  COALESCE(ACUM_CR_Q, 0) * 100 AS ACUM_CR_Q,
FROM T_Cal_CR
ORDER BY 1, 2, 4
