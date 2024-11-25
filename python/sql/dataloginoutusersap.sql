SELECT
  [ClientName] AS ClientName,
  [Date] AS DateData,
  MIN([Time]) AS First_login,
  CASE WHEN MAX([Time]) = MIN([Time]) THEN NULL ELSE MAX([Time]) END AS Last_Login
FROM[PGI_UAT].[dbo].[USR5]
GROUP BY [ClientName], [Date]
ORDER BY DateData DESC