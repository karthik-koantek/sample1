﻿CREATE FUNCTION dbo.GenerateNumbers (@Numbers INT)
RETURNS TABLE WITH SCHEMABINDING
AS RETURN
   WITH  E1(N) AS (SELECT 1 UNION ALL SELECT 1),
         E2(N) AS (SELECT 1 FROM E1 a, E1 b),
         E4(N) AS (SELECT 1 FROM E2 a, E2 b),
         E8(N) AS (SELECT 1 FROM E4 a, E4 b),
        E16(N) AS (SELECT 1 FROM E8 a, E8 b),
   cteTally(N) AS (
SELECT TOP (ABS(@Numbers)) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E16)
SELECT N FROM cteTally;
GO