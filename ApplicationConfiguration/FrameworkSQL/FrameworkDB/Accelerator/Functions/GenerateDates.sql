CREATE FUNCTION [dbo].[GenerateDates] (@FromDate DATETIME2)
RETURNS TABLE WITH SCHEMABINDING AS
RETURN
    SELECT
        n.N AS SequenceNumber
        ,CONVERT(VARCHAR(20),FORMAT(CONVERT(DATE,dt.DT), 'yyyy/MM/dd')) AS [Date]
        ,dp.YY AS [Year]
        ,dp.MM AS [Month]
        ,dp.DD AS [Day]
        ,dp.DW AS [DayOfWeek]
        ,dp.DY AS [DayOfYear]
        ,CONVERT(VARCHAR(20),FORMAT(CONVERT(DATE,dp.LastDayOfMonth), 'yyyy/MM/dd')) AS LastDayOfMonth
    FROM dbo.GenerateNumbers(10000) n
    CROSS APPLY (SELECT DATEADD(dd,(n.N-1)*SIGN(10000),@FromDate) AS DT) dt
    CROSS APPLY
    (
        SELECT DATEPART(yy,dt.DT) AS YY
        ,DATEPART(mm,dt.DT) AS MM
        ,DATEPART(dd,dt.DT) AS DD
        ,DATEPART(dw,dt.DT) AS DW
        ,DATEPART(dy,dt.DT) AS DY
        ,DATEADD(mm,DATEDIFF(mm,-1,dt.DT),-1) AS LastDayOfMonth
    ) dp;
GO