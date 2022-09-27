CREATE PROCEDURE dbo.InsertWindowedExtraction
 @StepKey BIGINT
,@DeleteExisting BIT = 0
,@BeginDate DATETIME2
,@EndDate DATETIME2
,@Interval CHAR(1) = 'Y'  --Y, M, OR W
,@ProcessLatestWindowsFirst BIT = 1
AS
BEGIN
    DECLARE @SQL NVARCHAR(4000)
        ,@INSERT NVARCHAR(1000)
        ,@SELECT NVARCHAR(1000)
        ,@WHERE NVARCHAR(1000)
        ,@PARAMS NVARCHAR(1000)
        ,@CRLF NVARCHAR(10) = CHAR(13);

    IF @DeleteExisting = 1
        DELETE FROM dbo.WindowedExtraction WHERE StepKey = @StepKey;

    SET @INSERT = N'INSERT dbo.WindowedExtraction' + @CRLF +
    '(StepKey, WindowStart, WindowEnd, WindowOrder, IsActive)' + @CRLF;

    SET @SELECT = N'SELECT ' + @CRLF +
    '@StepKey AS StepKey' + @CRLF +
    ',CONVERT(VARCHAR(20),[Date]) AS WindowStart ' + @CRLF +
    ',CONVERT(VARCHAR(20),FORMAT(CONVERT(DATE,DATEADD(' + CASE WHEN @Interval = 'Y' THEN 'YEAR' WHEN @Interval = 'M' THEN 'MONTH' WHEN @Interval = 'W' THEN 'WEEK' END + ',1,[Date])), ''yyyy/MM/dd'')) AS WindowEnd' + @CRLF +
    ',ROW_NUMBER() OVER(ORDER BY [Date] ' + CASE WHEN @ProcessLatestWindowsFirst = 1 THEN 'DESC' ELSE 'ASC' END + ') * 10 AS WindowOrder' + @CRLF +
    ',1 AS IsActive' + @CRLF +
    'FROM dbo.GenerateDates(@BeginDate)' + @CRLF;

    SET @WHERE = N'WHERE [Date] BETWEEN @BeginDate AND @EndDate' + @CRLF;

    IF @Interval = 'Y'
        SET @WHERE += 'AND [DayOfYear] = 1';
    IF @Interval = 'M'
        SET @WHERE += 'AND [Day] = 1';
    IF @Interval = 'W'
        SET @WHERE += 'AND [DayOfWeek] = 1';

    SET @SQL = @INSERT + @SELECT + @WHERE + ';';
    PRINT @SQL;

    SET @PARAMS = N'@StepKey BIGINT, @BeginDate DATETIME2, @EndDate DATETIME2';

    EXEC SP_EXECUTESQL @SQL, @PARAMS, @StepKey=@StepKey, @BeginDate=@BeginDate, @EndDate=@EndDate;
END
GO