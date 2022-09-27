CREATE PROCEDURE dbo.GetValidationRunDetail
 @ProjectLogKey BIGINT = NULL
,@LastN SMALLINT = 10
,@ValidationStatus VARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQL NVARCHAR(MAX)
    ,@Select NVARCHAR(MAX)
    ,@Where NVARCHAR(1000)
    ,@OrderBy NVARCHAR(1000)
    ,@Params NVARCHAR(1000);

    SET @Select = N'
    SELECT
     pl.ProjectLogKey
    ,vl.ValidationLogKey
    ,vl.ValidationLogGuid
    ,vl.StepLogGuid
    ,vl.ValidationKey
    ,sl.StartDateTime AS StepStartDateTime
    ,sl.EndDateTime AS StepEndDateTime
    ,CASE WHEN sl.Error IS NULL THEN '''' ELSE sl.Error END AS StepError
    ,vl.ValidationStatusKey
    ,s.ValidationStatus
    ,JSON_VALUE(vl.Parameters,''$.stepKey'') AS StepKey
    ,JSON_VALUE(vl.Parameters,''$.dataLakeZone'') AS DataLakeZone
    ,JSON_VALUE(vl.Parameters,''$.databaseCatalog'') AS DatabaseCatalog
    ,JSON_VALUE(vl.Parameters,''$.objectType'') AS ObjectType
    ,JSON_VALUE(vl.Parameters,''$.tableOrViewName'') AS TableOrViewName
    ,JSON_VALUE(vl.Parameters,''$.expectedColumns'') AS ExpectedColumns
    ,JSON_VALUE(vl.Parameters,''$.expectedNewOrModifiedRows2Days'') AS ExpectedNewOrModifiedRows2Days
    ,JSON_VALUE(vl.Parameters,''$.expectedNewOrModifiedRows6Days'') AS ExpectedNewOrModifiedRows6Days
    ,CASE WHEN vl.Error <> '''' THEN JSON_VALUE(vl.Error,''$.sourceName'') ELSE '''' END AS ErrorSourceName
    ,CASE WHEN vl.Error <> '''' THEN JSON_VALUE(vl.Error,''$.errorCode'') ELSE '''' END AS ErrorCode
    ,CASE WHEN vl.Error <> '''' THEN JSON_VALUE(vl.Error,''$.errorDescription'') ELSE '''' END AS ErrorDescription
    ,CASE WHEN vl.Error <> '''' THEN JSON_VALUE(vl.Error,''$.expectedColumns'') ELSE '''' END AS ErrorExpectedColumns
    ,CASE WHEN vl.Error <> '''' THEN JSON_VALUE(vl.Error,''$.actualColumns'') ELSE '''' END AS ErrorActualColumns
    FROM dbo.ValidationLog vl
    JOIN dbo.ValidationStatus s ON vl.ValidationStatusKey=s.ValidationStatusKey
    JOIN dbo.StepLog sl ON vl.StepLogGuid=sl.StepLogGuid
    JOIN dbo.JobLog jl ON sl.JobLogGuid=jl.JobLogGuid
    JOIN dbo.StageLog stgl ON jl.StageLogGuid=stgl.StageLogGuid
    JOIN dbo.SystemLog sysl ON stgl.SystemLogGuid=sysl.SystemLogGuid
    JOIN dbo.ProjectLog pl ON sysl.ProjectLogGuid=pl.ProjectLogGuid
    ';

    IF @ProjectLogKey IS NOT NULL
    BEGIN
        SET @Where = N'WHERE pl.ProjectLogKey = @ProjectLogKey';
    END
    ELSE
    BEGIN
        SET @Where = N'WHERE pl.ProjectLogKey IN (SELECT TOP ' + CONVERT(VARCHAR(10), @LastN) + ' ProjectLogKey FROM dbo.ProjectLog ORDER BY ProjectLogKey DESC)';
    END

    IF @ValidationStatus IS NOT NULL AND @ValidationStatus <> ''
    BEGIN
        SET @Where += N' AND (s.ValidationStatus = @ValidationStatus)';
    END

    SET @OrderBy = N' ORDER BY pl.ProjectLogKey DESC, vl.ValidationLogKey DESC;';

    SET @SQL = @Select + @Where + @OrderBy;
    SET @Params = N'@ProjectLogKey BIGINT, @ValidationStatus VARCHAR(20)';

    PRINT @SQL;
    EXEC SP_EXECUTESQL @SQL, @Params, @ProjectLogKey = @ProjectLogKey, @ValidationStatus = @ValidationStatus;

END
GO
