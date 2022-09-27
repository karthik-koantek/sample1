CREATE PROCEDURE dbo.GetDataQualityValidationSummary
 @ProjectLogKey BIGINT = NULL
,@LastN SMALLINT = 10
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
    ,r.StepLogGuid
    ,r.BatchId
    ,r.ExpectationSuiteName
    ,r.ExpectationsVersion
    ,r.ValidationTime
    ,r.RunName
    ,r.RunTime
    ,r.EvaluatedExpectations
    ,r.SuccessPercent
    ,r.SuccessfulExpectations
    ,r.UnsuccessfulExpectations
    FROM dbo.dataqualityvalidationresult r
    JOIN dbo.StepLog sl ON r.StepLogGuid=sl.StepLogGuid
    JOIN dbo.JobLog jl ON sl.JobLogGuid=jl.JobLogGuid
    JOIN dbo.StageLog stl ON jl.StageLogGuid=stl.StageLogGuid
    JOIN dbo.SystemLog syl ON stl.SystemLogGuid=syl.SystemLogGuid
    JOIN dbo.ProjectLog pl ON syl.ProjectLogGuid=pl.ProjectLogGuid
    ';

    IF @ProjectLogKey IS NOT NULL
    BEGIN
        SET @Where = N'WHERE pl.ProjectLogKey = @ProjectLogKey';
    END
    ELSE
    BEGIN
        SET @Where = N'WHERE pl.ProjectLogKey IN (SELECT TOP ' + CONVERT(VARCHAR(10), @LastN) + ' ProjectLogKey FROM dbo.ProjectLog ORDER BY ProjectLogKey DESC)';
    END

    SET @OrderBy = N' ORDER BY pl.ProjectLogKey DESC, r.RunTime DESC';

    SET @SQL = @Select + @Where + @OrderBy;
    SET @Params = N'@ProjectLogKey BIGINT';

    EXEC SP_EXECUTESQL @SQL, @Params, @ProjectLogKey = @ProjectLogKey;

END
GO