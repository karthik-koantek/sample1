CREATE PROCEDURE dbo.GetDataQualityValidationDetail
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
    DataQualityValidationResultDetailId
    ,BatchId
    ,Success
    ,ExpectationType
    ,ExceptionMessage
    ,ExceptionTraceback
    ,RaisedException
    ,KwargsColumn
    ,KwargsColumnList
    ,KwargsMaxValue
    ,KwargsMinValue
    ,KwargsMostly
    ,KwargsRegex
    ,KwargsResultFormat
    ,KwargsTypeList
    ,KwargsValueSet
    ,ResultMissingPercent
    ,ResultObservedValue
    ,ResultPartialUnexpectedList
    ,ResultUnexpectedCount
    ,ResultUnexpectedPercent
    ,ResultUnexpectedPercentNonMissing
    ,ResultUnexpectedPercentDouble
    ,ResultUnexpectedPercentTotal
    FROM dbo.DataQualityValidationResultDetail
    ';

    IF @ProjectLogKey IS NOT NULL
    BEGIN
        SET @Where = N'
        WHERE BatchId IN
        (
            SELECT r.BatchId
            FROM dbo.DataQualityValidationResult r
            JOIN dbo.StepLog sl ON r.StepLogGuid=sl.StepLogGuid
            JOIN dbo.JobLog jl ON sl.JobLogGuid=jl.JobLogGuid
            JOIN dbo.StageLog stl ON jl.StageLogGuid=stl.StageLogGuid
            JOIN dbo.SystemLog syl ON stl.SystemLogGuid=syl.SystemLogGuid
            JOIN dbo.ProjectLog pl ON syl.ProjectLogGuid=pl.ProjectLogGuid
            WHERE pl.ProjectLogKey = @ProjectLogKey
        )
        ';
    END
    ELSE
    BEGIN
        SET @Where = N'
        WHERE BatchId IN
        (
            SELECT r.BatchId
            FROM dbo.DataQualityValidationResult r
            JOIN dbo.StepLog sl ON r.StepLogGuid=sl.StepLogGuid
            JOIN dbo.JobLog jl ON sl.JobLogGuid=jl.JobLogGuid
            JOIN dbo.StageLog stl ON jl.StageLogGuid=stl.StageLogGuid
            JOIN dbo.SystemLog syl ON stl.SystemLogGuid=syl.SystemLogGuid
            JOIN dbo.ProjectLog pl ON syl.ProjectLogGuid=pl.ProjectLogGuid
            WHERE pl.ProjectLogKey IN (SELECT TOP ' + CONVERT(VARCHAR(10),@LastN) + ' ProjectLogKey FROM dbo.ProjectLog ORDER BY ProjectLogKey DESC)
        )
        ';
    END

    SET @OrderBy = N' ORDER BY DataQualityValidationResultDetailId DESC';

    SET @SQL = @Select + @Where + @OrderBy;
        SET @Params = N'@ProjectLogKey BIGINT';

    EXEC SP_EXECUTESQL @SQL, @Params, @ProjectLogKey = @ProjectLogKey;

END
GO