CREATE PROCEDURE dbo.GetProjectRuns
AS
BEGIN
    SELECT TOP 50
     pl.ProjectLogKey, pl.ProjectLogGuid, pl.ProjectKey, pl.StartDateTime, pl.EndDateTime, pl.LogStatusKey, ls.LogStatus, pl.Error
    ,JSON_VALUE(pl.Parameters, '$.projectName') AS ProjectName
    ,JSON_VALUE(pl.Parameters, '$.dateToProcess') AS DateToProcess
    ,JSON_VALUE(pl.Parameters, '$.timeout') AS TimeoutSeconds
    ,JSON_VALUE(pl.Parameters, '$.threadPool') AS ThreadPool
    ,JSON_VALUE(pl.Context, '$.jobGroup') AS JobGroup
    ,JSON_VALUE(pl.Context, '$.tags.notebookId') AS NotebookId
    ,JSON_VALUE(pl.Context, '$.tags.sourceIpAddress') AS SourceIPAddress
    ,JSON_VALUE(pl.Context, '$.tags.clusterId') AS ClusterId
    ,JSON_VALUE(pl.Context, '$.tags.user') AS USER_NAME
    ,JSON_VALUE(pl.Context, '$.extraContext.notebook_path') AS NotebookPath
    ,pl.Parameters, pl.Context
    FROM dbo.ProjectLog pl
    JOIN dbo.LogStatus ls ON pl.LogStatusKey=ls.LogStatusKey
    ORDER BY pl.StartDateTime DESC;
END
GO