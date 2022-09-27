CREATE PROCEDURE dbo.GetProjectConfiguration
 @ProjectKey SMALLINT = NULL
,@ProjectName VARCHAR(255) = NULL
,@SystemKey INT = NULL
,@SystemName VARCHAR(255) = NULL
,@SystemIsActive BIT = NULL
,@SystemIsRestart BIT = NULL
,@StageKey INT = NULL
,@StageName VARCHAR(255) = NULL
,@StageIsActive BIT = NULL
,@StageIsRestart BIT = NULL
,@JobKey BIGINT = NULL
,@JobName VARCHAR(255) = NULL
,@JobIsActive BIT = NULL
,@JobIsRestart BIT = NULL
,@StepKey BIGINT = NULL
,@StepName VARCHAR(255) = NULL
,@StepIsActive BIT = NULL
,@StepIsRestart BIT = NULL
,@ParameterKey BIGINT = NULL
,@ParameterName VARCHAR(255) = NULL
,@ReturnParametersAsJSON BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQL NVARCHAR(MAX),
        @SELECT NVARCHAR(4000),
        @WHERE NVARCHAR(4000),
        @ORDERBY NVARCHAR(2000),
        @PARAMS NVARCHAR(4000),
        @CRLF NVARCHAR(10) = CHAR(13);

    IF @ReturnParametersAsJSON = 0
    BEGIN
        SET @SELECT = N'SELECT
        p.ProjectKey, p.ProjectName, p.CreatedDate
        ,s.SystemKey, s.SystemName, s.SystemSecretScope, s.SystemOrder, s.IsActive AS SystemIsActive, s.IsRestart AS SystemIsRestart, s.CreatedDate AS SystemCreatedDate, s.ModifiedDate AS SystemModifiedDate
        ,st.StageKey, st.StageName, st.IsActive AS StageIsActive, st.IsRestart AS StageIsRestart, st.StageOrder, st.NumberOfThreads, st.CreatedDate AS StageCreatedDate, st.ModifiedDate AS StageModifiedDate
        ,j.JobKey, j.JobName, j.JobOrder, j.IsActive AS JobIsActive, j.IsRestart AS JobIsRestart, j.CreatedDate AS JobCreatedDate, j.ModifiedDate AS JobModifiedDate
        ,stp.StepKey, stp.StepName, stp.StepOrder, stp.IsActive AS StepIsActive, stp.IsRestart AS StepIsRestart, stp.CreatedDate AS StepCreatedDate, stp.ModifiedDate AS StepModifiedDate
        ,pm.ParameterKey, pm.ParameterName, pm.ParameterValue, pm.CreatedDate AS ParameterCreatedDate, pm.ModifiedDate AS ParameterModifiedDate
        FROM dbo.Project p
        JOIN dbo.System s ON p.ProjectKey = s.ProjectKey
        JOIN dbo.Stage st ON s.SystemKey = st.SystemKey
        JOIN dbo.Job j ON st.StageKey = j.StageKey
        JOIN dbo.Step stp ON j.JobKey = stp.JobKey
        JOIN dbo.Parameter pm ON stp.StepKey = pm.StepKey' + @CRLF;
    END
    ELSE
    BEGIN
        SET @SELECT = N'SELECT
        p.ProjectKey, p.ProjectName, p.CreatedDate
        ,s.SystemKey, s.SystemName, s.SystemSecretScope, s.SystemOrder, s.IsActive AS SystemIsActive, s.IsRestart AS SystemIsRestart, s.CreatedDate AS SystemCreatedDate, s.ModifiedDate AS SystemModifiedDate
        ,st.StageKey, st.StageName, st.IsActive AS StageIsActive, st.IsRestart AS StageIsRestart, st.StageOrder, st.NumberOfThreads, st.CreatedDate AS StageCreatedDate, st.ModifiedDate AS StageModifiedDate
        ,j.JobKey, j.JobName, j.JobOrder, j.IsActive AS JobIsActive, j.IsRestart AS JobIsRestart, j.CreatedDate AS JobCreatedDate, j.ModifiedDate AS JobModifiedDate
        ,stp.StepKey, stp.StepName, stp.StepOrder, stp.IsActive AS StepIsActive, stp.IsRestart AS StepIsRestart, stp.CreatedDate AS StepCreatedDate, stp.ModifiedDate AS StepModifiedDate
        ,(SELECT pm.ParameterKey, pm.ParameterName, pm.ParameterValue, pm.CreatedDate AS ParameterCreatedDate, pm.ModifiedDate AS ParameterModifiedDate FROM dbo.Parameter pm WHERE pm.StepKey = stp.StepKey FOR JSON AUTO) AS Parameters
        FROM dbo.Project p
        JOIN dbo.System s ON p.ProjectKey = s.ProjectKey
        JOIN dbo.Stage st ON s.SystemKey = st.SystemKey
        JOIN dbo.Job j ON st.StageKey = j.StageKey
        JOIN dbo.Step stp ON j.JobKey = stp.JobKey' + @CRLF;
    END

    SET @WHERE = N'WHERE 1=1' + @CRLF;

    IF @ProjectKey IS NOT NULL
        SET @WHERE += N'AND p.ProjectKey=@ProjectKey' + @CRLF;
    IF @ProjectName IS NOT NULL
        SET @WHERE += N'AND p.ProjectName=@ProjectName' + @CRLF;
    IF @SystemKey IS NOT NULL
        SET @WHERE += N'AND s.SystemKey=@SystemKey' + @CRLF;
    IF @SystemName IS NOT NULL
        SET @WHERE += N'AND s.SystemName=@SystemName' + @CRLF;
    IF @SystemIsActive IS NOT NULL
        SET @WHERE += N'AND s.IsActive=@SystemIsActive' + @CRLF;
    IF @SystemIsRestart IS NOT NULL
        SET @WHERE += N'AND s.IsRestart=@SystemIsRestart' + @CRLF;
    IF @StageKey IS NOT NULL
        SET @WHERE += N'AND st.StageKey=@StageKey' + @CRLF;
    IF @StageName IS NOT NULL
        SET @WHERE += N'AND st.StageName=@StageName' + @CRLF;
    IF @StageIsActive IS NOT NULL
        SET @WHERE += N'AND st.IsActive=@StageIsActive' + @CRLF;
    IF @StageIsRestart IS NOT NULL
        SET @WHERE += N'AND st.IsRestart=@StageIsRestart' + @CRLF;
    IF @JobKey IS NOT NULL
        SET @WHERE += N'AND j.JobKey=@JobKey' + @CRLF;
    IF @JobName IS NOT NULL
        SET @WHERE += N'AND j.JobName=@JobName' + @CRLF;
    IF @JobIsActive IS NOT NULL
        SET @WHERE += N'AND j.IsActive=@JobIsActive' + @CRLF;
    IF @JobIsRestart IS NOT NULL
        SET @WHERE += N'AND j.IsRestart=@JobIsRestart' + @CRLF;
    IF @StepKey IS NOT NULL
        SET @WHERE += N'AND stp.StepKey=@StepKey' + @CRLF;
    IF @StepName IS NOT NULL
        SET @WHERE += N'AND stp.StepName=@StepName' + @CRLF;
    IF @StepIsActive IS NOT NULL
        SET @WHERE += N'AND stp.IsActive=@StepIsActive' + @CRLF;
    IF @StepIsRestart IS NOT NULL
        SET @WHERE += N'AND stp.IsRestart=@StepIsRestart' + @CRLF;
    IF @ParameterKey IS NOT NULL
        SET @WHERE += N'AND p.ParameterKey=@ParameterKey' + @CRLF;
    IF @ParameterName IS NOT NULL
        SET @WHERE += N'AND p.ParameterName=@ParameterName' + @CRLF;

    SET @ORDERBY = N'ORDER BY p.ProjectKey, s.SystemOrder, st.StageOrder, j.JobOrder, stp.StepOrder'

    SET @SQL = @SELECT + @WHERE + @ORDERBY + ';';
    PRINT @SQL;

    SET @PARAMS = N'@ProjectKey SMALLINT,@ProjectName VARCHAR(255),@SystemKey INT,@SystemName VARCHAR(255),@SystemIsActive BIT,@SystemIsRestart BIT
    ,@StageKey INT,@StageName VARCHAR(255),@StageIsActive BIT,@StageIsRestart BIT,@JobKey BIGINT,@JobName VARCHAR(255),@JobIsActive BIT,@JobIsRestart BIT
    ,@StepKey BIGINT,@StepName VARCHAR(255),@StepIsActive BIT,@StepIsRestart BIT,@ParameterKey BIGINT,@ParameterName VARCHAR(255)';

    EXEC SP_EXECUTESQL @SQL, @PARAMS, @ProjectKey=@ProjectKey,@ProjectName=@ProjectName
                                        ,@SystemKey=@SystemKey,@SystemName=@SystemName,@SystemIsActive=@SystemIsActive,@SystemIsRestart=@SystemIsRestart
                                        ,@StageKey=@StageKey,@StageName=@StageName,@StageIsActive=@StageIsActive,@StageIsRestart=@StageIsRestart
                                        ,@JobKey=@JobKey,@JobName=@JobName,@JobIsActive=@JobIsActive,@JobIsRestart=@JobIsRestart
                                        ,@StepKey=@StepKey,@StepName=@StepName,@StepIsActive=@StepIsActive,@StepIsRestart=@StepIsRestart
                                        ,@ParameterKey=@ParameterKey,@ParameterName=@ParameterName;
END
GO



