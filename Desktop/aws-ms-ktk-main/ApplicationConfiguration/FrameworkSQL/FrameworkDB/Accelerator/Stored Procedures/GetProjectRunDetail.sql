CREATE PROCEDURE [dbo].[GetProjectRunDetail]
 @ProjectLogKey BIGINT = NULL
,@LastN SMALLINT = 10
,@LogStatus VARCHAR(10) = NULL
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
  ,pl.ProjectLogGuid
  ,pl.ProjectKey
  ,COALESCE(p.ProjectName, JSON_VALUE(pl.Parameters, ''$.projectName'')) AS ProjectName
  ,pl.StartDateTime AS ''Project Start Time''
  ,pl.EndDateTime AS ''Project End Time''
  ,pls.LogStatus AS ''Project Status''
  ,pl.Parameters AS ''Project Parameters''
  ,pl.Error AS ''Project Errors''
  ,sl.SystemLogKey
  ,sl.SystemLogGuid
  ,sl.SystemKey
  ,JSON_VALUE(sl.Parameters, ''$.systemName'') AS ''System Name''
  ,s.SystemOrder AS ''System Order''
  ,s.IsRestart AS ''System Is Restart''
  ,sl.StartDateTime AS ''System Start Time''
  ,CASE WHEN sl.StartDateTime IS NOT NULL THEN COALESCE(sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS ''System End Time''
  ,CASE WHEN sl.StartDateTime IS NOT NULL AND sls.LogStatus = ''Started'' AND pls.LogStatus = ''Failed'' THEN ''Failed'' ELSE sls.LogStatus END AS ''System Status''
  ,sl.Parameters AS ''System Parameters''
  ,sl.Error AS ''System Errors''
  ,stl.StageLogKey
  ,stl.StageLogGuid
  ,stl.StageKey
  ,JSON_VALUE(stl.Parameters, ''$.stageName'') AS ''Stage Name''
  ,st.StageOrder AS ''Stage Order''
  ,st.IsRestart AS ''Stage Is Restart''
  ,stl.StartDateTime AS ''Stage Start Time''
  ,CASE WHEN stl.StartDateTime IS NOT NULL THEN COALESCE(stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS ''Stage End Time''
  ,CASE WHEN stl.StartDateTime IS NOT NULL AND stls.LogStatus = ''Started'' AND pls.LogStatus = ''Failed'' THEN ''Failed'' ELSE stls.LogStatus END AS ''Stage Status''
  ,stl.Parameters AS ''Stage Parameters''
  ,stl.Error AS ''Stage Errors''
  ,jl.JobLogKey
  ,jl.JobLogGuid
  ,jl.JobKey
  ,JSON_VALUE(jl.Parameters, ''$.jobName'') AS ''Job Name''
  ,j.JobOrder AS ''Job Order''
  ,j.IsRestart AS ''Job Is Restart''
  ,jl.StartDateTime AS ''Job Start Time''
  ,CASE WHEN jl.StartDateTime IS NOT NULL THEN COALESCE(jl.EndDateTime, stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS ''Job End Time''
  ,CASE WHEN jl.StartDateTime IS NOT NULL AND jls.LogStatus = ''Started'' AND pls.LogStatus = ''Failed'' THEN ''Failed'' ELSE jls.LogStatus END AS ''Job Status''
  ,jl.Parameters AS ''Job Parameters''
  ,jl.Error AS ''Job Errors''
  ,stpl.StepLogKey
  ,stpl.StepLogGuid
  ,stpl.StepKey
  ,JSON_VALUE(stpl.Parameters, ''$.stepName'') AS ''Step Name''
  ,stp.StepOrder AS ''Step Order''
  ,stp.IsRestart AS ''Step Is Restart''
  ,stpl.StartDateTime AS ''Step Start Time''
  ,CASE WHEN stpl.StartDateTime IS NOT NULL THEN COALESCE(stpl.EndDateTime, jl.EndDateTime, stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS ''Step End Time''
  ,CASE WHEN stpl.StartDateTime IS NOT NULL AND stpls.LogStatus = ''Started'' AND pls.LogStatus = ''Failed'' THEN ''Failed'' ELSE stpls.LogStatus END AS ''Step Status''
  ,stpl.Parameters AS ''Step Parameters''
  ,stpl.Error AS ''Step Errors''
  ,n.NotebookLogKey
  ,n.NotebookLogGuid
  ,n.StartDateTime AS ''Notebook Start Time''
  ,CASE WHEN n.StartDateTime IS NOT NULL THEN COALESCE(n.EndDateTime, stpl.EndDateTime, jl.EndDateTime, stl.EndDateTime, sl.EndDateTime, pl.EndDateTime) ELSE NULL END AS ''Notebook End Time''
  ,CASE WHEN n.StartDateTime IS NOT NULL AND nls.LogStatus = ''Started'' AND pls.LogStatus = ''Failed'' THEN ''Failed'' ELSE nls.LogStatus END AS ''Notebook Status''
  ,n.Parameters AS ''Notebook Parameters''
  ,n.Error AS ''Notebook Errors''
  ,n.RowsAffected AS ''Rows Affected''
  FROM dbo.ProjectLog pl
  LEFT JOIN dbo.SystemLog sl ON pl.ProjectLogGuid = sl.ProjectLogGuid
  LEFT JOIN dbo.StageLog stl ON sl.SystemLogGuid = stl.SystemLogGuid
  LEFT JOIN dbo.JobLog jl ON stl.StageLogGuid = jl.StageLogGuid
  LEFT JOIN dbo.StepLog stpl ON jl.JobLogGuid = stpl.JobLogGuid
  LEFT JOIN dbo.NotebookLog n ON stpl.StepLogGuid = n.StepLogGuid
  LEFT JOIN dbo.LogStatus pls ON pl.LogStatusKey = pls.LogStatusKey
  LEFT JOIN dbo.LogStatus sls ON sl.LogStatusKey = sls.LogStatusKey
  LEFT JOIN dbo.LogStatus stls ON stl.LogStatusKey = stls.LogStatusKey
  LEFT JOIN dbo.LogStatus jls ON jl.LogStatusKey = jls.LogStatusKey
  LEFT JOIN dbo.LogStatus stpls ON stpl.LogStatusKey = stpls.LogStatusKey
  LEFT JOIN dbo.LogStatus nls ON n.LogStatusKey = nls.LogStatusKey
  LEFT JOIN dbo.Project p ON pl.ProjectKey = p.ProjectKey
  LEFT JOIN dbo.System s ON sl.SystemKey = s.SystemKey
  LEFT JOIN dbo.Stage st ON stl.StageKey = st.StageKey
  LEFT JOIN dbo.Job j ON jl.JobKey = j.JobKey
  LEFT JOIN dbo.Step stp ON stpl.StepKey = stp.StepKey
  ';

  IF @ProjectLogKey IS NOT NULL
  BEGIN
    SET @Where = N'WHERE pl.ProjectLogKey = @ProjectLogKey';
  END
  ELSE
  BEGIN
    SET @Where = N'WHERE pl.ProjectLogKey IN (SELECT TOP ' + CONVERT(VARCHAR(10), @LastN) + ' ProjectLogKey FROM dbo.ProjectLog ORDER BY ProjectLogKey DESC)
  AND JSON_VALUE(pl.Parameters, ''$.projectName'') <> ''''
    ';
  END

  IF @LogStatus IS NOT NULL AND @LogStatus <> ''
  BEGIN
    SET @Where += N' AND (nls.LogStatus = @LogStatus OR stpls.LogStatus = @LogStatus)';
  END

  SET @OrderBy = N' ORDER BY pl.ProjectLogKey DESC, sl.SystemLogKey DESC, stl.StageLogKey DESC, jl.JobLogKey DESC, stpl.StepLogKey DESC, n.NotebookLogKey DESC;';

  SET @SQL = @Select + @Where + @OrderBy;
  SET @Params = N'@ProjectLogKey BIGINT, @LogStatus VARCHAR(10)';

  EXEC SP_EXECUTESQL @SQL, @Params, @ProjectLogKey = @ProjectLogKey, @LogStatus = @LogStatus;

END
GO
