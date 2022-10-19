CREATE PROCEDURE [dbo].[GetJob]
 @JobName VARCHAR(255)
,@JobKey BIGINT
AS
BEGIN
SELECT TOP 100 PERCENT
   j.JobKey
  ,j.JobName
  ,j.StageKey
  ,s.StepKey
  ,s.StepName
  ,s.StepOrder
  FROM dbo.Job j
  JOIN dbo.Step s ON j.JobKey=s.JobKey
  WHERE j.JobName = @JobName
  AND j.JobKey = @JobKey
  AND s.IsActive = 1
  AND s.IsRestart = 1
  ORDER BY s.StepOrder
END
