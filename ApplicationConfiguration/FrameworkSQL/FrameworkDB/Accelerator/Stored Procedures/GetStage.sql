CREATE PROCEDURE [dbo].[GetStage]
 @StageName VARCHAR(255)
AS
BEGIN
SELECT TOP 100 PERCENT
   s.StageKey
  ,s.StageName
  ,s.SystemKey
  ,j.JobKey
  ,j.JobName
  ,j.JobOrder
  FROM dbo.Stage s
  JOIN dbo.Job j ON s.StageKey=j.StageKey
  WHERE s.StageName = @StageName
  AND j.IsActive = 1
  AND j.IsRestart = 1
  ORDER BY j.JobOrder
END

