CREATE PROCEDURE [dbo].[GetParameters]
 @StepKey BIGINT
AS
BEGIN
SELECT
   s.StepKey
  ,s.StepName
  ,s.JobKey
  ,p.ParameterName
  ,p.ParameterValue
  FROM dbo.Step s
  JOIN dbo.Parameter p ON s.StepKey=p.StepKey
  WHERE s.StepKey = @StepKey
  UNION
  SELECT StepKey, StepName, JobKey, ParameterName, ParameterValue
  FROM dbo.GetWindow(@StepKey)
END

