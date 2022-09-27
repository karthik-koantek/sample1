CREATE FUNCTION [dbo].[StepKey]
(
	 @ProjectName VARCHAR(255)
	,@SystemName VARCHAR(255)
	,@StageName VARCHAR(255)
	,@JobName VARCHAR(255)
	,@StepName VARCHAR(255)
)
RETURNS BIGINT
AS
BEGIN
	DECLARE @StepKey BIGINT;

	SELECT @StepKey=stp.StepKey
	FROM dbo.Step stp
	JOIN dbo.Job j ON stp.JobKey=j.JobKey
	JOIN dbo.Stage stg ON j.StageKey=stg.StageKey
	JOIN dbo.System s ON stg.SystemKey=s.SystemKey
	JOIN dbo.Project p ON s.ProjectKey=p.ProjectKey
	WHERE stp.StepName = @StepName
	AND j.JobName = @JobName
	AND stg.StageName = @StageName
	AND s.SystemName = @SystemName
	AND p.ProjectName = @ProjectName;

	RETURN @StepKey
END
