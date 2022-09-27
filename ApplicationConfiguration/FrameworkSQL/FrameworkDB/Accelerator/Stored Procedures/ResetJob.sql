CREATE PROCEDURE [dbo].[ResetJob]
 @JobKey BIGINT
AS
BEGIN
	UPDATE dbo.Step
	SET IsRestart = 1
	FROM dbo.Step s
	JOIN dbo.Job j ON s.JobKey=j.JobKey
	WHERE j.JobKey = @JobKey;
END
GO
