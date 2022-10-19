CREATE PROCEDURE [dbo].[ResetStage]
 @StageKey INT
AS
BEGIN
	UPDATE dbo.Job
	SET IsRestart = 1
	FROM dbo.Job j
	JOIN dbo.Stage s ON j.StageKey=s.StageKey
	WHERE s.StageKey = @StageKey;
END
GO
