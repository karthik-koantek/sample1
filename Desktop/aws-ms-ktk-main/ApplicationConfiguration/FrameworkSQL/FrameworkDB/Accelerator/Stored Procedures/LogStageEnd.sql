CREATE PROCEDURE [dbo].[LogStageEnd]
 @StageLogGuid UNIQUEIDENTIFIER
,@StageKey INT
AS
BEGIN
	UPDATE dbo.StageLog
	SET
	 LogStatusKey = 2
	,StageKey = @StageKey
	,EndDateTime = SYSDATETIME()
	WHERE StageLogGuid = @StageLogGuid;

	UPDATE dbo.Stage
	SET
	 IsRestart = 0
	WHERE StageKey = @StageKey;
END
GO
