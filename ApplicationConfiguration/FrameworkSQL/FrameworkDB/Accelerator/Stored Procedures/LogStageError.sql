CREATE PROCEDURE [dbo].[LogStageError]
 @StageLogGuid UNIQUEIDENTIFIER
,@StageKey INT
,@Error VARCHAR(MAX)
AS
BEGIN
	IF @StageKey = -1
		SET @StageKey = NULL;

	UPDATE dbo.StageLog
	SET
	 LogStatusKey = 3
	,StageKey = @StageKey
	,EndDateTime = SYSDATETIME()
	,Error = @Error
	WHERE StageLogGuid = @StageLogGuid;
END
GO
