CREATE PROCEDURE [dbo].[LogStepError]
 @StepLogGuid UNIQUEIDENTIFIER
,@StepKey BIGINT
,@Error VARCHAR(MAX)
AS
BEGIN
	IF @StepKey = -1
		SET @StepKey = NULL;

	UPDATE dbo.StepLog
	SET
	 LogStatusKey = 3
	,StepKey = @StepKey
	,EndDateTime = SYSDATETIME()
	,Error = @Error
	WHERE StepLogGuid = @StepLogGuid;
END
GO
