CREATE PROCEDURE [dbo].[LogJobError]
 @JobLogGuid UNIQUEIDENTIFIER
,@JobKey BIGINT
,@Error VARCHAR(MAX)
AS
BEGIN
	IF @JobKey = -1
		SET @JobKey = NULL;

	UPDATE dbo.JobLog
	SET
	 LogStatusKey = 3
	,JobKey = @JobKey
	,EndDateTime = SYSDATETIME()
	,Error = @Error
	WHERE JobLogGuid = @JobLogGuid;
END
GO