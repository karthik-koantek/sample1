CREATE PROCEDURE [dbo].[LogJobEnd]
 @JobLogGuid UNIQUEIDENTIFIER
,@JobKey BIGINT
AS
BEGIN
	UPDATE dbo.JobLog
	SET
	 LogStatusKey = 2
	,JobKey = @JobKey
	,EndDateTime = SYSDATETIME()
	WHERE JobLogGuid = @JobLogGuid;

	UPDATE dbo.Job
	SET
	 IsRestart = 0
	WHERE JobKey = @JobKey;
END
GO
