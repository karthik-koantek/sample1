CREATE PROCEDURE [dbo].[LogJobStart]
 @JobLogGuid UNIQUEIDENTIFIER
,@StageLogGuid UNIQUEIDENTIFIER
,@JobKey BIGINT
,@Parameters VARCHAR(MAX)
,@Context VARCHAR(MAX)
AS
BEGIN

	IF @StageLogGuid = '00000000-0000-0000-0000-000000000000'
		SET @StageLogGuid = NULL;

	IF @JobKey = -1
		SET @JobKey = NULL;

	INSERT dbo.JobLog
	(
		 JobLogGuid
		,StageLogGuid
		,JobKey
		,LogStatusKey
		,Parameters
		,Context
	)
	VALUES
	(
		 @JobLogGuid
		,@StageLogGuid
		,@JobKey
		,1
		,@Parameters
		,@Context
	);
END
GO
