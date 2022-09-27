CREATE PROCEDURE [dbo].[LogStepStart]
 @StepLogGuid UNIQUEIDENTIFIER
,@JobLogGuid UNIQUEIDENTIFIER
,@StepKey BIGINT
,@Parameters VARCHAR(MAX)
,@Context VARCHAR(MAX)
AS
BEGIN
	IF @JobLogGuid = '00000000-0000-0000-0000-000000000000'
		SET @JobLogGuid = NULL;

	IF @StepKey = -1
		SET @StepKey = NULL;

	INSERT dbo.StepLog
	(
		 StepLogGuid
		,JobLogGuid
		,StepKey
		,LogStatusKey
		,Parameters
		,Context
	)
	VALUES
	(
		 @StepLogGuid
		,@JobLogGuid
		,@StepKey
		,1
		,@Parameters
		,@Context
	);
END
GO
