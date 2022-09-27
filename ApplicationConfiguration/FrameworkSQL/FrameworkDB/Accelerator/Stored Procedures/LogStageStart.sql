CREATE PROCEDURE [dbo].[LogStageStart]
 @StageLogGuid UNIQUEIDENTIFIER
,@SystemLogGuid UNIQUEIDENTIFIER
,@StageKey INT
,@Parameters VARCHAR(MAX)
,@Context VARCHAR(MAX)
AS
BEGIN

	IF @SystemLogGuid = '00000000-0000-0000-0000-000000000000'
		SET @SystemLogGuid = NULL;

	IF @StageKey = -1
		SET @StageKey = NULL;

	INSERT dbo.StageLog
	(
		 StageLogGuid
		,SystemLogGuid
		,StageKey
		,LogStatusKey
		,Parameters
		,Context
	)
	VALUES
	(
		 @StageLogGuid
		,@SystemLogGuid
		,@StageKey
		,1
		,@Parameters
		,@Context
	);
END
GO