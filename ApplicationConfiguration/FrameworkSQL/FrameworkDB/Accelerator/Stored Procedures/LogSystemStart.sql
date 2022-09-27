CREATE PROCEDURE [dbo].[LogSystemStart]
 @SystemLogGuid UNIQUEIDENTIFIER
,@ProjectLogGuid UNIQUEIDENTIFIER
,@SystemKey INT
,@Parameters VARCHAR(MAX)
,@Context VARCHAR(MAX)
AS
BEGIN

	IF @SystemKey = -1
		SET @SystemKey = NULL;

	IF @ProjectLogGuid = '00000000-0000-0000-0000-000000000000'
		SET @ProjectLogGuid = NULL;

	INSERT dbo.SystemLog
	(
		 SystemLogGuid
		,ProjectLogGuid
		,SystemKey
		,LogStatusKey
		,Parameters
		,Context
	)
	VALUES
	(
		 @SystemLogGuid
		,@ProjectLogGuid
		,@SystemKey
		,1
		,@Parameters
		,@Context
	);
END
GO
