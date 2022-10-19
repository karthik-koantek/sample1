CREATE PROCEDURE [dbo].[LogSystemEnd]
 @SystemLogGuid UNIQUEIDENTIFIER
,@SystemKey INT
AS
BEGIN
	UPDATE dbo.SystemLog
	SET
	 LogStatusKey = 2
	,SystemKey = @SystemKey
	,EndDateTime = SYSDATETIME()
	WHERE SystemLogGuid = @SystemLogGuid;

	UPDATE dbo.[System]
	SET
	 IsRestart = 0
	WHERE SystemKey = @SystemKey;
END
GO