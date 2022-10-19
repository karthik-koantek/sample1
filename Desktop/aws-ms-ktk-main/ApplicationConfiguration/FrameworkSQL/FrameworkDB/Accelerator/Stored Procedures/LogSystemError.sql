CREATE PROCEDURE [dbo].[LogSystemError]
 @SystemLogGuid UNIQUEIDENTIFIER
,@SystemKey INT
,@Error VARCHAR(MAX)
AS
BEGIN
	IF @SystemKey = -1
		SET @SystemKey = NULL;

	UPDATE dbo.SystemLog
	SET
	 LogStatusKey = 3
	,SystemKey = @SystemKey
	,EndDateTime = SYSDATETIME()
	,Error = @Error
	WHERE SystemLogGuid = @SystemLogGuid;
END
GO
