CREATE PROCEDURE dbo.LogProjectError
 @ProjectLogGuid UNIQUEIDENTIFIER
,@ProjectKey SMALLINT
,@Error VARCHAR(MAX)
AS
BEGIN
	IF @ProjectKey = -1
		SET @ProjectKey = NULL;

    UPDATE dbo.ProjectLog
    SET
     LogStatusKey = 3
	,ProjectKey = @ProjectKey
    ,EndDateTime = SYSDATETIME()
    ,Error = @Error
    WHERE ProjectLogGuid = @ProjectLogGuid;
END
GO