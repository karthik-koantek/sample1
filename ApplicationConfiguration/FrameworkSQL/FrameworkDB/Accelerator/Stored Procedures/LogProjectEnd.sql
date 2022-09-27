CREATE PROCEDURE dbo.LogProjectEnd
 @ProjectLogGuid UNIQUEIDENTIFIER
,@ProjectKey SMALLINT
AS
BEGIN
	IF @ProjectKey = -1
		SET @ProjectKey = NULL;

    UPDATE dbo.ProjectLog
    SET
     LogStatusKey = 2
	,ProjectKey = @ProjectKey
    ,EndDateTime = SYSDATETIME()
    WHERE ProjectLogGuid = @ProjectLogGuid;
END
GO