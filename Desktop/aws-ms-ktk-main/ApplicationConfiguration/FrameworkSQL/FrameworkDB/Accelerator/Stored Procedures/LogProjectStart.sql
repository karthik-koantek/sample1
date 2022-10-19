CREATE PROCEDURE dbo.LogProjectStart
 @ProjectLogGuid UNIQUEIDENTIFIER
,@Parameters VARCHAR(MAX)
,@Context VARCHAR(MAX)
AS
BEGIN
    INSERT dbo.ProjectLog
    (
		 ProjectLogGuid
        ,LogStatusKey
        ,Parameters
        ,Context
    )
    VALUES
    (
		 @ProjectLogGuid
        ,1
        ,@Parameters
        ,@Context
    );
END
GO