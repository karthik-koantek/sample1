CREATE PROCEDURE dbo.ResetProject
 @ProjectKey SMALLINT
AS
BEGIN
    UPDATE dbo.[System]
    SET IsRestart = 1
    FROM dbo.[System] es
    JOIN dbo.Project p ON es.ProjectKey=p.ProjectKey
    WHERE p.ProjectKey = @ProjectKey;
END
GO