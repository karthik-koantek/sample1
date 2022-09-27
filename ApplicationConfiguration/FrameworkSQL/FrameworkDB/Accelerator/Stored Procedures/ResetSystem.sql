CREATE PROCEDURE [dbo].[ResetSystem]
 @SystemKey INT
AS
BEGIN
	UPDATE dbo.Stage
	SET IsRestart = 1
	FROM dbo.Stage s
	JOIN dbo.[System] es ON s.SystemKey=es.SystemKey
	WHERE es.SystemKey = @SystemKey;
END
GO
