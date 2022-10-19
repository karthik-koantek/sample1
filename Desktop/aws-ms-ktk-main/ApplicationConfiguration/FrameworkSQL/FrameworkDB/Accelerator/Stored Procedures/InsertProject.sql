CREATE PROCEDURE [dbo].[InsertProject]
 @ProjectName VARCHAR(255)
,@ProjectKey SMALLINT OUTPUT
AS
BEGIN
	DECLARE @tblProjectKey AS TABLE (ProjectKey SMALLINT);

	SELECT @ProjectKey = ProjectKey
	FROM dbo.Project
	WHERE ProjectName = @ProjectName;

	IF @ProjectKey IS NULL
	BEGIN
		INSERT dbo.Project(ProjectName)
		OUTPUT INSERTED.ProjectKey INTO @tblProjectKey
		VALUES(@ProjectName);

		SELECT TOP 1 @ProjectKey = ProjectKey
		FROM @tblProjectKey;
	END
END
GO
