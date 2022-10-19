CREATE PROCEDURE [dbo].[InsertSystem]
 @ProjectName VARCHAR(255)
,@SystemName VARCHAR(255)
,@SystemSecretScope VARCHAR(255)
,@SystemOrder INT = 10
,@SystemIsActive BIT = 1
,@SystemIsRestart BIT = 1
,@SystemKey INT OUTPUT
AS
BEGIN
	DECLARE @ProjectKey SMALLINT;

	EXEC dbo.InsertProject @ProjectName=@ProjectName, @ProjectKey=@ProjectKey OUTPUT;

	MERGE dbo.[System] t
	USING
	(
		SELECT
		 @SystemName AS SystemName
		,@SystemSecretScope AS SystemSecretScope
		,@ProjectKey AS ProjectKey
		,@SystemOrder AS SystemOrder
		,@SystemIsActive AS IsActive
		,@SystemIsRestart AS IsRestart
	) s ON t.ProjectKey = s.ProjectKey AND t.SystemName = s.SystemName
	WHEN MATCHED THEN UPDATE
	SET
	  t.SystemSecretScope = s.SystemSecretScope
	 ,t.SystemOrder = s.SystemOrder
	 ,t.IsActive = s.IsActive
	 ,t.IsRestart = s.IsRestart
	 ,t.ModifiedDate = SYSDATETIME()
	WHEN NOT MATCHED THEN INSERT
	(
		 SystemName
		,SystemSecretScope
		,ProjectKey
		,SystemOrder
		,IsActive
		,IsRestart
		,CreatedDate
	)
	VALUES
	(
		 s.SystemName
		,s.SystemSecretScope
		,s.ProjectKey
		,s.SystemOrder
		,s.IsActive
		,s.IsRestart
		,SYSDATETIME()
	);

	SELECT @SystemKey = SystemKey
	FROM dbo.[System]
	WHERE SystemName = @SystemName;
END
GO
