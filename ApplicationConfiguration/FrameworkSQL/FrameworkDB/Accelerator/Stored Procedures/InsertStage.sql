CREATE PROCEDURE [dbo].[InsertStage]
 @ProjectName VARCHAR(255)
,@SystemName VARCHAR(255)
,@SystemSecretScope VARCHAR(255)
,@SystemOrder INT = 10
,@SystemIsActive BIT = 1
,@SystemIsRestart BIT = 1
,@StageName VARCHAR(255)
,@StageIsActive BIT = 1
,@StageIsRestart BIT = 1
,@StageOrder INT = 10
,@StageKey INT OUTPUT
AS
BEGIN
	DECLARE @SystemKey INT;

	EXEC dbo.InsertSystem @ProjectName=@ProjectName, @SystemName=@SystemName, @SystemSecretScope=@SystemSecretScope, @SystemOrder=@SystemOrder, @SystemIsActive=@SystemIsActive, @SystemIsRestart=@SystemIsRestart, @SystemKey=@SystemKey OUTPUT;

	MERGE dbo.Stage t
	USING
	(
		SELECT
		 @StageName AS StageName
		,@SystemKey AS SystemKey
		,@StageIsActive AS IsActive
		,@StageIsRestart AS IsRestart
		,@StageOrder AS StageOrder
	) s ON t.SystemKey = s.SystemKey AND t.StageName = s.StageName
	WHEN MATCHED THEN UPDATE
	SET
	 t.IsActive = s.IsActive
	,t.IsRestart = s.IsRestart
	,t.StageOrder = s.StageOrder
	,t.ModifiedDate = SYSDATETIME()
	WHEN NOT MATCHED THEN INSERT
	(
		 StageName
		,SystemKey
		,IsActive
		,IsRestart
		,StageOrder
		,CreatedDate
	)
	VALUES
	(
		 s.StageName
		,s.SystemKey
		,s.IsActive
		,s.IsRestart
		,s.StageOrder
		,SYSDATETIME()
	);

	SELECT @StageKey = StageKey
	FROM dbo.Stage
	WHERE StageName = @StageName;
END
GO
