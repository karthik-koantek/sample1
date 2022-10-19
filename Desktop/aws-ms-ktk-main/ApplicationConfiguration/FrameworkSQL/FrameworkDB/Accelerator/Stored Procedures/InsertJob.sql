CREATE PROCEDURE [dbo].[InsertJob]
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
,@JobName VARCHAR(255)
,@JobOrder INT = 10
,@JobIsActive BIT = 1
,@JobIsRestart BIT = 1
,@JobKey BIGINT OUTPUT
AS
BEGIN
	DECLARE @StageKey INT;

	EXEC dbo.InsertStage @ProjectName=@ProjectName, @SystemName=@SystemName, @SystemSecretScope=@SystemSecretScope, @SystemOrder=@SystemOrder, @SystemIsActive=@SystemIsActive, @SystemIsRestart=@SystemIsRestart, @StageName=@StageName, @StageIsActive=@StageIsActive, @StageIsRestart=@StageIsRestart, @StageOrder=@StageOrder, @StageKey=@StageKey OUTPUT;

	MERGE dbo.Job t
	USING
	(
		SELECT
		 @JobName AS JobName
		,@StageKey AS StageKey
		,@JobOrder AS JobOrder
		,@JobIsActive AS IsActive
		,@JobIsRestart AS IsRestart
	) s ON t.StageKey = s.StageKey AND t.JobName = s.JobName
	WHEN MATCHED THEN UPDATE
	SET
	 t.JobOrder = s.JobOrder
	,t.IsActive = s.IsActive
	,t.IsRestart = s.IsRestart
	,t.ModifiedDate = SYSDATETIME()
	WHEN NOT MATCHED THEN INSERT
	(
		 JobName
		,StageKey
		,JobOrder
		,IsActive
		,IsRestart
		,CreatedDate
	)
	VALUES
	(
		 s.JobName
		,s.StageKey
		,s.JobOrder
		,s.IsActive
		,s.IsRestart
		,SYSDATETIME()
	);

	SELECT @JobKey = JobKey
	FROM dbo.Job
	WHERE JobName = @JobName;
END
GO
