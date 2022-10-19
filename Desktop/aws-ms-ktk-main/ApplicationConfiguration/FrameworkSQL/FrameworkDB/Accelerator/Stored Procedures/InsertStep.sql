CREATE PROCEDURE [dbo].[InsertStep]
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
,@StepName VARCHAR(255)
,@StepOrder INT = 10
,@StepIsActive BIT = 1
,@StepIsRestart BIT = 1
,@StepKey BIGINT OUTPUT
AS
BEGIN
	DECLARE @JobKey BIGINT;

	EXEC dbo.InsertJob @ProjectName=@ProjectName, @SystemName=@SystemName, @SystemSecretScope=@SystemSecretScope, @SystemOrder=@SystemOrder, @SystemIsActive=@SystemIsActive, @SystemIsRestart=@SystemIsRestart, @StageName=@StageName, @StageIsActive=@StageIsActive, @StageIsRestart=@StageIsRestart, @StageOrder=@StageOrder, @JobName=@JobName, @JobOrder=@JobOrder, @JobIsActive=@JobIsActive, @JobIsRestart=@JobIsRestart, @JobKey=@JobKey OUTPUT;

	MERGE dbo.Step t
	USING
	(
		SELECT
		 @StepName AS StepName
		,@JobKey AS JobKey
		,@StepOrder AS StepOrder
		,@StepIsActive AS IsActive
		,@StepIsRestart AS IsRestart
	) s ON t.JobKey = s.JobKey AND t.StepName = s.StepName
	WHEN MATCHED THEN UPDATE
	SET
	 t.StepOrder = s.StepOrder
	,t.IsActive = s.IsActive
	,t.IsRestart = s.IsRestart
	,t.ModifiedDate = SYSDATETIME()
	WHEN NOT MATCHED THEN INSERT
	(
		 StepName
		,JobKey
		,StepOrder
		,IsActive
		,IsRestart
		,CreatedDate
	)
	VALUES
	(
		 s.StepName
		,s.JobKey
		,s.StepOrder
		,s.IsActive
		,s.IsRestart
		,SYSDATETIME()
	);

	SELECT @StepKey = StepKey
	FROM dbo.Step
	WHERE StepName = @StepName;
END
GO
