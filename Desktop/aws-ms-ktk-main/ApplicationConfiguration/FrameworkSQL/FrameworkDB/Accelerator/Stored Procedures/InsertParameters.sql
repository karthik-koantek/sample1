CREATE PROCEDURE [dbo].[InsertParameters]
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
,@Parameters Parameters READONLY
AS
BEGIN
	DECLARE @StepKey BIGINT;

	EXEC dbo.InsertStep @ProjectName=@ProjectName, @SystemName=@SystemName, @SystemSecretScope=@SystemSecretScope, @SystemOrder=@SystemOrder, @SystemIsActive=@SystemIsActive, @SystemIsRestart=@SystemIsRestart, @StageName=@StageName, @StageIsActive=@StageIsActive, @StageIsRestart=@StageIsRestart, @StageOrder=@StageOrder, @JobName=@JobName, @JobOrder=@JobOrder, @JobIsActive=@JobIsActive, @JobIsRestart=@JobIsRestart, @StepName=@StepName, @StepOrder=@StepOrder, @StepIsActive=@StepIsActive, @StepIsRestart=@StepIsRestart, @StepKey=@StepKey OUTPUT;

	MERGE dbo.Parameter AS t
	USING
    (
        SELECT
		 @StepKey AS StepKey
		,ParameterName AS ParameterName
		,ParameterValue AS ParameterValue
        FROM @Parameters
    ) AS s ON t.StepKey=s.StepKey AND t.ParameterName=s.ParameterName
	WHEN MATCHED THEN UPDATE
	SET
		 t.ParameterValue=s.ParameterValue
		,ModifiedDate = SYSDATETIME()
	WHEN NOT MATCHED THEN INSERT
	(
		 StepKey
		,ParameterName
		,ParameterValue
		,CreatedDate
	)
	VALUES
	(
		 @StepKey
		,s.ParameterName
		,s.ParameterValue
		,SYSDATETIME()
	);
END
GO
