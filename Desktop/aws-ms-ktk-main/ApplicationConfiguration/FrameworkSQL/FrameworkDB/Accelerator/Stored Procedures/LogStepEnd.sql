CREATE PROCEDURE [dbo].[LogStepEnd]
 @JobKey BIGINT
,@StepLogGuid UNIQUEIDENTIFIER
,@StepKey BIGINT
,@WindowStart VARCHAR(20) = NULL
,@WindowEnd VARCHAR(20) = NULL
AS
BEGIN
	IF @StepKey = -1
		SET @StepKey = NULL;

	UPDATE dbo.StepLog
	SET
	 LogStatusKey = 2
	,StepKey = @StepKey
	,EndDateTime = SYSDATETIME()
	WHERE StepLogGuid = @StepLogGuid;

	UPDATE dbo.Step
	SET
	 IsRestart = 0
	WHERE StepKey = @StepKey;

	IF @WindowStart IS NOT NULL AND @WindowEnd IS NOT NULL --raw zone step using windowedExtraction
		UPDATE dbo.WindowedExtraction
		SET ExtractionTimestamp = SYSDATETIME()
		WHERE StepKey = @StepKey
		AND WindowStart = @WindowStart
		AND WindowEnd = @WindowEnd
		AND CONVERT(DATE,WindowEnd) < SYSDATETIME();

	IF @WindowStart IS NULL AND @WindowEnd IS NOT NULL  --query zone step or a non-windowedExtraction step
		UPDATE dbo.WindowedExtraction
		SET QueryZoneTimestamp = SYSDATETIME()
		FROM dbo.WindowedExtraction we
		JOIN dbo.Step s ON we.StepKey=s.StepKey
		WHERE s.JobKey = @JobKey
		AND we.WindowEnd = @WindowEnd
		AND we.ExtractionTimestamp IS NOT NULL
		AND we.WindowEnd < SYSDATETIME();

END
GO
