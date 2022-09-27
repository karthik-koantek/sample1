CREATE PROCEDURE [dbo].[HydrateStreamingNotebook]
 @ProjectName VARCHAR(255)
,@SystemName VARCHAR(255)
,@SystemSecretScope VARCHAR(255)
,@SystemOrder INT = 10
,@SystemIsActive BIT = 1
,@StageName VARCHAR(255)
,@StageIsActive BIT = 1
,@StageOrder INT = 10
,@JobName VARCHAR(255)
,@JobOrder INT = 10
,@JobIsActive BIT = 1
,@ExternalSystem VARCHAR(255)           --Event Hub External System
,@Trigger VARCHAR(50) = 'Once'          --Once, Microbatch
,@MaxEventsPerTrigger SMALLINT = 10000
,@Offset SMALLINT = 0                   --Event Hub Ingestion Offset
,@Interval SMALLINT = 10                --Interval Seconds
,@EventHubStartingPosition VARCHAR(50) = 'fromStartOfStream'
,@DatabaseName VARCHAR(100) = ''		--Database Name (if applicable)
,@TableName VARCHAR(100) = ''           --Table Name
,@Destination VARCHAR(20) = 'silvergeneral'
,@StopSeconds SMALLINT = 120
,@ColumnName VARCHAR(100) = ''          --Sentiment Column Name (used by the sentiment analysis notebook)
,@OutputTableName VARCHAR(100) = ''     --Scored Table Name (used by the sentiment analysis notebook)
,@NotebookPath VARCHAR(255)
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
	DECLARE @Parameters Parameters;

    	IF @ProjectName IS NULL OR @ProjectName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @ProjectName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @SystemName IS NULL OR @SystemName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @SystemName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @SystemSecretScope IS NULL OR @SystemSecretScope = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @SystemSecretScope is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @StageName IS NULL OR @StageName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @StageName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @JobName IS NULL OR @JobName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @JobName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @ExternalSystem IS NULL OR @ExternalSystem = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @ExternalSystem is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @TableName IS NULL OR @TableName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @TableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

	IF @NotebookPath IS NULL OR @NotebookPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @NotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

	IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

	INSERT @Parameters (ParameterName, ParameterValue)
	SELECT 'tableName' AS ParameterName, @TableName AS ParameterValue
    UNION
	SELECT 'databaseName', @DatabaseName
	UNION
	SELECT 'destination', @Destination
	UNION
    SELECT 'externalSystem', @ExternalSystem
	UNION
    SELECT 'trigger', @Trigger
    UNION
	SELECT 'maxEventsPerTrigger', CONVERT(VARCHAR(50),@MaxEventsPerTrigger)
	UNION
	SELECT 'interval', CONVERT(VARCHAR(50),@Interval)
	UNION
	SELECT 'offset', CONVERT(VARCHAR(50),@Offset)
	UNION
	SELECT 'columnName', @ColumnName
	UNION
	SELECT 'outputTableName', @OutputTableName
	UNION
	SELECT 'stopSeconds', CONVERT(VARCHAR(50),@StopSeconds)
	UNION
	SELECT 'eventHubStartingPosition', @EventHubStartingPosition;

	EXEC dbo.InsertParameters
	 @ProjectName=@ProjectName
	,@SystemName=@SystemName
	,@SystemSecretScope=@SystemSecretScope
	,@SystemOrder=@SystemOrder
	,@SystemIsActive=@SystemIsActive
	,@StageName=@StageName
	,@StageIsActive=@StageIsActive
	,@StageOrder=@StageOrder
	,@JobName=@JobName
	,@JobOrder=@JobOrder
	,@JobIsActive=@JobIsActive
	,@StepName=@NotebookPath
	,@StepOrder=10
	,@StepIsActive=1
	,@Parameters=@Parameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO
