CREATE PROCEDURE [dbo].[HydrateSummaryZone]
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
,@TableName VARCHAR(100)
,@CleansePath BIT = 1
,@RenameFile BIT = 1
,@SummaryZoneNotebookPath VARCHAR(255) = '../DataEngineering/SummaryZone/Power BI File'
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;

	DECLARE @SummaryZoneParameters Parameters;

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

	IF @TableName IS NULL OR @TableName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @TableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @SummaryZoneNotebookPath IS NULL OR @SummaryZoneNotebookPath = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @SummaryZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

	INSERT @SummaryZoneParameters (ParameterName, ParameterValue)
	SELECT 'tableName' AS ParameterName, @TableName AS ParameterValue
    UNION
    SELECT 'cleansePath', CONVERT(VARCHAR(255),@CleansePath)
    UNION
    SELECT 'renameFile', CONVERT(VARCHAR(255),@RenameFile);

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
	,@StepName=@SummaryZoneNotebookPath
	,@StepOrder=10
	,@StepIsActive=1
	,@Parameters=@SummaryZoneParameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO