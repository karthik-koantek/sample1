CREATE PROCEDURE [dbo].[HydratePresentationZone]
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
,@DatabaseName VARCHAR(100) = ''
,@TableName VARCHAR(100)
,@CollectionName VARCHAR(100) = ''
,@IDColumn VARCHAR(100) = ''
,@DestinationSchemaName VARCHAR(100) = 'dbo'
,@StoredProcedureName VARCHAR(100) = ''
,@SaveMode VARCHAR(100) = 'overwrite'
,@BulkCopyBatchSize VARCHAR(10) = '2500'
,@BulkCopyTableLock VARCHAR(10) = 'true'
,@BulkCopyTimeout VARCHAR(10) = '600'
,@PresentationZoneNotebookPath VARCHAR(255) = '../Data Engineering/Presentation Zone/SQL Spark Connector'
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;

	DECLARE @PresentationZoneParameters Parameters;

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

	IF @PresentationZoneNotebookPath IS NULL OR @PresentationZoneNotebookPath = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @PresentationZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

	INSERT @PresentationZoneParameters (ParameterName, ParameterValue)
	SELECT 'tableName' AS ParameterName, @TableName AS ParameterValue
    UNION
	SELECT 'destinationSchemaName', @DestinationSchemaName
	UNION
    SELECT 'storedProcedureName', @StoredProcedureName
    UNION
    SELECT 'saveMode', @SaveMode
    UNION
    SELECT 'bulkCopyBatchSize', @BulkCopyBatchSize
    UNION
    SELECT 'bulkCopyTableLock', @BulkCopyTableLock
    UNION
    SELECT 'bulkCopyTimeout', @BulkCopyTimeout
	UNION
    SELECT 'externalSystem', @SystemSecretScope
	UNION
	SELECT 'collection', @CollectionName
	UNION
	SELECT 'databaseName', @DatabaseName
	UNION
	SELECT 'idColumn', @IDColumn;

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
	,@StepName=@PresentationZoneNotebookPath
	,@StepOrder=10
	,@StepIsActive=1
	,@Parameters=@PresentationZoneParameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO
