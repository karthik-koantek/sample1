CREATE PROCEDURE [dbo].[HydratePlatinumZone]
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
,@TableName VARCHAR(400)
,@DestinationSchemaName VARCHAR(100) = ''
,@DestinationTableName VARCHAR(400) = ''
,@StoredProcedureName VARCHAR(400) = ''
,@NumPartitions VARCHAR(3) = '8'
,@IgnoreDateToProcessFilter VARCHAR(10) = 'true'
,@LoadType VARCHAR(20) = 'overwrite'
,@TruncateTableInsteadOfDropAndReplace VARCHAR(10) = 'true'
,@IsolationLevel VARCHAR(20) = 'READ_COMMITTED'
,@PresentationZoneNotebookPath VARCHAR(255) = '../Data Engineering/Presentation Zone/SQL Spark Connector 3.0'
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
	SELECT 'externalSystem' AS ParameterName, @SystemSecretScope AS ParameterValue
	UNION
	SELECT 'tableName', @TableName
    UNION
	SELECT 'destinationSchemaName', @DestinationSchemaName
	UNION
	SELECT 'destinationTableName', @DestinationTableName
	UNION
    SELECT 'storedProcedureName', @StoredProcedureName
    UNION
    SELECT 'numPartitions', @NumPartitions
    UNION
    SELECT 'ignoreDateToProcessFilter', @IgnoreDateToProcessFilter
    UNION
    SELECT 'loadType', @LoadType
    UNION
    SELECT 'truncateTableInsteadOfDropAndReplace', @TruncateTableInsteadOfDropAndReplace
	UNION
	SELECT 'isolationLevel', @IsolationLevel;

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
