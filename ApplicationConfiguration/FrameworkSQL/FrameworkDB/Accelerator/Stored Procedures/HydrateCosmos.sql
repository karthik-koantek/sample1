﻿CREATE PROCEDURE [dbo].[HydrateCosmos]
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
,@Collection VARCHAR(20)
,@PartitionColumn VARCHAR(50) = ''
,@SchemaName VARCHAR(50)
,@TableName VARCHAR(100)
,@PushdownPredicate VARCHAR(300) = ''
,@SilverZonePartitionColumn VARCHAR(100) = ''
,@SilverZoneClusterColumn VARCHAR(100) = 'pk'
,@SilverZoneClusterBuckets VARCHAR(5) = '8'
,@OptimizeWhere VARCHAR(255) = ''
,@OptimizeZOrderBy VARCHAR(255) = ''
,@VacuumRetentionHours VARCHAR(5) = 168
,@NumPartitions VARCHAR(3) = '8'
,@PrimaryKeyColumns VARCHAR(255)
,@ExplodeAndFlatten VARCHAR(5) = 'True'
,@CleanseColumnNames VARCHAR(5) = 'True'
,@TimestampColumns VARCHAR(MAX) = ''
,@TimestampFormat VARCHAR(30) = 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
,@EncryptColumns VARCHAR(MAX) = ''
,@LoadType VARCHAR(10) = 'Merge'
,@Destination VARCHAR(20) = 'silvergeneral'
,@BronzeZoneNotebookPath VARCHAR(255) = '../Data Engineering/Bronze Zone/Batch CosmosDB'
,@SilverZoneNotebookPath VARCHAR(255) = '../Data Engineering/Silver Zone/Delta Load'
,@GoldZoneNotebookPath VARCHAR(255) = ''
,@PresentationZoneNotebookPath VARCHAR(255) = ''
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
	DECLARE @BronzeParameters Parameters;
	DECLARE @SilverParameters Parameters;

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

	 IF @Collection IS NULL OR @Collection = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @Collection is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @SchemaName IS NULL OR @SchemaName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @SchemaName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @TableName IS NULL OR @TableName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @TableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @PrimaryKeyColumns IS NULL OR @PrimaryKeyColumns = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @PrimaryKeyColumns is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @BronzeZoneNotebookPath IS NULL OR @BronzeZoneNotebookPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @BronzeZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @SilverZoneNotebookPath IS NULL OR @SilverZoneNotebookPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @SilverZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

	INSERT @BronzeParameters (ParameterName, ParameterValue)
	SELECT 'collection' AS ParameterName, @Collection AS ParameterValue
    UNION
    SELECT 'partitionColumn', @PartitionColumn
    UNION
    SELECT 'externalSystem', @SystemSecretScope
	UNION
	SELECT 'schemaName', @SchemaName
	UNION
	SELECT 'tableName', @TableName
	UNION 
	SELECT 'pushdownPredicate', @PushdownPredicate;

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
	,@StepName=@BronzeZoneNotebookPath
	,@StepOrder=10
	,@StepIsActive=1
	,@Parameters=@BronzeParameters;

	INSERT @SilverParameters (ParameterName, ParameterValue)
	SELECT 'schemaName' AS ParameterName, @SchemaName AS ParameterValue
    UNION
    SELECT 'tableName', @TableName
    UNION
    SELECT 'numPartitions', @NumPartitions
    UNION
    SELECT 'primaryKeyColumns', @PrimaryKeyColumns
    UNION
    SELECT 'externalSystem', @SystemSecretScope
	UNION
	SELECT 'partitionCol', @SilverZonePartitionColumn
	UNION
	SELECT 'clusterCol', @SilverZoneClusterColumn
	UNION
	SELECT 'clusterBuckets', @SilverZoneClusterBuckets
	UNION
	SELECT 'optimizeWhere', @OptimizeWhere
	UNION
	SELECT 'optimizeZOrderBy', @OptimizeZOrderBy
	UNION
	SELECT 'vacuumRetentionHours', @VacuumRetentionHours
	UNION
	SELECT 'loadType', @LoadType
	UNION
	SELECT 'destination', @Destination
	UNION
	SELECT 'explodeAndFlatten', @ExplodeAndFlatten
	UNION
	SELECT 'cleanseColumnNames', @CleanseColumnNames
	UNION
	SELECT 'timestampColumns', @TimestampColumns
	UNION
	SELECT 'timestampFormat', @TimestampFormat
	UNION
	SELECT 'encryptColumns', @EncryptColumns;

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
	,@StepName=@SilverZoneNotebookPath
	,@StepOrder=20
	,@StepIsActive=1
	,@Parameters=@SilverParameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO
