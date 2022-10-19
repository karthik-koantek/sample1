CREATE PROCEDURE [dbo].[HydrateBatchParquet]
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
,@MountPoint VARCHAR(30) = ''
,@ExternalDataPath VARCHAR(200) = ''
,@SchemaName VARCHAR(50)
,@TableName VARCHAR(100)
,@QueryZonePartitionColumn VARCHAR(100) = ''
,@QueryZoneClusterColumn VARCHAR(100) = 'pk'
,@QueryZoneClusterBuckets VARCHAR(5) = '8'
,@OptimizeWhere VARCHAR(255) = ''
,@OptimizeZOrderBy VARCHAR(255) = ''
,@VacuumRetentionHours VARCHAR(5) = 168
,@PartitionColumn VARCHAR(50) = ''
,@NumPartitions VARCHAR(3) = '8'
,@PrimaryKeyColumns VARCHAR(255)
,@RawZoneNotebookPath VARCHAR(255) = '../DataEngineering/RawZone/Batch File Parquet'
,@QueryZoneNotebookPath VARCHAR(255) = '../DataEngineering/QueryZone/Delta Load'
AS
BEGIN
	DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
	DECLARE @RawParameters Parameters;
	DECLARE @QueryParameters Parameters;

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

	IF @MountPoint IS NULL OR @MountPoint = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @MountPoint is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @ExternalDataPath IS NULL OR @ExternalDataPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @ExternalDataPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @PrimaryKeyColumns IS NULL OR @PrimaryKeyColumns = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @PrimaryKeyColumns is not supported.' + CHAR(13) + CHAR(10) + ' ';
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

    IF @RawZoneNotebookPath IS NULL OR @RawZoneNotebookPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @RawZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @QueryZoneNotebookPath IS NULL OR @QueryZoneNotebookPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @QueryZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error
        GOTO ReturnCode
    END

	INSERT @RawParameters (ParameterName, ParameterValue)
	SELECT 'schemaName' AS ParameterName, @SchemaName AS ParameterValue
    UNION
    SELECT 'tableName', @TableName
    UNION
    SELECT 'partitionColumn', @PartitionColumn
    UNION
    SELECT 'numPartitions', @NumPartitions
    UNION
    SELECT 'externalSystem', @SystemSecretScope
	UNION
	SELECT 'mountPoint', @MountPoint
    UNION
    SELECT 'externalDataPath', @ExternalDataPath;

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
	,@StepName=@RawZoneNotebookPath
	,@StepOrder=10
	,@StepIsActive=1
	,@Parameters=@RawParameters;

	INSERT @QueryParameters (ParameterName, ParameterValue)
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
	SELECT 'clusterCol', @QueryZoneClusterColumn
	UNION
	SELECT 'clusterBuckets', @QueryZoneClusterBuckets
	UNION
	SELECT 'optimizeWhere', @OptimizeWhere
	UNION
	SELECT 'optimizeZOrderBy', @OptimizeZOrderBy
	UNION
	SELECT 'vacuumRetentionHours', @VacuumRetentionHours;

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
	,@StepName=@QueryZoneNotebookPath
	,@StepOrder=20
	,@StepIsActive=1
	,@Parameters=@QueryParameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO