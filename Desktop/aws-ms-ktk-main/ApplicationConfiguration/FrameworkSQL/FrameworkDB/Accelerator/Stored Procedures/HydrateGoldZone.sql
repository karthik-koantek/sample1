CREATE PROCEDURE [dbo].[HydrateGoldZone]
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
,@ViewName VARCHAR(400)
,@TableName VARCHAR(400)
,@Purpose VARCHAR(100) = 'general'
,@PrimaryKeyColumns VARCHAR(MAX) = ''
,@PartitionColumn VARCHAR(100) = ''
,@ClusterColumn VARCHAR(100) = 'pk'
,@ClusterBuckets VARCHAR(5) = '8'
,@OptimizeWhere VARCHAR(255) = ''
,@OptimizeZOrderBy VARCHAR(255) = ''
,@VacuumRetentionHours VARCHAR(5) = '168'
,@LoadType VARCHAR(10) = 'Merge'
,@MergeSchema VARCHAR(10) = 'False'
,@DeleteNotInSource VARCHAR(10) = 'false'
,@Destination VARCHAR(20) = 'goldgeneral'
,@ConcurrentProcessingPartitionLiteral VARCHAR(MAX) = ''
,@GoldZoneNotebookPath VARCHAR(255) = '../Data Engineering/Gold Zone/Gold Load'
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;

	DECLARE @GoldZoneParameters Parameters;

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

	IF @ViewName IS NULL OR @ViewName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @ViewName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @TableName IS NULL OR @TableName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @TableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @GoldZoneNotebookPath IS NULL OR @GoldZoneNotebookPath = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @GoldZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

	INSERT @GoldZoneParameters (ParameterName, ParameterValue)
	SELECT 'tableName' AS ParameterName, @TableName AS ParameterValue
    UNION
	SELECT 'viewName', @ViewName
	UNION
	SELECT 'purpose', @Purpose
	UNION
    SELECT 'primaryKeyColumns', @PrimaryKeyColumns
    UNION
	SELECT 'partitionCol', @PartitionColumn
	UNION
	SELECT 'clusterCol', @ClusterColumn
	UNION
	SELECT 'clusterBuckets', @ClusterBuckets
	UNION
	SELECT 'optimizeWhere', @OptimizeWhere
	UNION
	SELECT 'optimizeZOrderBy', @OptimizeZOrderBy
	UNION
	SELECT 'vacuumRetentionHours', @VacuumRetentionHours
	UNION
	SELECT 'loadType', @LoadType
	UNION
	SELECT 'mergeSchema', @MergeSchema
	UNION
	SELECT 'deleteNotInSource', @DeleteNotInSource
	UNION
	SELECT 'destination', @Destination
	UNION
	SELECT 'concurrentProcessingPartitionLiteral', @ConcurrentProcessingPartitionLiteral;

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
	,@StepName=@GoldZoneNotebookPath
	,@StepOrder=10
	,@StepIsActive=1
	,@Parameters=@GoldZoneParameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO