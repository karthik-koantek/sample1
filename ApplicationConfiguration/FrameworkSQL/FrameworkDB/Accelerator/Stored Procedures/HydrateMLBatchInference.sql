CREATE PROCEDURE [dbo].[HydrateMLBatchInference]
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
,@ExperimentNotebookPath VARCHAR(1000) = ''
,@ExperimentId VARCHAR(20) = ''
,@MetricClause VARCHAR(100) = ''
,@TableName VARCHAR(100)
,@DeltaHistoryMinutes INT = -1
,@ExcludedColumns VARCHAR(MAX) = ''
,@Partitions VARCHAR(3) = '8'
,@PrimaryKeyColumns VARCHAR(MAX) = ''
,@DestinationTableName VARCHAR(100)
,@PartitionCol VARCHAR(100) = ''
,@ClusterCol VARCHAR(100) = ''
,@ClusterBuckets VARCHAR(20) = '8'
,@OptimizeWhere VARCHAR(255) = ''
,@OptimizeZOrderBy VARCHAR(255) = ''
,@VacuumRetentionHours VARCHAR(5) = 168
,@LoadType VARCHAR(10) = 'Merge'
,@Destination VARCHAR(20) = 'silvergeneral'
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

    IF @TableName IS NULL OR @TableName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @TableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

    IF @DestinationTableName IS NULL OR @DestinationTableName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @DestinationTableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
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

    INSERT @Parameters(ParameterName, ParameterValue)
    SELECT 'experimentNotebookPath' AS ParameterName, @ExperimentNotebookPath AS ParameterValue
    UNION
    SELECT 'experimentId', @ExperimentId
    UNION
    SELECT 'metricClause', @MetricClause
    UNION
    SELECT 'tableName', @TableName
    UNION
    SELECT 'deltaHistoryMinutes', CONVERT(VARCHAR,@DeltaHistoryMinutes)
    UNION
    SELECT 'excludedColumns', @ExcludedColumns
    UNION
    SELECT 'partitions', @Partitions
    UNION
    SELECT 'primaryKeyColumns', @PrimaryKeyColumns
    UNION
    SELECT 'destinationTableName', @DestinationTableName
    UNION
    SELECT 'partitionCol', @PartitionCol
    UNION
    SELECT 'clusterCol', @ClusterCol
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
    SELECT 'destination', @Destination;

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
