CREATE PROCEDURE [dbo].[HydrateBatchAPI]
 -- orchestration level parameters (these are typically supplied by the calling stored procedure from the hydration script)
 @ProjectName VARCHAR(255)
,@SystemName VARCHAR(255)
,@SystemSecretScope VARCHAR(255) = '' --this will be "internal" for nintex (or any time we are using our own transient zone via mount point path)
,@SystemSecretScopeTransient VARCHAR(255) = '' --this will be a seperate scope for storing and fetching API specific secrets
,@SystemOrder INT = 10
,@SystemIsActive BIT = 1
,@StageName VARCHAR(255)
,@StageIsActive BIT = 1
,@StageOrder INT = 10
,@JobName VARCHAR(255)
,@JobOrder INT = 10
,@JobIsActive BIT = 1
-- api-transient parameters (there should be a paremter for each user supplied widget of the Batch API Notebook)
,@RepoName VARCHAR(255) = '' --the path to the repo from where we will import the custom API class e.g. "/Workspace/Repos/edward.edgeworth@koantek.com/apisupport" or "/Workspace/Repos/master"
,@ClassPath VARCHAR(255) = ''--the relative path (when appended to @repoName to the directory path ) e.g. "/ApplicationConfiguration/Clients/Outreach/"
,@ClassImportFileName VARCHAR(255) = ''-- file filename (and also the name of the class we are importing) e.g. "OutreachAPI"
,@InstantiateAPICall VARCHAR(255) = '' -- the call to instantiate and authenticate with the API (e.g. api = OutreachAPI(snb.ClientId, snb.ClientSecret, authorization_code=snb.AuthorizationCode))
,@APICall VARCHAR(255) = '' -- the actual API call to make (e.g. data = api.getUsers())
,@DestinationFilePath VARCHAR(255) = '' -- path where results of API call are stored.  (e.g. /mnt/data-nintex/outreach/Users.json)
,@IsInitialLoad VARCHAR(255) = 'False' -- for setting urldatetime for fetching time bound records, 'false' as default for fetching daily records
 --the following parameters are for incremental processing requirements (these build date range steps within dbo.windowedxtraction)
,@UseWindowedExtraction BIT = 0 --1 for large batch apis where we need to backfill, else 0 (process everything)
,@WindowingColumn VARCHAR(50) = '' --what is the date predicate/column we are using to filter by in the source?
,@WindowedExtractionBeginDate DATE = NULL --what is the earliest that data exists
,@WindowedExtractionEndDate DATE = NULL --what is the latest that data exists (use today unless data terminates sometime in the past)
,@WindowedExtractionInterval CHAR(1) = NULL --'Y', 'M', 'W' (date intervals by year, month or week)
,@WindowedExtractionProcessLatestWindowFirst BIT = NULL --work backwards (latest first, or forwards (earliest first))
-- transient-bronze parameters (there should be a parameter for each user supplied widget of the Batch File JSON Notebook)
,@ExternalDataPath VARCHAR(200) = '' --full mnt path to the transient zone source 
,@IsDatePartitioned VARCHAR(10) = 'False' --are we date partitioning the landed data the transient zone? 
,@FileExtension VARCHAR(15) = '' -- are we looking for specific file extensions in transient (e.g. *.json)?
,@MultiLine VARCHAR(10) = 'False' --is the json file multi-line or single line?
,@SchemaName VARCHAR(50)  -- "marketo" or "outreach".  this becomes part of the data lake path to the bronze data, and the schema name and file path in silver zone
,@TableName VARCHAR(100)  -- friendly endpoint name e.g. "leads" this becomes part of the data lake path to the bronze data, and the table name and file path in the silver zone
,@PartitionColumn VARCHAR(50) = '' --bronze zone partition column (must be implemented in the notebook)
,@NumPartitions VARCHAR(3) = '8' --bronze zone number of partitions (e.g. how many json files to create)
--bronze-silver parameters (there should be a parameter for each user supplied widget of the Delta Load Notebook)
,@SilverZonePartitionColumn VARCHAR(100) = ''  --if we want the silver zone delta table to be partitioned, this is the partition column
,@SilverZoneClusterColumn VARCHAR(100) = 'pk'  --if we want to do clustering , this is the key (default to pk which is the surrogate key of the table)
,@SilverZoneClusterBuckets VARCHAR(5) = '8'  --default number of clustering buckets
,@OptimizeWhere VARCHAR(255) = ''  --where clause predicate for optimize command (usually can leave blank)
,@OptimizeZOrderBy VARCHAR(255) = '' --order by clause predicate for optimize command
,@VacuumRetentionHours VARCHAR(5) = '168' --how many hours back to retain vacuum retention (e.g. data older than 7 days can no longer be time traveled)
,@ExplodeAndFlatten VARCHAR(5) = 'True' --recursively explode and flatten json until the dataframe is fully relational rows/columns
,@CleanseColumnNames VARCHAR(5) = 'True' --apply regex on the column names to ensure ansi-compliance
,@TimestampColumns VARCHAR(MAX) = '' --list of timestamp columns that will come in as STRING but need to be converted to timestamp
,@TimestampFormat VARCHAR(30) = 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''' --timestamp format for the above conversion
,@EncryptColumns VARCHAR(MAX) = '' --list of columns that need to be one-way encrypted/obfuscated
,@PrimaryKeyColumns VARCHAR(255) = '' --list of column(s) to use as join critera (between staging and silver table). required for merge load type
,@LoadType VARCHAR(10) = 'Merge' --load type to the delta table (e.g. merge, overwrite, append.  merge requires a priamry key)
,@Destination VARCHAR(20) = 'silvergeneral' --which data lake classification zone the data ends up in (e.g. database catalog)
--default paths to the notebooks that are the steps within this microprocess
,@TransientZoneNotebookPath VARCHAR(255) = '../Data Engineering/Bronze Zone/Batch API'
,@BronzeZoneNotebookPath VARCHAR(255) = '../Data Engineering/Bronze Zone/Batch File JSON'
,@SilverZoneNotebookPath VARCHAR(255) = '../Data Engineering/Silver Zone/Delta Load'
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
    DECLARE @TransientParameters Parameters;
    DECLARE @BronzeParameters Parameters;
    DECLARE @SilverParameters Parameters;

	IF @SystemSecretScopeTransient IS NULL OR @SystemSecretScopeTransient = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @SystemSecretScopeTransient is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
    
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

    IF @TransientZoneNotebookPath IS NULL OR @TransientZoneNotebookPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @TransientZoneNotebookPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
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

    IF @RepoName IS NULL OR @RepoName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @RepoName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @ClassPath IS NULL OR @ClassPath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @ClassPath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @ClassImportFileName IS NULL OR @ClassImportFileName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @ClassImportFileName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @InstantiateAPICall IS NULL OR @InstantiateAPICall = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @InstantiateAPICall is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @APICall IS NULL OR @APICall = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @APICall is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @DestinationFilePath IS NULL OR @DestinationFilePath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @DestinationFilePath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

	  IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

    INSERT @TransientParameters (ParameterName, ParameterValue)
    SELECT 'externalSystem' AS ParameterName, @SystemSecretScopeTransient AS ParameterValue
    UNION 
    SELECT 'classPath', @ClassPath
    UNION
    SELECT 'classImportFileName', @ClassImportFileName
    UNION
    SELECT 'instantiateAPICall', @InstantiateAPICall
    UNION
    SELECT 'APICall', @APICall
    UNION
    SELECT 'destinationFilePath', @DestinationFilePath
    UNION
    SELECT 'isInitialLoad', @IsInitialLoad;;

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
    ,@StepName=@TransientZoneNotebookPath
    ,@StepOrder=10
    ,@StepIsActive=1
    ,@Parameters=@TransientParameters;

    INSERT @BronzeParameters (ParameterName, ParameterValue)
	SELECT 'schemaName' AS ParameterName, @SchemaName AS ParameterValue
    UNION
    SELECT 'tableName', @TableName
    UNION
    SELECT 'numPartitions', @NumPartitions
    UNION
    SELECT 'externalSystem', @SystemSecretScope
    UNION
    SELECT 'externalDataPath', @DestinationFilePath
    UNION
    SELECT 'fileExtension', @FileExtension
    UNION
    SELECT 'multiLine', @MultiLine;

    IF @IsDatePartitioned <> 'True'
        INSERT @BronzeParameters(ParameterName, ParameterValue)
        SELECT 'dateToProcess', '-1';

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
	,@StepOrder=20
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

    IF @IsDatePartitioned <> 'True'
        INSERT @SilverParameters(ParameterName, ParameterValue)
        SELECT 'dateToProcess', '-1';

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
	,@StepOrder=30
	,@StepIsActive=1
	,@Parameters=@SilverParameters;

	ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO
