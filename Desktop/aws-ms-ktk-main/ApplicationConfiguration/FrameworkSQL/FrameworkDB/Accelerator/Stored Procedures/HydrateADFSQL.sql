CREATE PROCEDURE [dbo].[HydrateADFSQL]
 @ProjectName VARCHAR(255)
,@SystemName VARCHAR(255)
,@SystemOrder INT = 10
,@SystemIsActive BIT = 1
,@StageName VARCHAR(255)
,@StageIsActive BIT = 1
,@StageOrder INT = 10
,@JobName VARCHAR(255)
,@JobOrder INT = 10
,@JobIsActive BIT = 1
,@ServerName VARCHAR(200)
,@DatabaseName VARCHAR(100)
,@UserName VARCHAR(100)
,@PasswordKeyVaultSecretName VARCHAR(100)
,@SchemaName VARCHAR(100)
,@TableName VARCHAR(100)
,@PushdownQuery VARCHAR(MAX) = ''
,@StoredProcedureCall VARCHAR(MAX) = ''
,@QueryTimeoutMinutes SMALLINT = 120
,@IsolationLevel VARCHAR(100) = 'ReadCommitted'
,@DestinationContainerName VARCHAR(100)
,@DestinationFilePath VARCHAR(100)
,@DestinationEncoding VARCHAR(20) = 'UTF-8'
,@FilePattern VARCHAR(100) = 'Array of objects'
,@ADFPipelineName VARCHAR(255) = 'On Premises Database Query to Staging'
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
	DECLARE @Parameters Parameters;

    IF @ADFPipelineName NOT IN ('On Premises Database Query to Staging', 'On Premises Database Table to Staging', 'On Premises Database Stored Procedure to Staging')
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @ADFPipelineName is not supported.' + CHAR(13) + CHAR(10) + ' ';
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

    IF @ServerName IS NULL OR @ServerName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @ServerName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @DatabaseName IS NULL OR @DatabaseName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @DatabaseName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @UserName IS NULL OR @UserName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @UserName is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @PasswordKeyVaultSecretName IS NULL OR @PasswordKeyVaultSecretName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @PasswordKeyVaultSecretName is not supported.' + CHAR(13) + CHAR(10) + ' ';
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

    IF @DestinationContainerName IS NULL OR @DestinationContainerName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @DestinationContainerName is not supported.' + CHAR(13) + CHAR(10) + ' ';
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
        SET @ReturnCode = @Error
        GOTO ReturnCode
    END

    INSERT @Parameters (ParameterName, ParameterValue)
    SELECT 'serverName' AS ParameterName, @ServerName AS ParameterValue
    UNION
    SELECT 'databaseName', @DatabaseName
    UNION
    SELECT 'userName', @UserName
    UNION
    SELECT 'passwordKeyVaultSecretName', @PasswordKeyVaultSecretName
    UNION
	SELECT 'schemaName', @SchemaName
    UNION
    SELECT 'tableName', @TableName
    UNION
    SELECT 'destinationContainerName', @DestinationContainerName
	UNION
	SELECT 'destinationFilePath', @DestinationFilePath
	UNION
	SELECT 'destinationEncoding', @DestinationEncoding
    UNION
    SELECT 'filePattern', @FilePattern
    UNION
    SELECT 'queryTimeoutMinutes', CONVERT(VARCHAR,@QueryTimeoutMinutes)
    UNION
    SELECT 'isolationLevel', @IsolationLevel;

    IF @ADFPipelineName = 'On Premises Database Query to Staging'
    BEGIN
        INSERT @Parameters (ParameterName, ParameterValue)
        SELECT 'pushdownQuery' AS ParameterName, @PushdownQuery AS ParameterValue;
    END

    IF @ADFPipelineName = 'On Premises Database Stored Procedure to Staging'
    BEGIN
        INSERT @Parameters (ParameterName, ParameterValue)
        SELECT 'storedProcedureCall' AS ParameterName, @StoredProcedureCall AS ParameterValue
    END

    EXEC dbo.InsertParameters
	 @ProjectName=@ProjectName
	,@SystemName=@SystemName
	,@SystemSecretScope=''
	,@SystemOrder=@SystemOrder
	,@SystemIsActive=@SystemIsActive
	,@StageName=@StageName
	,@StageIsActive=@StageIsActive
	,@StageOrder=@StageOrder
	,@JobName=@JobName
	,@JobOrder=@JobOrder
	,@JobIsActive=@JobIsActive
	,@StepName=@ADFPipelineName
	,@StepOrder=20
	,@StepIsActive=1
	,@Parameters=@Parameters;

    ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO

