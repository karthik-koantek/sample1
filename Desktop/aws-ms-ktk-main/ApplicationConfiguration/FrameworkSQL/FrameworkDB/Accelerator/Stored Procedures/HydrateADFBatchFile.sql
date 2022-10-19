CREATE PROCEDURE [dbo].[HydrateADFBatchFile]
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
,@LocalFilePath VARCHAR(100)
,@LocalFileName VARCHAR(100)
,@LocalEncoding VARCHAR(20) = 'UTF-8'
,@Recursive CHAR(1) = '1'
,@EnablePartitionDiscovery CHAR(1) = '0'
,@DestinationContainerName VARCHAR(100)
,@DestinationFilePath VARCHAR(100)
,@DestinationEncoding VARCHAR(20) = 'UTF-8'
,@ColumnDelimiter VARCHAR(3) = ','
,@RowDelimiter VARCHAR(10) = '\r\n'
,@EscapeCharacter VARCHAR(2) = '\\'
,@QuoteCharacter CHAR(1) = '"'
,@FirstRowAsHeader CHAR(1) = '1'
,@NullValue VARCHAR(10) = 'NULL'
,@Namespaces CHAR(1) = '1'
,@DetectDataType CHAR(1) = '1'
,@ADFPipelineName VARCHAR(255) = 'On Premises JSON to Staging'
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
	DECLARE @Parameters Parameters;

    IF @ADFPipelineName NOT IN ('On Premises Any to Staging', 'On Premises JSON to Staging', 'On Premises Delimited to Staging', 'On Premises Parquet to Staging', 'On Premises XML to Staging')
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

    IF @LocalFilePath IS NULL OR @LocalFilePath = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @LocalFilePath is not supported.' + CHAR(13) + CHAR(10) + ' ';
        RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
        SET @Error = @@ERROR;
    END

    IF @LocalFileName IS NULL OR @LocalFileName = ''
    BEGIN
        SET @ErrorMessage = 'The value for the parameter @LocalFileName is not supported.' + CHAR(13) + CHAR(10) + ' ';
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
	SELECT 'localFilePath' AS ParameterName, @LocalFilePath AS ParameterValue
    UNION
    SELECT 'localFileName', @LocalFileName
    UNION
    SELECT 'localEncoding', @LocalEncoding
    UNION
    SELECT 'recursive', @Recursive
    UNION
    SELECT 'enablePartitionDiscovery', @EnablePartitionDiscovery
    UNION
    SELECT 'destinationContainerName', @DestinationContainerName
	UNION
	SELECT 'destinationFilePath', @DestinationFilePath
	UNION
	SELECT 'destinationEncoding', @DestinationEncoding;

    IF @ADFPipelineName = 'On Premises Delimited to Staging'
    BEGIN
        INSERT @Parameters (ParameterName, ParameterValue)
        SELECT 'columnDelimiter' AS ParameterName, @ColumnDelimiter AS ParameterValue
        UNION
        SELECT 'rowDelimiter', @RowDelimiter
        UNION
        SELECT 'escapeCharacter', @EscapeCharacter
        UNION
        SELECT 'quoteCharacter', @QuoteCharacter
        UNION
        SELECT 'firstRowAsHeader', @FirstRowAsHeader
        UNION
        SELECT 'nullValue', @NullValue;
    END

    IF @ADFPipelineName = 'On Premises XML to Staging'
    BEGIN
        INSERT @Parameters (ParameterName, ParameterValue)
        SELECT 'nullValue' AS ParameterName, @NullValue AS ParameterValue
        UNION
        SELECT 'namespaces', @Namespaces
        UNION
        SELECT 'detectDataType', @DetectDataType
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

