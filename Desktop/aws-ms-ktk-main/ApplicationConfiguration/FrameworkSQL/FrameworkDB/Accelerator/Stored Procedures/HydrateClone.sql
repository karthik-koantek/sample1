CREATE PROCEDURE [dbo].[HydrateClone]
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
,@SourceTableName VARCHAR(255)
,@Destination VARCHAR(100) = 'sandbox'
,@DestinationRelativePath VARCHAR(1000)
,@DestinationTableName VARCHAR(1000)
,@AppendTodaysDateToTableName VARCHAR(10) = 'True'
,@OverwriteTable VARCHAR(10) = 'True'
,@DestinationTableComment VARCHAR(MAX) = ''
,@CloneType VARCHAR(10) = 'SHALLOW'
,@TimeTravelTimestampExpression VARCHAR(100) = ''
,@TimeTravelVersionExpression VARCHAR(100) = ''
,@LogRetentionDurationDays VARCHAR(10) = '7'
,@DeletedFileRetentionDurationDays VARCHAR(10) = '7'
,@NotebookPath VARCHAR(255) = '../Data Engineering/Sandbox Zone/Clone Table'
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

	IF @SourceTableName IS NULL OR @SourceTableName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @SourceTableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @DestinationRelativePath IS NULL OR @DestinationRelativePath = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @DestinationRelativePath is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

	IF @DestinationTableName IS NULL OR @DestinationTableName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @DestinationTableName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

    IF @DestinationTableComment IS NULL OR @DestinationTableComment = ''
	BEGIN
		SET @DestinationTableComment = @CloneType + ' clone of source table ' + @SourceTableName;
	END

	IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

	INSERT @Parameters (ParameterName, ParameterValue)
	SELECT 'sourceTableName' AS ParameterName, @SourceTableName AS ParameterValue
    UNION
	SELECT 'destination', @Destination
	UNION
	SELECT 'destinationRelativePath', @DestinationRelativePath
	UNION
    SELECT 'destinationTableName', @DestinationTableName
    UNION
	SELECT 'destinationTableComment', @DestinationTableComment
	UNION
	SELECT 'cloneType', @CloneType
	UNION
	SELECT 'timeTravelTimestampExpression', @TimeTravelTimestampExpression
	UNION
	SELECT 'timeTravelVersionExpression', @TimeTravelVersionExpression
	UNION
	SELECT 'logRetentionDurationDays', @LogRetentionDurationDays
	UNION
	SELECT 'deletedFileRetentionDurationDays', @DeletedFileRetentionDurationDays
	UNION 
	SELECT 'appendTodaysDateToTableName', @AppendTodaysDateToTableName
	UNION 
	SELECT 'overwriteTable', @OverwriteTable;

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