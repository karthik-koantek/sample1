CREATE PROCEDURE [dbo].[HydrateEDANotebook]
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
,@DeltaHistoryMinutes VARCHAR(7) = '-1'
,@SamplePercent VARCHAR(3) = '-1'
,@ColumnToAnalyze VARCHAR(200) = ''
,@ColumnToAnalyze2 VARCHAR(200) = ''
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

    INSERT @Parameters (ParameterName, ParameterValue)
    SELECT 'tableName' AS ParameterName, @TableName AS ParameterValue
    UNION
    SELECT 'deltaHistoryMinutes', @DeltaHistoryMinutes
    UNION
    SELECT 'samplePercent', @SamplePercent
    UNION 
    SELECT 'columnToAnalyze', @ColumnToAnalyze 
    UNION 
    SELECT 'columnToAnalyze2', @ColumnToAnalyze2;

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
