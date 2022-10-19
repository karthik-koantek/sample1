CREATE PROCEDURE [dbo].[HydrateValidationTest]
 @DataLakeZone VARCHAR(100)
,@DatabaseCatalog VARCHAR(100)
,@ObjectType VARCHAR(100)
,@ObjectName VARCHAR(400)
,@Query VARCHAR(MAX) = ''
,@IsActive BIT = 1
,@IsRestart BIT = 1
,@ExpectedColumns VARCHAR(MAX) = ''
,@ExpectedNewOrModifiedRows2Days BIGINT = 0
,@ExpectedNewOrModifiedRows6Days BIGINT = 0
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;
    DECLARE @DataLakeZoneKey TINYINT;
    DECLARE @DatabaseCatalogKey SMALLINT;
    DECLARE @ValidationObjectTypeKey SMALLINT;

    SELECT @DataLakeZoneKey = DataLakeZoneKey FROM dbo.DataLakeZone WHERE DataLakeZone = @DataLakeZone;
    IF @DataLakeZoneKey IS NULL OR @DataLakeZoneKey = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @DataLakeZone is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
    SELECT @DatabaseCatalogKey = DatabaseCatalogKey FROM dbo.DatabaseCatalog WHERE DatabaseCatalog = @DatabaseCatalog;
    IF @DatabaseCatalogKey IS NULL OR @DatabaseCatalogKey = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @DatabaseCatalogKey is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
    SELECT @ValidationObjectTypeKey = ValidationObjectTypeKey FROM dbo.ValidationObjectType WHERE ValidationObjectType = @ObjectType;
    IF @ValidationObjectTypeKey IS NULL OR @ValidationObjectTypeKey = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @ValidationObjectTypeKey is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
    IF @ObjectName IS NULL OR @ObjectName = ''
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @ObjectName is not supported.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
    IF @Query IS NULL OR @Query = '' AND @ObjectType = 'Query'
	BEGIN
		SET @ErrorMessage = 'The value for the parameter @Query must be supplied when @ObjectType is Query.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
    IF (@ExpectedNewOrModifiedRows2Days <> 0 OR @ExpectedNewOrModifiedRows6Days <> 0) AND @ObjectType <> 'Table'
    BEGIN
		SET @ErrorMessage = 'The value for the parameter @ExpectedNewOrModifiedRows is not supported when @ObjectType is not Table.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END

    IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO ReturnCode;
    END

    INSERT dbo.Validation (DataLakeZoneKey,DatabaseCatalogKey,ValidationObjectTypeKey,ObjectName,Query,IsActive,IsRestart,ExpectedColumns,ExpectedNewOrModifiedRows2Days,ExpectedNewOrModifiedRows6Days)
    VALUES (@DataLakeZoneKey,@DatabaseCatalogKey,@ValidationObjectTypeKey,@ObjectName,@Query,@IsActive,@IsRestart,@ExpectedColumns,@ExpectedNewOrModifiedRows2Days,@ExpectedNewOrModifiedRows6Days);

    ReturnCode:
    IF @ReturnCode <> 0
    BEGIN
        RETURN @ReturnCode;
    END
END
GO

