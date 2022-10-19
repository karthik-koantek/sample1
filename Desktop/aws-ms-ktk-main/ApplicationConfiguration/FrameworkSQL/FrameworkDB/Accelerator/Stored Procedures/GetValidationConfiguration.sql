CREATE PROCEDURE dbo.GetValidationConfiguration
 @DataLakeZoneKey TINYINT = NULL
,@DataLakeZone VARCHAR(100) = NULL
,@DatabaseCatalogKey SMALLINT = NULL
,@DatabaseCatalog VARCHAR(100) = NULL
,@ValidationObjectTypeKey SMALLINT = NULL
,@ValidationObjectType VARCHAR(100) = NULL
,@ObjectName VARCHAR(400) = NULL
,@Query VARCHAR(MAX) = NULL
,@IsActive BIT = NULL
,@IsRestart BIT = NULL
,@InExpectedColumns VARCHAR(100) = NULL
,@ExpectedNewOrModifiedRows2DaysGreaterThan BIGINT = NULL
,@ExpectedNewOrModifiedRows6DaysGreaterThan BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQL NVARCHAR(MAX),
        @SELECT NVARCHAR(4000),
        @WHERE NVARCHAR(4000),
        @ORDERBY NVARCHAR(2000),
        @PARAMS NVARCHAR(4000),
        @CRLF NVARCHAR(10) = CHAR(13);

    SET @SELECT = N'
    SELECT
         v.ValidationKey
        ,v.DataLakeZoneKey
        ,z.DataLakeZone
        ,v.DatabaseCatalogKey
        ,db.DatabaseCatalog
        ,v.ValidationObjectTypeKey
        ,t.ValidationObjectType
        ,v.ObjectName
        ,v.Query
        ,v.IsActive
        ,v.IsRestart
        ,v.CreatedDate
        ,v.ModifiedDate
        ,v.ExpectedColumns
        ,v.ExpectedNewOrModifiedRows2Days
        ,v.ExpectedNewOrModifiedRows6Days
    FROM dbo.Validation v
    JOIN dbo.DataLakeZone z ON v.DataLakeZoneKey = z.DataLakeZoneKey
    JOIN dbo.DatabaseCatalog db ON v.DatabaseCatalogKey = db.DatabaseCatalogKey
    JOIN dbo.ValidationObjectType t ON v.ValidationObjectTypeKey = t.ValidationObjectTypeKey
    ' + @CRLF;

    SET @WHERE = N'WHERE 1=1' + @CRLF;

    IF @DataLakeZoneKey IS NOT NULL
        SET @WHERE += N'AND v.DataLakeZoneKey=@DataLakeZoneKey' + @CRLF;
    IF @DataLakeZone IS NOT NULL
        SET @WHERE += N'AND z.DataLakeZone=@DataLakeZone' + @CRLF;
    IF @DatabaseCatalogKey IS NOT NULL
        SET @WHERE += N'AND v.DatabaseCatalogKey=@DatabaseCatalogKey' + @CRLF;
    IF @DatabaseCatalog IS NOT NULL
        SET @WHERE += N'AND db.DatabaseCatalog=@DatabaseCatalog' + @CRLF;
    IF @ValidationObjectTypeKey IS NOT NULL
        SET @WHERE += N'AND v.ValidationObjectTypeKey=@ValidationObjectType' + @CRLF;
    IF @ValidationObjectType IS NOT NULL
        SET @WHERE += N'AND t.ValidationObjectType=@ValidationObjectType' + @CRLF;
    IF @ObjectName IS NOT NULL
        SET @WHERE += N'AND v.ObjectName=@ObjectName' + @CRLF;
    IF @Query IS NOT NULL
        SET @WHERE += N'AND v.Query=@Query' + @CRLF;
    IF @IsActive IS NOT NULL
        SET @WHERE += N'AND v.IsActive=@IsActive' + @CRLF;
    IF @IsRestart IS NOT NULL
        SET @WHERE += N'AND v.IsRestart=@IsRestart' + @CRLF;
    IF @InExpectedColumns IS NOT NULL
        SET @WHERE += N'AND CHARINDEX(@InExpectedColumns,v.ExpectedColumns)<>0' + @CRLF;
    IF @ExpectedNewOrModifiedRows2DaysGreaterThan IS NOT NULL
        SET @WHERE += N'AND v.ExpectedNewOrModifiedRows2Days>=@ExpectedNewOrModifiedRows2DaysGreaterThan' + @CRLF;
    IF @ExpectedNewOrModifiedRows6DaysGreaterThan IS NOT NULL
        SET @WHERE += N'AND v.ExpectedNewOrModifiedRows6Days>=@ExpectedNewOrModifiedRows6DAysGreaterThan' + @CRLF;

    SET @ORDERBY = N'ORDER BY v.ValidationKey ASC' + @CRLF;

    SET @SQL = @SELECT + @WHERE + @ORDERBY + ';';
    PRINT @SQL;

    SET @PARAMS = N' @DataLakeZoneKey TINYINT,@DataLakeZone VARCHAR(100),@DatabaseCatalogKey SMALLINT,@DatabaseCatalog VARCHAR(100),@ValidationObjectTypeKey SMALLINT
    ,@ValidationObjectType VARCHAR(100),@ObjectName VARCHAR(400),@Query VARCHAR(MAX),@IsActive BIT,@IsRestart BIT,@InExpectedColumns VARCHAR(100)
    ,@ExpectedNewOrModifiedRows2DaysGreaterThan BIGINT,@ExpectedNewOrModifiedRows6DaysGreaterThan BIGINT';

    EXEC SP_EXECUTESQL @SQL, @PARAMS, @DataLakeZoneKey=@DataLakeZoneKey,@DataLakeZone=@DataLakeZone,@DatabaseCatalogKey=@DatabaseCatalogKey,@DatabaseCatalog=@DatabaseCatalog
    ,@ValidationObjectTypeKey=@ValidationObjectTypeKey,@ValidationObjectType=@ValidationObjectType,@ObjectName=@ObjectName,@Query=@Query,@IsActive= @IsActive
    ,@IsRestart=@IsRestart,@InExpectedColumns=@InExpectedColumns,@ExpectedNewOrModifiedRows2DaysGreaterThan=@ExpectedNewOrModifiedRows2DaysGreaterThan
    ,@ExpectedNewOrModifiedRows6DaysGreaterThan=@ExpectedNewOrModifiedRows6DaysGreaterThan;
END
GO
