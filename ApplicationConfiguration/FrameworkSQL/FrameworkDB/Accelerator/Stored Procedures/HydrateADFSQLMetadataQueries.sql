CREATE PROCEDURE [dbo].[HydrateADFSQLMetadataQueries]
 @ProjectName VARCHAR(255)
,@SystemName VARCHAR(255)
,@StageName VARCHAR(255)
,@ServerName VARCHAR(100)
,@DatabaseName VARCHAR(100)
,@UserName VARCHAR(100)
,@PasswordKeyVaultSecretName VARCHAR(100)
,@DestinationContainerName VARCHAR(100) = 'azuredatafactory'
,@DestinationBasePath VARCHAR(255)
AS
BEGIN

    DECLARE @TablesQuery VARCHAR(MAX);
    DECLARE @KeysQuery VARCHAR(MAX);
    DECLARE @ColumnsQuery VARCHAR(MAX);
    DECLARE @ProcsQuery VARCHAR(MAX);

    SET @TablesQuery = 'SELECT TOP 100 PERCENT
        QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) AS [SchemaName]
        ,QUOTENAME(sOBJ.name) AS [TableName]
        ,QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + ''.'' + QUOTENAME(sOBJ.name) AS [FullyQualifiedTableName]
        ,SUM(sPTN.Rows) AS [RowCount]
    FROM sys.objects AS sOBJ
    INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id
    WHERE sOBJ.type = ''U''
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2 -- 0:Heap, 1:Clustered
    GROUP BY sOBJ.schema_id, sOBJ.name
    ORDER BY [FullyQualifiedTableName]';

    SET @KeysQuery = 'SELECT TOP 100 PERCENT
        QUOTENAME(SCHEMA_NAME(tab.schema_id)) AS [SchemaName]
        ,QUOTENAME(tab.[name]) AS [TableName]
        ,QUOTENAME(SCHEMA_NAME(tab.schema_id)) + ''.'' + QUOTENAME(tab.[name]) AS [FullyQualifiedTableName]
        ,pk.[name] AS [PrimaryKeyName]
        ,SUBSTRING(column_names, 1, LEN(column_names)-1) AS [Columns]
    FROM sys.tables tab
    LEFT OUTER JOIN sys.indexes pk ON tab.object_id = pk.object_id
                                    AND pk.is_primary_key = 1
    CROSS APPLY
    (
        SELECT col.[name] + '', ''
        FROM sys.index_columns ic
        JOIN sys.columns col ON ic.object_id = col.object_id
                                AND ic.column_id = col.column_id
        WHERE ic.object_id = tab.object_id
        AND ic.index_id = pk.index_id
        ORDER BY col.column_id
        FOR XML PATH ('''')
    ) D (column_names)
    ORDER BY schema_name(tab.schema_id), tab.[name]';

    SET @ColumnsQuery = 'SELECT
        QUOTENAME(TABLE_SCHEMA) AS [SchemaName]
        ,QUOTENAME(TABLE_NAME) AS [TableName]
        ,QUOTENAME(TABLE_SCHEMA) + ''.'' + QUOTENAME(TABLE_NAME) AS [FullyQualifiedTableName]
        ,QUOTENAME(COLUMN_NAME) AS [ColumnName]
        ,QUOTENAME(TABLE_SCHEMA) + ''.'' + QUOTENAME(TABLE_NAME) + ''.'' + QUOTENAME(COLUMN_NAME) AS [FullyQualifiedColumnName]
        ,ORDINAL_POSITION AS [OrdinalPosition]
        ,COLUMN_DEFAULT AS [ColumnDefault]
        ,IS_NULLABLE AS [IsNullable]
        ,DATA_TYPE AS [DataType]
        ,CHARACTER_MAXIMUM_LENGTH AS [CharacterMaximumLength]
        ,NUMERIC_PRECISION AS [NumericPrecision]
        ,NUMERIC_SCALE AS [NumericScale]
        ,DATETIME_PRECISION AS [DatetimePrecision]
        ,COLLATION_NAME AS [CollationName]
    FROM INFORMATION_SCHEMA.COLUMNS
    ORDER BY [FullyQualifiedColumnName]';

    SET @ProcsQuery = 'SELECT TOP 100 PERCENT
        QUOTENAME(SCHEMA_NAME(SCHEMA_ID)) AS [SchemaName]
        ,QUOTENAME(SO.name) AS [ObjectName]
        ,SO.Type_Desc AS [ObjectType]
        ,P.parameter_id AS [ParameterID]
        ,P.name AS [ParameterName]
        ,TYPE_NAME(P.user_type_id) AS [ParameterDataType]
        ,P.max_length AS [ParameterMaxBytes]
        ,P.is_output AS [IsOutPutParameter]
    FROM sys.objects AS SO
    LEFT JOIN sys.parameters AS P ON SO.OBJECT_ID = P.OBJECT_ID
    WHERE SO.OBJECT_ID IN
    (
        SELECT OBJECT_ID
        FROM sys.objects
        WHERE TYPE IN (''P'',''FN'', ''TF'', ''IF'')
    )
    ORDER BY [SchemaName], SO.name, P.parameter_id';

    DECLARE @JobName VARCHAR(255);
    DECLARE @DestinationPath VARCHAR(255);

    SELECT @JobName = @StageName + '_ListTables', @DestinationPath = @DestinationBasePath + 'listtables';
    EXEC [dbo].[HydrateADFSQL] @ProjectName = @ProjectName,@SystemName = @SystemName,@SystemOrder = 10,@SystemIsActive = 1,@StageName = @StageName,@StageIsActive = 1,@StageOrder = 10,@JobName = @JobName,@JobOrder = 10,@JobIsActive = 1,@ServerName=@ServerName,@DatabaseName=@DatabaseName,@UserName=@UserName,@PasswordKeyVaultSecretName=@PasswordKeyVaultSecretName,@SchemaName = 'sys',@TableName = 'objects',@PushdownQuery = @TablesQuery,@DestinationContainerName = @DestinationContainerName,@DestinationFilePath = @DestinationPath,@DestinationEncoding = 'UTF-8',@ADFPipelineName = 'On Premises Database Query to Staging';
    SELECT @JobName = @StageName + '_ListKeyColumns', @DestinationPath = @DestinationBasePath + 'listkeycolumns';
    EXEC [dbo].[HydrateADFSQL] @ProjectName = @ProjectName,@SystemName = @SystemName,@SystemOrder = 10,@SystemIsActive = 1,@StageName = @StageName,@StageIsActive = 1,@StageOrder = 10,@JobName = @JobName,@JobOrder = 10,@JobIsActive = 1,@ServerName=@ServerName,@DatabaseName=@DatabaseName,@UserName=@UserName,@PasswordKeyVaultSecretName=@PasswordKeyVaultSecretName,@SchemaName = 'sys',@TableName = 'tables',@PushdownQuery = @KeysQuery,@DestinationContainerName = @DestinationContainerName,@DestinationFilePath = @DestinationPath,@DestinationEncoding = 'UTF-8',@ADFPipelineName = 'On Premises Database Query to Staging';
    SELECT @JobName = @StageName + '_ListColumns', @DestinationPath = @DestinationBasePath + 'listcolumns';
    EXEC [dbo].[HydrateADFSQL] @ProjectName = @ProjectName,@SystemName = @SystemName,@SystemOrder = 10,@SystemIsActive = 1,@StageName = @StageName,@StageIsActive = 1,@StageOrder = 10,@JobName = @JobName,@JobOrder = 10,@JobIsActive = 1,@ServerName=@ServerName,@DatabaseName=@DatabaseName,@UserName=@UserName,@PasswordKeyVaultSecretName=@PasswordKeyVaultSecretName,@SchemaName = 'INFORMATION_SCHEMA',@TableName = 'COLUMNS',@PushdownQuery = @ColumnsQuery,@DestinationContainerName = @DestinationContainerName,@DestinationFilePath = @DestinationPath,@DestinationEncoding = 'UTF-8',@ADFPipelineName = 'On Premises Database Query to Staging';
    SELECT @JobName = @StageName + '_ListProcedures', @DestinationPath = @DestinationBasePath + 'listprocedures';
    EXEC [dbo].[HydrateADFSQL] @ProjectName = @ProjectName,@SystemName = @SystemName,@SystemOrder = 10,@SystemIsActive = 1,@StageName = @StageName,@StageIsActive = 1,@StageOrder = 10,@JobName = @JobName,@JobOrder = 10,@JobIsActive = 1,@ServerName=@ServerName,@DatabaseName=@DatabaseName,@UserName=@UserName,@PasswordKeyVaultSecretName=@PasswordKeyVaultSecretName,@SchemaName = 'sys',@TableName = 'objects',@PushdownQuery = @ProcsQuery,@DestinationContainerName = @DestinationContainerName,@DestinationFilePath = @DestinationPath,@DestinationEncoding = 'UTF-8',@ADFPipelineName = 'On Premises Database Query to Staging';
END
GO