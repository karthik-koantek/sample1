CREATE PROCEDURE [dbo].[HydrateSpiderMetadataForSQLSource]
 @ClientName VARCHAR(10)
,@ServerName VARCHAR(100)
,@DatabaseName VARCHAR(100)
,@UserName VARCHAR(100)
,@PasswordKeyVaultSecretName VARCHAR(100)
,@DestinationContainerName VARCHAR(100)
,@SampleRowCount VARCHAR(10)
,@ExcludeTablesWithZeroRowCount VARCHAR(10)
,@ColumnFilterTerms VARCHAR(4000)
AS
BEGIN

    DECLARE @ProjectName VARCHAR(100) = 'Generate_Spider_Metadata_' + @ClientName + '_1_ADF';
    DECLARE @SystemName VARCHAR(100) = @ProjectName + '_' + REPLACE(@ServerName,'\','_');
    DECLARE @StageName VARCHAR(100) = @SystemName + '_MetadataQueries';
    DECLARE @DestinationBasePath VARCHAR(100) = 'spider/' + @ClientName + '/' + REPLACE(@ServerName,'\','_') + '/';

    --ADF Project to run metadata profiling queries against source (to get list of tables, row counts, columns, keys, procs etc.)
    EXEC dbo.HydrateADFSQLMetadataQueries
     @ProjectName=@ProjectName
    ,@SystemName=@SystemName
    ,@StageName=@StageName
    ,@ServerName=@ServerName
    ,@DatabaseName=@DatabaseName
    ,@UserName=@UserName
    ,@PasswordKeyVaultSecretName=@PasswordKeyVaultSecretName
    ,@DestinationContainerName=@DestinationContainerName
    ,@DestinationBasePath=@DestinationBasePath;

    --Databricks Project to use the above metadata profiling queries to generate DVC metadata hydration for the ADF source
    DECLARE @Parameters Parameters;
    INSERT @Parameters(ParameterName, ParameterValue)
    SELECT 'projectName' AS ParameterName, 'Spider_' + @ClientName AS ParameterValue
    UNION
    SELECT 'systemName', 'Spider_' + @ClientName + '_' + REPLACE(@ServerName,'\','_')
    UNION
    SELECT 'stageName', 'Spider_' + @ClientName + '_' + REPLACE(@ServerName,'\','_') + '_Sample'
    UNION
    SELECT 'serverName', @ServerName
    UNION
    SELECT 'databaseName', @DatabaseName
    UNION
    SELECT 'userName', @UserName
    UNION
    SELECT 'passwordKeyVaultSecretName', @PasswordKeyVaultSecretName
    UNION
    SELECT 'destinationContainerName', @DestinationContainerName
    UNION
    SELECT 'destinationBasePath', @DestinationBasePath
    UNION
    SELECT 'sampleRowCount', @SampleRowCount
    UNION
    SELECT 'excludeTablesWithZeroRowCount', @ExcludeTablesWithZeroRowCount
    UNION
    SELECT 'columnFilterTerms', @ColumnFilterTerms;

    SET @ProjectName = 'Generate_Spider_Metadata_' + @ClientName + '_2_ADB';
    SET @SystemName = @ProjectName + '_' + REPLACE(@ServerName,'\','_');
    SET @StageName = @SystemName + '_GenerateHydrationMetadata';
    DECLARE @JobName VARCHAR(100) = @StageName + '_GenerateMetadataForADFSQLSource';

    EXEC dbo.HydrateGenericNotebook
     @ProjectName=@ProjectName
    ,@SystemName=@SystemName
    ,@SystemSecretScope='internal'
    ,@SystemIsActive=1
    ,@SystemOrder=10
    ,@StageName=@StageName
    ,@StageIsActive=1
    ,@StageOrder=10
    ,@JobName=@JobName
    ,@JobIsActive=1
    ,@JobOrder=10
    ,@Parameters=@Parameters
    ,@NotebookPath='../Data Engineering/Metadata/Generate Metadata for ADF SQL Source';

END
GO