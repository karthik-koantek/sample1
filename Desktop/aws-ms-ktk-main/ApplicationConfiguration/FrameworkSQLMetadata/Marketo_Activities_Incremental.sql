DECLARE @ProjectName VARCHAR(255) = 'Marketo_Activities_Incremental';
DECLARE @SystemName VARCHAR(255) = 'Marketo_Ingest';
DECLARE @SystemOrder INT = 10;
DECLARE @StageName VARCHAR(255) = 'Marketo_Ingest_Activities_Incremental';
DECLARE @StageOrder INT = 10;
DECLARE @SystemSecretScope VARCHAR(255) = 'internal';
DECLARE @SystemSecretScopeTransient VARCHAR(255) = 'marketo';
DECLARE @JobOrder INT = 10;
--*--bulk_export_activities--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Marketo_internal_Marketo_bulk_export_activities',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Marketo/',@ClassImportFileName = 'MarketoAPI',@InstantiateAPICall = 'api = MarketoClient(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken)',@APICall = 'data = api.bulk_export_activities(snb.destinationFilePath,True)',@DestinationFilePath = 'mnt/staging-transient/marketo/bulk_export_activities.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Marketo',@TableName = 'bulk_export_activities',@PrimaryKeyColumns = '*';
