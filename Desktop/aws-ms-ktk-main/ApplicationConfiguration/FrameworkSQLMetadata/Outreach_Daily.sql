DECLARE @ProjectName VARCHAR(255) = 'Outreach_Daily';
DECLARE @SystemName VARCHAR(255) = 'Outreach_Ingest';
DECLARE @SystemOrder INT = 10;
DECLARE @StageName VARCHAR(255) = 'Outreach_Ingest_Daily';
DECLARE @StageOrder INT = 10;
DECLARE @SystemSecretScope VARCHAR(255) = 'internal';
DECLARE @SystemSecretScopeTransient VARCHAR(255) = 'outreach';
DECLARE @JobOrder INT = 10;
--*--Users--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Users',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getUsers(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Users.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Users',@PrimaryKeyColumns = '*';
--*--Accounts--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Accounts',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getAccounts(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Accounts.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Accounts',@PrimaryKeyColumns = '*';
--*--Teams--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Teams',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getTeams(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Teams.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Teams',@PrimaryKeyColumns = '*';
--*--Prospects--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Prospects',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getProspects(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Prospects.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Prospects',@PrimaryKeyColumns = '*';
--*--Calls--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal__Outreach_Calls',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getCalls(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Calls.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Calls',@PrimaryKeyColumns = '*';
--*--Tasks--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Tasks',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getTasks(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Tasks.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Tasks',@PrimaryKeyColumns = '*';
--*--Sequences--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Sequences',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getSequences(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Sequences.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Sequences',@PrimaryKeyColumns = '*';
--*--Sequence_states--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Sequence_states',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getSequence_states(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Sequence_states.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Sequence_states',@PrimaryKeyColumns = '*';
--*--Sequence_steps--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Sequence_steps',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getSequence_steps(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Sequence_steps.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Sequence_steps',@PrimaryKeyColumns = '*';
--*--Mailings--*--
EXEC dbo.HydrateBatchAPI @ProjectName = @ProjectName, @SystemName = @SystemName, @SystemSecretScope = @SystemSecretScope, @SystemOrder = @SystemOrder, @StageName = @StageName, @StageOrder = @StageOrder, @JobOrder = @JobOrder, @SystemSecretScopeTransient = @SystemSecretScopeTransient,@JobName = 'Outreach_internal_Outreach_Mailings',@RepoName = '/Workspace/Repos/Nintex/Feature',@ClassPath = '/Workspace/Repos/Nintex/Feature/ApplicationConfiguration/Clients/Outreach/',@ClassImportFileName = 'OutreachAPI',@InstantiateAPICall = 'api = OutreachAPI(snb.ClientId, snb.ClientSecret, access_token=snb.accessToken , refresh_token=snb.refreshToken, isInitialLoad=snb.isInitialLoad)',@APICall = 'data = api.getMailings(snb.destinationFilePath)',@DestinationFilePath = 'mnt/staging-transient/outreach/Mailings.json',@IsDatePartitioned = 'False',@FileExtension = 'json',@SchemaName = 'Outreach',@TableName = 'Mailings',@PrimaryKeyColumns = '*';